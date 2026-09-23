# TODO: Текстовый интерфейс (TUI) на базе Textual + разложение на пакет

План изменений: добавить интерактивный текстовый интерфейс (`tui`-подкоманда) на базе
[Textual](https://textual.io) и попутно разложить «single-file»-модуль на пакет.

## UX (согласовано с пользователем)

- Запуск: `python dbmigration.py tui <schema_name> <scripts_path> [--dbenv --host --port --dbname --user -n]` —
  новый subcommand `tui` с теми же общими DB-аргументами, что у остальных команд.
- При открытии окна в **центральной части** сразу выполняется полная команда `verify`
  и выводится результат (построчно, «живой» лог). Если схема ещё не инициализирована —
  вместо ошибки показывается подсказка «run init».
- **Правая панель** — список доступных команд, вычисленный из текущего состояния схемы и репозитория:
  - `init` — если контрольные таблицы версий ещё не существуют;
  - `update` — если таблицы есть; фиксированные переключатели (чекбоксы) опций:
    `--force-reapply-latest-version`, `--force-reapply-all-repeatable`, `--force-run-cleanup`, `--skip-confirmation`;
  - `verify` — если таблицы есть; опции: `--skip-diffs`, `--skip-display-recent-changes`, `--build-update-script`;
  - `run-tests` — если таблицы есть и в репозитории присутствует каталог `tests/`;
  - после `init`/`update` состояние пере-пробуется, панель перерисовывается.
- `update` без `--skip-confirmation` → модальное окно подтверждения (y/N), вместо сырого `get_char()`.

## Ключевые решения (зафиксированы)

1. **Фасад + пакет**: `dbmigration.py` остаётся тонким фасадом (`main()`, `build_parser`,
   `__main__`-guard, re-export публичного API). Тесты (`from dbmigration import ...`),
   README-примеры и Dockerfile продолжают работать без правок.
2. Полное разложение кода по модулям/пакетам (без «всё в одном файле»).
3. `textual` → обязательная зависимость дев-окружения (`requirements-dev.txt`);
   в TUI-хендлере — ленивый `import`, чтобы CLI-запуск без textual не требовал пакет.
4. Опции команд — фиксированные переключатели в правой панели.
5. Стартовый проход — полный `verify` в центре.

---

## Целевая структура

```
dbmigrations/
  dbmigration.py          # ФАСАД: main(), build_parser, re-export, __main__ guard
  _constants.py           # все модульные константы (TOML_CONFIG_FILE, имена папок, паттерны, лимиты)
  _i18n.py                # единый gettext-объект: _(), setup_translations(lang) — глобальное переключение
  _config.py              # read_toml_config, get_default_dbenv, get_dbenv_config,
                          #   build_connection_settings, add_common_db_arguments
  _options.py             # CommonCliOptions + Update/Verify/Init/RunTestsOptions + Deps (перенос без изменений)
  _output.py              # Output Protocol, ConsoleOutput (== print, CLI-поведение прежнее),
                          #   QueueOutput (TUI, потокобезопасен, поддержка end='')
  _confirmation.py        # Confirmation Protocol, ConsoleConfirm (get_char внутри),
                          #   TuiConfirm (резолвится из модалки TUI)
  _db.py                  # DbConnection, log_server_notices (принимают Output)
  _scripts.py             # ScriptFsInfo/ScriptDbInfo, сортировка, script_list.txt, @depends_on,
                          #   cleanup, diff/comment-рендеры, resolve_relative_script_path
  _git.py                 # GitChecker, CommitInfo, blob-хелперы OID
  _tool.py                # ExternalTool (вывод → Output)
  _migrations.py          # OwnMigration, MigrationCheckForOlderVersionControlTables
  _launch.py              # run_command → launch_command(cmd_cls, opts, config, out, confirm)
                          #   — единый путь запуска для CLI и TUI
  _state.py               # StateProbe: read-only интроспекция состояния схемы/репо
  commands/
    __init__.py           # re-export для фасада
    base.py               # BaseCommand (общие SQL-хелперы, deps, сортировка, Output через Deps)
    init_command.py       # InitCommand
    update_command.py     # UpdateCommand (подтверждение через Confirmation)
    verify_command.py     # VerifyCommand + UpdateScriptBuilder
    run_tests_command.py  # RunTestsCommand
  tui/
    __init__.py
    app.py                # MainApp (Header, центр RichLog, правая панель, Footer)
    worker.py             # asyncio.to_thread + QueueOutput + drain-очередь в RichLog
    screens/
      main_screen.py      # центральный лог + правая командная панель
      confirm_screen.py   # модальное подтверждение (y/N)
    widgets/
      command_panel.py    # список команд + чекбоксы опций (из StateProbe)
      log_panel.py        # обёртка над RichLog / Rich
      status_footer.py    # спиннер, код возврата, stop
translations/             # без изменения структуры; пополнить словами TUI
```

---

## Этапы (каждый этап — зелёные 87 тестов)

### Этап 1. Выделение ядра без изменения поведения
- [ ] `_constants.py`: перенести все модульные константы.
- [ ] `_i18n.py`: единый объект `_()`/`setup_translations`; остальные модули импортируют `_` из него.
- [ ] `_config.py`, `_options.py`, `_db.py`, `_scripts.py`, `_git.py`, `_tool.py`, `_migrations.py`
      — перенос кода как есть (минимальные импорты между модулями).
- [ ] Фасад `dbmigration.py`: `from ._... import *` (явный список публичных имён) + `main()` + `build_parser` + `__main__`.
- [ ] Прогнать: `pytest unit_tests/ tests/` — 87 passed; CLI-вывод не изменился.

### Этап 2. Output-sink рефакторинг
- [ ] `_output.py`: `Output` protocol (`print(text='', end='\n', flush=False)`), `ConsoleOutput`, `QueueOutput`.
- [ ] Заменить все 112 вызовов `print(...)` в коде команд на `self.out.print(...)`
      (получают `Output` из `Deps`/конструктора):
  - [ ] `DbConnection.__enter__/__exit__` — output параметром конструктора;
  - [ ] `ExternalTool.run()` — вывод psql в `self.out`;
  - [ ] `log_server_notices` — output параметром;
  - [ ] diff-рендеры и `BaseCommand` — через `self.out`.
- [ ] Проверить: CLI-вывод совпадает с эталонным (diff e2e-выводов); 87 тестов зелёные.

### Этап 3. Confirmation + launch
- [ ] `_confirmation.py`: `Confirmation.confirm(message) -> bool`; `ConsoleConfirm` = текущее поведение `get_char()`.
- [ ] `UpdateCommand._run` использует `Confirmation` (убрать прямой `get_char` из команды).
- [ ] `_launch.py`: из `run_command` (сейчас dbmigration.py:3323) выделить
      `launch_command(cmd_cls, opts, config, out, confirm) -> int`; CLI-хендлеры без изменений.
- [ ] 87 тестов зелёные.

### Этап 4. Разложение команд
- [ ] `commands/base.py` — `BaseCommand`; `commands/init_command.py`, `update_command.py`,
      `verify_command.py` (с `UpdateScriptBuilder`), `run_tests_command.py`.
- [ ] `commands/__init__.py` + фасад re-exportит `InitCommand/UpdateCommand/VerifyCommand/RunTestsCommand/
      BaseCommand/UpdateScriptBuilder`.
- [ ] 87 тестов зелёные (тесты не трогаем).

### Этап 5. StateProbe (read-only)
- [ ] `_state.py`: dataclass состояния, источник — существующие проверки:
  - [ ] `check_if_schema_exists` (dbmigration.py:1234);
  - [ ] `check_if_all_version_control_tables_exist / do_not_exist` (dbmigration.py:1500-1521) —
        без raising, только bool;
  - [ ] `check_if_schema_is_empty` (InitCommand);
  - [ ] `get_baseline_version_installed` (dbmigration.py:2138), `get_latest_version_installed` (dbmigration.py:1354);
  - [ ] структура репозитория: `baseline/versions/repeatable/tests`, `target_version.txt`,
        `target_environment_id.txt`, `set_search_path.txt`;
  - [ ] является ли репозиторий git-репо (для diff-опций).
- [ ] Юнит-тесты StateProbe (обращения к заглушкам, без БД).

### Этап 6. TUI-приложение
- [ ] `tui/widgets/log_panel.py` — RichLog с живым потоком строк.
- [ ] `tui/widgets/command_panel.py` — команды из StateProbe + чекбоксы опций (см. UX).
- [ ] `tui/screens/confirm_screen.py` — подтверждение y/N для `update`.
- [ ] `tui/worker.py` — `asyncio.to_thread(launch_command, ...)`; `QueueOutput` → `asyncio.Queue`
      → drain в RichLog; обработка status/exit code/stop.
- [ ] `tui/app.py` — MainApp: Header (схема/dbenv/подключение), центр-лог, правая панель,
      Footer (спиннер, код возврата), биндинги (run/confirm/cancel/quit, refresh).
- [ ] subcommand `tui` в `build_parser` + `_tui_handler` (ленивый import tui, валидация пути/аргументов).
- [ ] После `init`/`update` — повторный StateProbe и обновление правой панели.

### Этап 7. Переводы
- [ ] Новые строки TUI (заголовки, кнопки, лейблы, подсказки, подтверждения) — через `_()`.
- [ ] Обновить `translations/messages.pot`, `translations/ru/LC_MESSAGES/messages.po` (включая
      переводы), пересобрать `messages.mo`: `pybabel compile -d translations -l ru`.
- [ ] Обновить ref-комментарии `#: dbmigration.py:NNNN` в `.pot` (номера строк изменились после разложения).

### Этап 8. Документация
- [ ] `README.md` / `README.ru.md`: команда `tui` (запуск, панель, работа), про опции/клавиши.
- [ ] `AGENTS.md`: новая структура пакета, правило «dbmigration.py — фасад;
      новые модули в пакете», синхронизация переводов, как запускать TUI.
- [ ] Dockerfile/`requirements-dev.txt`: добавлен `textual` (из решений).

### Этап 9. Тесты TUI и регрессия
- [ ] Юниты: `QueueOutput` (частичные строки/`end=''`, потокобезопасность),
      `ConsoleConfirm`/`TuiConfirm`, `StateProbe`, `launch_command` без потока.
- [ ] Юниты команд через mock `Output` (как сейчас `__get__(cmd)` binding в test_verify_command.py).
- [ ] TUI: `app.run_test()` / Pilot — рендер экранов, выбор команды, чекбоксы, подтверждение;
      headless, без БД.
- [ ] Полная регрессия: `pytest unit_tests/ tests/` — 87 старых + новые зелёные.

---

## Риски и подводные камни

- Прямой `print()` во внешних библиотеках не попадает в лог — только через `Output`.
- `log_server_notices` вызывается из psycopg внутри запроса — потокобезопасный, не блокирующий.
- `get_char()` нельзя использовать в TUI (терминал занят Textual) — только `Confirmation`.
- psycopg синхронный → команды только в фоновом потоке; «stop» — graceful между операциями,
  не мгновенное прерывание.
- Крупные diff-ы: рендерить с усечением, не тормозить event loop.
- Фасад должен re-exportить всё, что публично импортируют тесты (проверить grep по
  `from dbmigration import`).
- Распутать круговые импорты: константы/i18n — без зависимостей вниз.

---

## Основной чеклист (сводно)

1. [ ] Ядро-модули + фасад (этап 1)
2. [ ] Output-sink (этап 2)
3. [ ] Confirmation + launch_command (этап 3)
4. [ ] commands/* (этап 4)
5. [ ] StateProbe (этап 5)
6. [ ] TUI app/screens/widgets + subcommand tui (этап 6)
7. [ ] Переводы (.pot/.po/.mo) (этап 7)
8. [ ] README/AGENTS/requirements (этап 8)
9. [ ] Тесты TUI + регрессия 87 (этап 9)