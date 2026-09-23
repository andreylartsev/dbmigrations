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
- **Состояние воркера в Footer**: `idle / running <текущий скрипт> / cancel requested` — видно, на чём остановка,
  даже когда psycopg выполняет один большой SQL без промежуточного вывода.
- **Подсветка имени текущего скрипта в логе**: при запуске каждого скрипта в центр выводится заметная строка
  (оверрайт есть в сообщениях `apply_*`), чтобы без построчного прогресса было понятно, что выполняется сейчас.

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
      status_footer.py    # спиннер, код возврата, stop, состояние воркера
translations/             # без изменения структуры; пополнить словами TUI
```

---

## Этапы (каждый этап — зелёные 87 тестов)

### Этап 1. Выделение ядра без изменения поведения
- [x] `_constants.py`: перенести все модульные константы.
- [x] `_i18n.py`: единый объект `_()`/`setup_translations`; остальные модули импортируют `_` из него.
- [x] `_config.py`, `_options.py`, `_db.py`, `_scripts.py`, `_git.py`, `_tool.py`, `_migrations.py`
      — перенос кода как есть (минимальные импорты между модулями).
- [x] Фасад `dbmigration.py`: `from ._... import *` (явный список публичных имён) + `main()` + `build_parser` + `__main__`.
- [x] Прогнать: `pytest unit_tests/ tests/` — 87 passed; CLI-вывод не изменился.

### Этап 2. Output-sink рефакторинг
- [x] `_output.py`: `Output` protocol (`print(text='', end='\n', flush=False)`), `ConsoleOutput`, `QueueOutput`.
- [x] Заменить все 112 вызовов `print(...)` в коде команд на `self.out.print(...)`
      (получают `Output` из `Deps`/конструктора):
  - [x] `DbConnection.__enter__/__exit__` — output параметром конструктора;
  - [x] `ExternalTool.run()` — вывод psql в `self.out`;
  - [x] `log_server_notices` — output параметром;
  - [x] diff-рендеры и `BaseCommand` — через `self.out`.
      (u-тесты биндят методы на MagicMock → сайты = `(self.__dict__.get('out') or Output()).print(...)`)
- [x] Проверить: CLI-вывод совпадает с эталонным (diff e2e-выводов); 87 тестов зелёные.

### Этап 3. Confirmation + launch
- [x] `_confirmation.py`: `Confirmation.confirm(message) -> bool` (+`__call__`); `ConsoleConfirm` = текущее поведение `get_char()`.
- [x] `UpdateCommand._run` использует `Confirmation` (убрать прямой `get_char` из команды).
- [x] `_launch.py`: из `run_command` выделить
      `launch_command(cmd_cls, opts, config, out=None, confirm=None, use_run_tests_by_user=False) -> int`;
      CLI-хендлеры без изменений.
- [x] 87 тестов зелёные.

### Этап 4. Разложение команд
- [x] `commands/base.py` — `BaseCommand`; `commands/init_command.py`, `update_command.py`,
      `verify_command.py` (с `UpdateScriptBuilder`), `run_tests_command.py` (+ `TestFailed`).
- [x] `commands/__init__.py` + фасад re-exportит `InitCommand/UpdateCommand/VerifyCommand/RunTestsCommand/
      BaseCommand/UpdateScriptBuilder/TestFailed`.
- [x] `ExternalTool` вынесен в `_tool.py`; `TOOL_*` читаются через модуль `dbmigration` на вызове
      (`.replace` на `dbmigration.TOOL_*`), чтобы u-тестовый патч `dbmigration.TOOL_*` продолжал работать.
      В фасаде у `__main__`: `sys.modules.setdefault("dbmigration", sys.modules["__main__"])` до импорта `_tool`.
- [x] 87 тестов зелёные (тесты не трогаем).

### Этап 5. StateProbe (read-only)
- [x] `_state.py`: `State` dataclass + `StateProbe` (read-only, non-raising).
  - [x] `schema_exists`, `schema_is_empty`, `control_tables_exist`
        (через `VERSION_CONTROL_TABLE_NAMES`, вынесен в `_constants`; base.py использует его);
  - [x] `latest_version_installed`, `baseline_version_installed`;
  - [x] структура репозитория: `baseline/versions/repeatable/tests`, `target_version.txt`,
        `target_environment_id.txt`, `set_search_path.txt`, `.git`-маркер (в самом пути или родителях);
  - [x] computed: `connection_ok`, `initialized`, `can_init/can_update/can_verify/can_run_tests`.
- [x] `DbConnection` получил `quiet: bool = False` (подавление lifecycle-принтов для probe).
- [x] Юнит-тесты `unit_tests/test_state.py` (13 шт., stub `DbConnection`, без БД).
- [x] Проверено вживую: probe по реальной БД до/после `init` (samples/test1).

### Этап 6. TUI-приложение
- [x] `tui/widgets/log_panel.py` — RichLog с живым потоком строк (+ подсветка строк `Running script:`/`Run migration:`).
- [x] `tui/widgets/command_panel.py` — команды из StateProbe + чекбоксы опций (см. UX);
      `update_state()` обновляет существующие виджеты (без пересоздания, чтобы не ломать id).
- [x] `tui/screens/confirm_screen.py` — подтверждение y/N для `update` (`ModalScreen[bool]`).
- [x] `tui/worker.py` — `asyncio.to_thread(launch_command, ...)`; `QueueOutput` → `asyncio.Queue`
      → drain в RichLog; завершение — sentinel в очереди; обработка error/exit code/stop.
- [x] `tui/widgets/status_bar.py` — состояние воркера `idle / running <текущий скрипт> /
      cancel requested`, спиннер, код возврата (название `status_bar.py`, не `status_footer.py`).
- [x] Подсветка текущего скрипта в логе: `LogPanel.parse_script_name()` + StatusBar.current_script.
- [x] `tui/app.py` — MainApp: Header (схема/путь), центр-лог, правая панель,
      StatusBar + Footer, биндинги (ctrl+r run, ctrl+c cancel, r refresh, q quit),
      авто-`verify` при открытии (флаг `auto_verify`, если контрольные таблицы существуют).
- [x] subcommand `tui` в `build_parser` + `_tui_handler` (ленивый import tui, общие DB-аргументы).
- [x] После `init`/`update` — повторный StateProbe и обновление правой панели
      (`on_command_finished` → `refresh_state`).
- [x] 110 тестов зелёные (87 старых + 23 новых).

### Этап 7. Переводы
- [x] Новые строки TUI (заголовки, кнопки, лейблы, подсказки, подтверждения) — через `_()`
      (проверено: `setup_translations('ru')` до импорта tui даёт переведённые ENTRIES/BINDINGS).
- [x] `translations/messages.pot` пересоздан через `pybabel extract --input-dirs=. --ignore-dirs=unit_tests,tests,translations`
      (243 msgid, ref-комментарии по новым модулям: commands/, tui/, _*.py).
- [x] `translations/ru/LC_MESSAGES/messages.po` обновлён `pybabel update` + дописаны отсутствовавшие переводы (29 шт.),
      пересобран `messages.mo` (`pybabel compile -d translations -l ru`).
- [x] Ref-комментарии `#: dbmigration.py:NNNN` обновлены (теперь указывают на актуальные модули/строки).

### Этап 8. Документация
- [x] `README.md` / `README.ru.md`: команда `tui` (запуск, панель, работа), про опции/клавиши.
- [x] `AGENTS.md`: новая структура пакета, правило «dbmigration.py — фасад;
      новые модули в пакете», синхронизация переводов, как запускать TUI.
- [x] Dockerfile/`requirements-dev.txt`: добавлен `textual` (из решений); Dockerfile
      копирует все модули пакета + `commands/` + `tui/` + translations.

### Этап 9. Тесты TUI и регрессия
- [x] Юниты: `QueueOutput` (частичные строки/`end=''`), `TuiConfirm` (модальная резолюция),
      `CommandRunner` (drain/sentinel, error-path), `LogPanel.parse_script_name`.
- [x] TUI: `app.run_test(size=...)` / Pilot — рендер, кнопки enable/disable по StateProbe,
      запуск команды по кнопке, авто-`verify` при старте, quit; headless, без БД (всё в `unit_tests/test_tui.py`, 10 тестов).
- [x] Полная регрессия: `pytest unit_tests/ tests/` — **110 passed** (87 старых + 23 новых).
- [ ] (опц.) Проверить модал `ConfirmScreen` реально открывается при `update` без `--skip-confirmation`
      (Pilot с `TuiConfirm` + реальный `UpdateCommand` на stub-output).

---

## Риски и подводные камни

- Прямой `print()` во внешних библиотеках не попадает в лог — только через `Output`.
- `log_server_notices` вызывается из psycopg внутри запроса — потокобезопасный, не блокирующий.
- `get_char()` нельзя использовать в TUI (терминал занят Textual) — только `Confirmation`.
- psycopg синхронный → команды только в фоновом потоке; «stop» — graceful между операциями,
  не мгновенное прерывание.
- Пока psycopg выполняет один большой SQL (без промежуточного вывода), новых строк в лог нет;
  фокус на «состоянии воркера + подсветке текущего скрипта», чтобы это не выглядело зависанием.
- Крупные diff-ы: рендерить с усечением, не тормозить event loop.
- Фасад должен re-exportить всё, что публично импортируют тесты (проверить grep по
  `from dbmigration import`).
- Распутать круговые импорты: константы/i18n — без зависимостей вниз.

---

## Основной чеклист (сводно)

1. [x] Ядро-модули + фасад (этап 1)
2. [x] Output-sink (этап 2)
3. [x] Confirmation + launch_command (этап 3)
4. [x] commands/* (этап 4)
5. [x] StateProbe (этап 5)
6. [x] TUI app/screens/widgets + subcommand tui (этап 6)
7. [x] Переводы (.pot/.po/.mo) (этап 7)
8. [x] README/AGENTS/requirements (этап 8)
9. [x] Тесты TUI + регрессия 110 (этап 9)