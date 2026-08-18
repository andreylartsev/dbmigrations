# Simple PostgreSQL Database Migration Tool

🌐 **Читать на других языках: [English](README.md)**

Инструмент автоматизации и управления миграциями для баз данных PostgreSQL. Позволяет разворачивать структуру БД напрямую из исходного кода, хранящегося в Git, и безопасно доставлять DDL/DML изменения.

---

## 📌 Назначение утилиты

* **Интеграция с Git**: Развертывание и отслеживание схем баз данных из Git.
* **CI/CD процессы**: Доставка изменений кода БД (DDL/DML) на тестовые и продакшн-среды.
* **Сохранение данных**: Инкрементальные обновления без потери данных.
* **Гибкость типов миграций**: Управление структурой и данными.
* **Инспекция изменений (Dry-Run)**: Предварительная проверка миграций.
* **Генерация SQL для Code Review**: Сборка единого SQL-патча для DBA.
* **Юнит-тестирование базы данных**: Запуск тестов в изолированных транзакциях с автооткатом.

---

## ❓ Зачем нужен отдельный инструмент?

* **Автоматическое управление транзакциями**: Без явных `BEGIN`/`COMMIT`.
* **Встроенный контроль версий**: История и хэш-суммы в служебных таблицах.
* **Перехват ошибок**: Корректная обработка исключений.
* **Безопасность процессов**: Стандартизация, исключающая человеческий фактор.

---

## 🚀 Системные требования и установка

* **Python 3.11+**
* Библиотека **psycopg**

```powershell
python3.exe -m venv .venv
.\.venv\Scripts\Activate.ps1
python3.exe -m pip install -r requirements.txt
```

> ⚠️ **Важное замечание по безопасности**: Пароль пользователя **обязан** передаваться через переменную окружения `USER_PASSWORD`.

---

## 📁 Структура репозитория миграций

* `baseline/V000/` — Базовые скрипты инициализации.
* `versions/V001...VNNN/` — Версионируемые, инкрементальные скрипты.
* `repeatable/` — Идемпотентные скрипты (view, trigger, function).
* `tests/` — SQL-тесты.

---

## 💻 Интерфейс командной строки и использование

### 1. Инициализация (`init`)
Создает служебные таблицы в пустой схеме.
```bash
\$env:USER_PASSWORD="topsecret123"
python3 .\dbmigrations\dbmigration.py init test2 .\dbmigrations\samples\test1\
```

### 2. Накатывание миграций (`update`)
Последовательно применяет изменения.
```bash
python3 .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\
```

### 3. Проверка изменений (`verify`)
Проверяет согласованность и выводит список скриптов. Генерирует один общий скрипт обновления для ревью администраторами БД. Показывает историю последних изменений в схеме БД сгруппированную по Git-коммитам. 
```bash
python3 .\dbmigrations\dbmigration.py verify test2 .\dbmigrations\samples\test1\ --build-update-script review_patch.sql
```

### 4. Запуск юнит-тестов (`run-tests`)
Запускает тестовые SQL-скрипты из папки `tests`.
```bash
python3 .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
```

---

## ❓ Справка по флагам командной строки

Вызовите `python3 .\dbmigration.py -h` для просмотра глобальной справки.

**Ключевые флаги:**
* `init`: `--force-init`
* `verify`: `--skip-git-checks`, `--build-update-script`
* `update`: `--force-reapply-latest-version`, `--force-reapply-all-repeatable`, `--skip-confirmation`

## ❓ Как собирать и использовать Docker image


Сборка на Windows:

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker build .
[+] Building 4.8s (15/15) FINISHED                                                                                                                                                          docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                                                                        0.1s
 => => transferring dockerfile: 810B                                                                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/python:3.11-slim                                                                                                                                         0.9s
 => [internal] load .dockerignore                                                                                                                                                                           0.1s
 => => transferring context: 2B                                                                                                                                                                             0.0s
 => [internal] load build context                                                                                                                                                                           0.3s
 => => transferring context: 23.09kB                                                                                                                                                                        0.1s
 => [builder 1/6] FROM docker.io/library/python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7                                                                           0.5s
 => => resolve docker.io/library/python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7                                                                                   0.4s
 => CACHED [builder 2/6] WORKDIR /app                                                                                                                                                                       0.0s
 => CACHED [runner 3/6] RUN apt-get update && apt-get install -y --no-install-recommends     git     && rm -rf /var/lib/apt/lists/*                                                                         0.0s
 => CACHED [builder 3/6] RUN python -m pip install --no-cache-dir --upgrade pip     && python -m venv /opt/venv                                                                                             0.0s
 => CACHED [builder 4/6] COPY ../requirements-docker.txt .                                                                                                                                                  0.0s
 => CACHED [builder 5/6] COPY ../requirements-dev.txt .                                                                                                                                                     0.0s
 => CACHED [builder 6/6] RUN pip install --no-cache-dir -r ./requirements-docker.txt                                                                                                                        0.0s
 => CACHED [runner 4/6] COPY --from=builder /opt/venv /opt/venv                                                                                                                                             0.0s
 => CACHED [runner 5/6] RUN useradd --create-home appuser                                                                                                                                                   0.0s
 => [runner 6/6] COPY . .                                                                                                                                                                                   0.6s
 => exporting to image                                                                                                                                                                                      1.6s
 => => exporting layers                                                                                                                                                                                     0.8s
 => => exporting manifest sha256:59f6a061fc704b291eebbee778735eab2c53740b16519032cd844bd3d872a868                                                                                                           0.1s
 => => exporting config sha256:f934b243fae623df8a1b1d174531aa0ee5494470f00cc0aac97e5cf1173832f7                                                                                                             0.1s
 => => exporting attestation manifest sha256:3f54cae552ee4d243c0615c8c3eb3014a9ccf7180fd796f7fa6642c5c9e64da8                                                                                               0.2s
 => => exporting manifest list sha256:c405f3b0d2addb27c4b8948d994ceeafc06598b6d16534ec3f35863929f7e4a8                                                                                                      0.1s
 => => naming to moby-dangling@sha256:c405f3b0d2addb27c4b8948d994ceeafc06598b6d16534ec3f35863929f7e4a8                                                                                                      0.0s
 => => unpacking to moby-dangling@sha256:c405f3b0d2addb27c4b8948d994ceeafc06598b6d16534ec3f35863929f7e4a8                                                                                                   0.3s
```

Использование на Windows:

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker run --rm -e LC_ALL="ru" -e PGPASSWORD="***" -v "C:\Users\andrey.larcev\Projects\dbmigrations:/repo" -w /repo dbmigration verify test3 dbmigrations/samples/test1 --host host.docker.internal --skip-git-checks
Открыто соединение с БД: 'postgres@host.docker.internal:5432/test1'
Установлен путь поиска (search path) сессии: 'test3'.
Идентификатор среды целевой схемы совпадает с ID директории скриптов: 4a40342c-4546-4776-bf97-b02b2a858924
Выполняется перекрестная проверка согласованности между повторяемыми скриптами (repeatable) целевой версии и версионированными скриптами...
Завершено.
В целевой схеме установлена бэйзлайн-версия: V000
Последняя установленная версия: V002. Более новых скриптов для установки не найдено.
Целевая версия для повторяемых скриптов: 'V002'.
Измененных повторяемых скриптов для (повторной) установки не найдено.
Список недавних изменений целевой схемы:
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/dummy.sql (OID: 384d538d)]
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/_cleanup.sql (OID: 384d538d)]
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
Соединение с БД закрыто.
```
А так же использование в WSL:

``` bash
avl@n-LarcevAV:~/WinProjects/dbmigrations/dbmigrations$ docker run --rm -e LC_ALL="ru" -e PGPASSWORD="***" -v "/home/avl/WinProjects/dbmigrations:/repo" -w /repo dbmigration verify tes
t3 dbmigrations/samples/test1 --host host.docker.internal
Открыто соединение с БД: 'postgres@host.docker.internal:5432/test1'
Установлен путь поиска (search path) сессии: 'test3'.
Идентификатор среды целевой схемы совпадает с ID директории скриптов: 4a40342c-4546-4776-bf97-b02b2a858924
Выполняется перекрестная проверка согласованности между повторяемыми скриптами (repeatable) целевой версии и версионированными скриптами...
Завершено.
В целевой схеме установлена бэйзлайн-версия: V000
Последняя установленная версия: V002. Более новых скриптов для установки не найдено.
Целевая версия для повторяемых скриптов: 'V002'.
Измененных повторяемых скриптов для (повторной) установки не найдено.
Список недавних изменений целевой схемы:
[563cf87e] 2026-07-21 - more use of ScriptInfo
  Автор: Andrey Lartsev
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]
[2afeb5db] 2026-07-21 - more use of script info
  Автор: Andrey Lartsev
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]
[5361d3a9] 2026-06-19 - modified deps sample
  Автор: Andrey Lartsev
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]
[7eb51752] 2026-05-28 - updated readme.md
  Автор: Andrey Lartsev
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
[236b0868] 2026-05-18 - fixed running of _cleanup.sql within environment specific folders
  Автор: Andrey Lartsev
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/dummy.sql (OID: 384d538d)]
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/_cleanup.sql (OID: 384d538d)]
[d606d6a9] 2026-05-16 - added environment inheritance sample
  Автор: Andrey Lartsev
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]
Соединение с БД закрыто.
```
