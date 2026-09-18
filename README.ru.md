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

Сами скрипты деплоя часто засоряются boilerplate-кодом. Отдельный инструмент убирает его и стандартизирует процесс внесения изменений:

* **Автоматическое управление транзакциями**: Без явных `BEGIN`/`COMMIT`.
* **Встроенный контроль версий**: История и хэш-суммы в служебных таблицах.
* **Перехват ошибок**: Корректная обработка исключений.
* **Безопасность процессов**: Стандартизация, исключающая человеческий фактор.

По сути это пример реализации, который можно форкнуть и настроить под свои нужды.

---

## 🚀 Системные требования и установка

* **Python 3.11+** (в первую очередь из-за пакета `tomllib`, используемого для парсинга конфигурации)
* Библиотека **psycopg**

```powershell
python3.exe -m venv .venv
.\.venv\Scripts\Activate.ps1
python3.exe -m pip install -r requirements-dev.txt
```

> ⚠️ **Важное замечание по безопасности**: Пароль пользователя **обязан** передаваться через переменную окружения `USER_PASSWORD`.

---

## 📁 Структура репозитория миграций

Репозиторий скриптов — это папка (обычно Git-репозиторий) со следующей структурой:

* `baseline/V000/` — Базовые скрипты (анонимизированный дамп продакшн-БД). Применяется один раз;
* `versions/V001...VNNN/` — Версионируемые, инкрементальные скрипты (основные таблицы, данные). Применяются один раз по возрастанию версий;
* `repeatable/` — Идемпотентные скрипты (view, trigger, function, конфигурационные данные). Переприменяются при каждом `update`, если изменились;
* `tests/` — SQL-тесты (см. [Юнит-тестирование базы данных](#мед-тестирование-базы-данных-run-tests));

Порядок выполнения внутри папки задаётся файлом `script_list.txt`, если он есть, иначе — алфавитной сортировкой имён скриптов.

---

## 💻 Интерфейс командной строки и использование

У инструмента 4 подкоманды: `init`, `update`, `verify`, `run-tests`.

### 1. Инициализация (`init`)
Создает служебные таблицы в пустой схеме.
```bash
\$env:USER_PASSWORD="topsecret123"
python3 .\dbmigrations\dbmigration.py init test2 .\dbmigrations\samples\test1\
```

### 2. Накатывание миграций (`update`)
Последовательно применяет изменения.
```bash
python3 .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\ --skip-confirmation
```

### 3. Проверка изменений (`verify`)
Проверяет согласованность и выводит список скриптов. По умолчанию под каждой записью печатается unified diff (применённая в БД версия по git OID vs. текущий файл в репозитории); флаг `--skip-diffs` оставляет только список скриптов. Генерирует один общий скрипт обновления для ревью администраторами БД. Показывает историю последних изменений в схеме БД сгруппированную по Git-коммитам.
```bash
python3 .\dbmigrations\dbmigration.py verify test2 .\dbmigrations\samples\test1\ --build-update-script review_patch.sql
```

### 4. Запуск юнит-тестов (`run-tests`)
Запускает тестовые SQL-скрипты из папки `tests`.
```bash
python3 .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
```

Выполните `python3 .\dbmigration.py -h` для просмотра глобальной справки или см. полный [справочник флагов](#справочник-по-флагам-командной-строки) ниже.

---

## 🏁 Быстрый старт

Развернём [пример репозитория](./dbmigrations/samples/test1) по шагам.

### 1. Создайте пустую схему

```bash
CREATE SCHEMA test2;
```

### 2. Инициализируйте схему служебными таблицами

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> $env:USER_PASSWORD="topsecret123"
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py init test2 .\dbmigrations\samples\test1\
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test2'.
Creating the version control tables with environment ID: '4a40342c-4546-4776-bf97-b02b2a858924'
Created.
Closed db connection.
```

Первый аргумент — имя целевой схемы (`test2`), второй — путь к репозиторию скриптов.
Адрес сервера и учётные данные берутся из `dbmigration.toml` (см. [Конфигурация](#конфигурация)); пароль обязательно передаётся через переменную окружения `USER_PASSWORD`.

### 3. Накатите миграции

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\ --skip-confirmation
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test2'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Performing updates from scripts repository: '/workspace/dbmigrations/samples/test1'
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
The baseline version to install V000.
Apply baseline scripts...
Running script: [test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]...
Committed.
Running script: [test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]...
Committed.
Setting the baseline version to: 'V000'.
Committed.
Baseline scripts applied.
The latest installed version is V000.
Found 2 new versions for installation.
Apply versioned scripts...
Apply version V001...
Running script: [test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]...
Running script: [test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]...
Committed.
Apply version V002...
Running script: [test1/versions/V002/dummy.sql (OID: 384d538d)]...
Committed.
Versioned scripts applied.
Check repeatable scripts...
Target version matches the latest installed version: 'V002'.
Found 2 scripts to re-run
Apply repeatable scripts...
Running script: [test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]...
Server: NOTICE - view "latest_t1" does not exist, skipping
Committed.
Running script: [test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]...
Server: NOTICE - view "max_t2_kk" does not exist, skipping
Committed.
Repeatable scripts applied.
Updated.
Closed db connection.
```

### 4. Посмотрите результат в базе данных

```bash
psql -h localhost -U postgres -d test1
```

```sql
SET search_path TO test2;
SELECT * FROM test2.dbmigration_versions ORDER BY version_id;
```

```
 version_id | is_baseline |          created_at           | created_by | created_from
------------+-------------+-------------------------------+------------+--------------
 V000       | t           | 2026-09-18 07:10:23.835956+00 | postgres   | ::1
 V001       | f           | 2026-09-18 07:10:24.041018+00 | postgres   | ::1
 V002       | f           | 2026-09-18 07:10:24.136315+00 | postgres   | ::1
(3 rows)
```

Каждый установленный скрипт отслеживается по его Git OID, поэтому инструмент всегда знает, что именно было применено:

```sql
SELECT version_id, relative_path, left(git_blob_sha1, 10) AS oid
FROM test2.dbmigration_version_scripts
ORDER BY version_id, relative_path;
```

```
 version_id |               relative_path               |    oid
------------+-------------------------------------------+------------
 V000       | test1/baseline/V000/00_create_t1.sql      | 9bdf76b3fe
 V000       | test1/baseline/V000/01_insert_into_t1.sql | 2d3fb16951
 V001       | test1/versions/V001/00_create_t2.sql      | a3e53fb686
 V001       | test1/versions/V001/01_insert_into_t2.sql | ff5717bdc4
 V002       | test1/versions/V002/dummy.sql             | 384d538d26
(5 rows)
```

```sql
SELECT version_id, relative_path, left(git_blob_sha1, 10) AS oid
FROM test2.dbmigration_repeatable_scripts
ORDER BY relative_path;
```

```
 version_id |                 relative_path                 |    oid
------------+-----------------------------------------------+------------
 V002       | test1/repeatable/00_create_view_latest_t1.sql | 1504cd9a01
 V002       | test1/repeatable/01_create_view_max_t2_kk.sql | 1278759c33
(2 rows)
```

---

## 📝 Code Review и предпросмотр (`verify --build-update-script`)

`verify` проверяет согласованность репозитория скриптов и перечисляет всё, что будет применено к базе командой `update`. Опция `--build-update-script` дополнительно собирает все обновления в единый транзакционно-безопасный SQL-скрипт для ревью администратором БД.

Проверим свежеинициализированную схему:

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py verify test3 .\dbmigrations\samples\test1\ --build-update-script xx.sql
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test3'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
Baseline scripts to install:
[64b571a4] 2026-03-30 - intermediate results
  Author: Andrey Lartsev
    [test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]
    + New file, will be applied in full.
[c24a3a35] 2026-03-30 - added sample script repo
  Author: Andrey Lartsev
    [test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
    + New file, will be applied in full.
Versioned scripts to install:
[64b571a4] 2026-03-30 - intermediate results
  Author: Andrey Lartsev
    [test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]
    + New file, will be applied in full.
    [test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]
    + New file, will be applied in full.
[36ceff6c] 2026-04-21 - initial implementation of added field version_id to dbmigration_repeatable table
  Author: Andrey Lartsev
    [test1/versions/V002/dummy.sql (OID: 384d538d)]
    + New file, will be applied in full.
No versions are installed in the database schema.
Target version for repeatable scripts: 'V002'.
Repeatable scripts to (re)install:
[563cf87e] 2026-07-21 - more use of ScriptInfo
  Author: Andrey Lartsev
    [test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]
    + New file, will be applied in full.
[2afeb5db] 2026-07-21 - more use of script info
  Author: Andrey Lartsev
    [test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]
    + New file, will be applied in full.
Update script is written to 'xx.sql'.
Closed db connection.
```

> Новые скрипты, которые ещё ни разу не применялись, показываются свёрнуто: `+ New file, will be applied in full.`. Для уже применённых скриптов `verify` печатает unified diff применённой версии (по git OID) и текущего файла в репозитории. Используйте `--skip-diffs`, чтобы скрыть diff-ы.

Итоговый скрипт содержит все необходимые изменения: транзакционные блоки для безопасности и вставки в служебные таблицы версий:

```sql
-- Setting session search path to: test3
SELECT pg_catalog.set_config('search_path', 'test3', false);

-- --------- BASELINE VERSION: V000 ---------
BEGIN;
-- Apply script: [test1/baseline/V000/00_create_t1.sql (OID:9bdf76b3)]
create table t1 (
    v1 serial not null primary key
);
-- End of script.
COMMIT;
BEGIN;
-- Apply script: [test1/baseline/V000/01_insert_into_t1.sql (OID:2d3fb169)]
insert into t1 values (1);
insert into t1 values (2);
-- End of script.
COMMIT;
BEGIN;
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'test1/baseline/V000/00_create_t1.sql','9bdf76b3fe019f97e6cd603db08cb869e64896a6');
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'test1/baseline/V000/01_insert_into_t1.sql','2d3fb169511cf4596557955a64a4afbb770b5c16');
COMMIT;
-- --------- VERSION: V001 ---------
BEGIN;
-- Apply script: [test1/versions/V001/00_create_t2.sql (OID:a3e53fb6)]
create table t2 (
    kk varchar(36) not null primary key,
    created_at timestamp with time zone not null default current_timestamp
);
-- End of script.
-- Apply script: [test1/versions/V001/01_insert_into_t2.sql (OID:ff5717bd)]
insert into t2 values ('1');
insert into t2 values ('2');
-- End of script.
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V001', FALSE);
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'test1/versions/V001/00_create_t2.sql','a3e53fb6862ad9782f091a89482fb105f19799df');
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'test1/versions/V001/01_insert_into_t2.sql','ff5717bdc405de2b9f7ae50f3b7b0896d3a59071');
COMMIT;
-- --------- VERSION: V002 ---------
BEGIN;
-- Apply script: [test1/versions/V002/dummy.sql (OID:384d538d)]
DO $$
BEGIN
    NULL;
END
$$;
-- End of script.
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V002', FALSE);
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V002', 'test1/versions/V002/dummy.sql','384d538d26551be2c6c697c832c209e84c2a73d2');
COMMIT;
-- --------- REPEATABLE SCRIPTS FOR VERSION: V002 ---------
BEGIN;
-- Apply script: [test1/repeatable/00_create_view_latest_t1.sql (OID:1504cd9a)]
drop view if exists latest_t1;

create view latest_t1 as
    select max(v1) as v1 from t1;
-- End of script.
INSERT INTO "test3".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('1504cd9a0133594c04438c9022acce4aa1e60a33', 'V002', 'test1/repeatable/00_create_view_latest_t1.sql');
COMMIT;

BEGIN;
-- Apply script: [test1/repeatable/01_create_view_max_t2_kk.sql (OID:1278759c)]
drop view if exists max_t2_kk;

create view max_t2_kk as
    select max(kk) as kk from t2;
-- End of script.
INSERT INTO "test3".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('1278759c33e8b9349e663656c998023afb5491ea', 'V002', 'test1/repeatable/01_create_view_max_t2_kk.sql');
COMMIT;
```

После code review патч можно применить обычным `psql`:

```bash
psql -U postgres test1 -f xx.sql
```

---

## 🧪 Юнит-тестирование базы данных (`run-tests`)

Тестовые скрипты лежат в подпапке `tests/` репозитория скриптов. Каждый тест выполняется в собственной транзакции, которая затем откатывается; данные, подготовленные одним тестом, не «протекают» в другие.

Есть три типа тестовых скриптов:

1. С префиксом `assure_that_` — должны просто завершиться без ошибок;
2. С префиксом `is_true_that_` — должны вернуть одну запись с одним булевым значением `true`;
3. С префиксом `detect_missing_` — должны вернуть пустой результирующий набор.

Специальный скрипт `_setup.sql` готовит тестовые данные. Инструмент создаёт savepoint перед каждым setup-скриптом и откатывается к нему после.

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test2'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Running unit tests on scripts repository: '/workspace/dbmigrations/samples/test1'
Target version matches the latest installed version: 'V002'
Make savepoint...
Running setup: 'test1/tests/_setup.sql'...DONE
Running test: 'test1/tests/is_true_that_setup_data_is_populated.sql'...PASS
Make savepoint...
Running setup: 'test1/tests/table_t1/_setup.sql'...DONE
Running test: 'test1/tests/table_t1/is_true_that_setup_data_is_populated.sql'...PASS
Running test: 'test1/tests/table_t1/assure_that_t1_exists.sql'...PASS
Running test: 'test1/tests/table_t1/detect_missing_t1_records.sql'...FAIL. (2) Missing records:
=================================
id: 33
FAIL. Expected no results!
Rolled back to savepoint.
Running test: 'test1/tests/table_t2/assure_that_t2_is_ok.sql'...PASS
Running test: 'test1/tests/table_t2/detect_missing_t2_records.sql'...PASS
Running test: 'test1/tests/view_latest_t1/assure_that_view_latest_t1_exists.sql'...PASS
Running test: 'test1/tests/view_latest_t1/is_true_that_view_latest_t1_returns_max_value.sql'...PASS
Rolled back transaction.
Closed db connection.
Command error: Tests failed: 1, passed: 8.
```

---

## ⚙️ Конфигурация

Инструмент читает настройки из файла `dbmigration.toml`, лежащего рядом с `dbmigration.py`:

* `default_dbenv` — имя группы окружения базы данных, используемой, когда не передан `--dbenv`;
* `[dbenvs.<name>]` — группы окружений базы данных. Каждая группа может содержать любые опции подключения libpq/psycopg (`host`, `port`, `dbname`, `user`, `connect_timeout`, ...) плюс специфичные опции инструмента, например `no_password` или `run_tests_by`;
* `[options]` — маски файлов скриптов (`file_glob_filters`), кодировка скриптов (`file_read_encoding`, `file_read_encoding_errors`) и язык интерфейса (`language`, например `"ru"`);
* `[tools.<name>]` — внешние инструменты для применения дампов baseline (`psql`, `pg_restore`), используемые, когда в подпапке baseline есть файл `use_tool.txt`.

Параметры подключения можно переопределить из командной строки: `--host`, `--port`, `--dbname`, `--user`, `-n/--no-password`. Пароль пользователя читается из переменной окружения `USER_PASSWORD` и не должен храниться в файле конфигурации.

---

## ❓ Справочник по флагам командной строки

Глобальная справка:

```
usage: dbmigration.py [-h] {update,verify,init,run-tests} ...

Simple database migrations tool

positional arguments:
  {update,verify,init,run-tests}
                        Available subcommands
    update              Applies base, versioned, and repeatable scripts to the
                        target database schema.
    verify              Validates the target schema and lists versioned and
                        reproducible scripts to apply if the 'update' command
                        is executed.
    init                Creates version control tables in an empty database
                        schema.
    run-tests           Runs db unit test scripts to the target database
                        schema.

options:
  -h, --help            show this help message and exit
```

### `init`

```
usage: dbmigration.py init [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                           [--dbname DBNAME] [--user USER] [-n] [--force-init]
                           schema_name scripts_path

positional arguments:
  schema_name        the name of target database schema
  scripts_path       source scripts repository path

options:
  -h, --help         show this help message and exit
  --dbenv DBENV      db environment name within TOML config
  --host HOST        db server host name
  --port PORT        db server port
  --dbname DBNAME    database name
  --user USER        user name
  -n, --no-password  don't ask user password
  --force-init       Force create version control tables even on non empty
                     schema
```

> Примечание: для работы функций на основе git OID команды `verify` и `update` требуют доступную команду `git` и Git-репозиторий.

### `update`

```
usage: dbmigration.py update [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                             [--dbname DBNAME] [--user USER] [-n]
                             [--force-reapply-latest-version]
                             [--force-reapply-all-repeatable]
                             [--force-run-cleanup] [--skip-confirmation]
                             schema_name scripts_path

positional arguments:
  schema_name           the name of target database schema
  scripts_path          source scripts repository path

options:
  -h, --help            show this help message and exit
  --dbenv DBENV         db environment name within TOML config
  --host HOST           db server host name
  --port PORT           db server port
  --dbname DBNAME       database name
  --user USER           user name
  -n, --no-password     don't ask user password
  --force-reapply-latest-version
                        clean up the latest version within the database and
                        reapply the included *.sql scripts.
  --force-reapply-all-repeatable
                        reapply all repeatable scripts, regardless of changes.
  --force-run-cleanup   run the cleanup script before executing version-
                        specific scripts.
  --skip-confirmation   skip confirmation before executing updates.
```

### `verify`

```
usage: dbmigration.py verify [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                             [--dbname DBNAME] [--user USER] [-n]
                             [--skip-git-checks] [--skip-diffs]
                             [--skip-display-recent-changes]
                             [--build-update-script BUILD_UPDATE_SCRIPT]
                             schema_name scripts_path

positional arguments:
  schema_name           the name of target database schema
  scripts_path          source scripts repository path

options:
  -h, --help            show this help message and exit
  --dbenv DBENV         db environment name within TOML config
  --host HOST           db server host name
  --port PORT           db server port
  --dbname DBNAME       database name
  --user USER           user name
  -n, --no-password     don't ask user password
  --skip-git-checks     skip grouping changes by git commits
  --skip-diffs          skip unified text diffs between scripts applied in the
                        database (by git OID) and the current script files in
                        the repository
  --skip-display-recent-changes
                        skip display recent changes stored within target db
                        schema
  --build-update-script BUILD_UPDATE_SCRIPT
                        the update script path if you want one as an
                        additional result of the verify command
```

`verify` по умолчанию выводит unified diff-ы: под каждой записью скрипта показываются различия между версией, применённой в БД (по git OID), и текущим содержимым в репозитории. Флаг `--skip-diffs` отключает вывод diff-ов, оставляя только список скриптов. Новые (ещё не применённые) скрипты сворачиваются в строку `+ New file, will be applied in full.` Для отображения diff-ов требуется Git-репозиторий и доступная команда `git`.

### `run-tests`

```
usage: dbmigration.py run-tests [-h] [--dbenv DBENV] [--host HOST]
                                [--port PORT] [--dbname DBNAME] [--user USER]
                                [-n] [--skip-env-checks]
                                schema_name scripts_path

positional arguments:
  schema_name        the name of target database schema
  scripts_path       source scripts repository path

options:
  -h, --help         show this help message and exit
  --dbenv DBENV      db environment name within TOML config
  --host HOST        db server host name
  --port PORT        db server port
  --dbname DBNAME    database name
  --user USER        user name
  -n, --no-password  don't ask user password
  --skip-env-checks  Skip version and environment ID checks to run tests in
                     any plain environment not made by the tool itself
```

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
avl@n-LarcevAV:~/WinProjects/dbmigrations/dbmigrations$ docker run --rm -e LC_ALL="ru" -e PGPASSWORD="***" -v "/home/avl/WinProjects/dbmigrations:/repo" -w /repo dbmigration verify test3 dbmigrations/samples/test1 --host host.docker.internal
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