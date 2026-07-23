import subprocess
import pytest
import shutil

# Проверяем наличие psql в системе перед запуском тестов
PSQL_PATH = shutil.which("psql")

@pytest.mark.skipif(not PSQL_PATH, reason="Утилита psql не найдена в PATH системы")
def test_recreate_schema_via_psql():
    """Тест проверяет успешное пересоздание схемы test3 через psql."""
    
    # Конструируем команду точно так же, как в терминале
    command = [
        "psql",
        "-U", "postgres",
        "-d", "test1",
        "-c", "DROP SCHEMA IF EXISTS test3 CASCADE; CREATE SCHEMA test3;"
    ]
    
    # Запускаем команду
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"  # Важно для корректного чтения кириллицы
    )
    
    # 1. Проверяем код возврата (0 означает, что psql отработал без ошибок)
    assert result.returncode == 0, f"Ошибка выполнения psql: {result.stderr}"
    
    # 2. Проверяем основной вывод (stdout) — успешное удаление и создание
    assert "DROP SCHEMA" in result.stdout
    assert "CREATE SCHEMA" in result.stdout
    
    # 3. Проверяем системные замечания PostgreSQL (stderr)
    # Если схема была не пустая, проверяем наличие ключевых слов
    if "ЗАМЕЧАНИЕ" in result.stderr:
        assert "удаление распространяется" in result.stderr
        assert "test3.dbmigration_environment_id" in result.stderr
        assert "test3.dbmigration_versions" in result.stderr
        assert "test3.dbmigration_version_scripts" in result.stderr
        assert "test3.dbmigration_repeatable_scripts" in result.stderr
