import subprocess
import sys
import re
import pytest

PYTHON_EXE = sys.executable
SCRIPT_PATH = r"./../dbmigration.py"
SAMPLE_PATH = r"./../samples/test1/"

def test_dbmigration_init_success():
    """Тест проверяет успешную инициализацию структуры миграций в схеме test3."""
    
    # Формируем команду на основе вашего вывода
    command = [
        PYTHON_EXE,
        SCRIPT_PATH,
        "init",
        "test3",
        SAMPLE_PATH
    ]
    
    # Запускаем скрипт миграций
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8"  # Важно для корректного чтения строк
    )
    
    # 1. Проверяем код возврата (0 — успех)
    assert result.returncode == 0, f"Скрипт завершился с ошибкой: {result.stderr}"
    
    # 2. Проверяем статические строки вывода (stdout)
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"

    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Строка подключения к БД не найдена или имеет неверный формат в выводе: {result.stdout}"

    #assert "Opened db connection: 'postgres@localhost:5432/test1'" in result.stdout
    assert "Set session search path to 'test3'." in result.stdout
    assert "Created." in result.stdout
    assert "Closed db connection." in result.stdout
    
    # 3. Проверяем динамическую строку с UUID через регулярное выражение
    # Паттерн ищет текст создания таблиц и валидный UUID формат (8-4-4-4-12 символов)
    uuid_pattern = r"Creating the version control tables with environment ID: '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'"
    assert re.search(uuid_pattern, result.stdout) is not None, "Строка с UUID среды не найдена или формат UUID неверный"
