import argparse

# 1. Создаем парсер
parser = argparse.ArgumentParser(description="Описание вашей программы")

# 2. Добавляем аргументы
parser.add_argument("name", help="Имя пользователя") # Позиционный аргумент
parser.add_argument("-a", "--age", type=int, help="Возраст") # Опциональный аргумент
parser.add_argument("-v", "--verbose", action="store_true", help="Включить подробный вывод") # Флаг

# 3. Парсим аргументы
args = parser.parse_args()

# Использование
print(f"Привет, {args.name}!")
if args.age:
    print(f"Вам {args.age} лет.")
if args.verbose:
    print("Режим подробного вывода включен.")
