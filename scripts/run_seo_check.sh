#!/bin/bash
# run_seo_check.sh - Удобный запуск SEO проверки

cd "$(dirname "$0")/.."

echo "╔══════════════════════════════════════╗"
echo "║        🚀 SEO AGENT LAUNCHER         ║"
echo "╚══════════════════════════════════════╝"
echo ""
echo "📅 Дата: $(date '+%d.%m.%Y')"
echo "⏰ Время: $(date '+%H:%M:%S')"
echo ""

# Проверяем виртуальное окружение
if [ ! -d "venv" ]; then
    echo "❌ Ошибка: виртуальное окружение не найдено"
    echo "   Запустите: python3 -m venv venv"
    exit 1
fi

# Активируем venv
echo "🔧 Активация виртуального окружения..."
source venv/bin/activate

# Проверяем зависимости
echo "📦 Проверка зависимостей..."
if ! pip show pandas requests pyyaml > /dev/null 2>&1; then
    echo "⚠️  Предупреждение: установите зависимости:"
    echo "   pip install -r requirements.txt"
fi

# Проверяем конфигурацию
echo "⚙️  Проверка конфигурации..."
if [ ! -f "config/projects.yaml" ]; then
    echo "❌ Ошибка: файл config/projects.yaml не найден"
    exit 1
fi

# Показываем информацию о проектах
echo ""
echo "📊 ЗАГРУЖЕННЫЕ ПРОЕКТЫ:"
python3 -c "
import yaml
try:
    with open('config/projects.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    projects = config.get('projects', [])
    for i, p in enumerate(projects, 1):
        print(f'  {i}. {p.get(\"name\", \"Без имени\")}')
        print(f'     Домен: {p.get(\"domain\", \"Не указан\")}')
        keywords = p.get(\"keywords\", [])
        print(f'     Ключевых слов: {len(keywords)}')
        if keywords:
            print(f'     Пример: {keywords[0][:30]}...')
except Exception as e:
    print(f'  ❌ Ошибка загрузки конфигурации: {e}')
"

echo ""
read -p "▶️  Нажмите Enter для начала проверки (или Ctrl+C для отмены)..."

# Запускаем агента
echo ""
echo "╔══════════════════════════════════════╗"
echo "║        ЗАПУСК SEO ПРОВЕРКИ           ║"
echo "╚══════════════════════════════════════╝"
echo ""

# Запускаем главный файл (будет создан)
python3 seo_agent.py

# Сохраняем код завершения
exit_code=$?

echo ""
echo "╔══════════════════════════════════════╗"
echo "║         РЕЗУЛЬТАТЫ ЗАПУСКА           ║"
echo "╚══════════════════════════════════════╝"
echo ""

if [ $exit_code -eq 0 ]; then
    echo "✅ SEO проверка завершена успешно!"
    echo "📁 Отчёты сохранены в data/reports/"
    echo "📊 Последние данные в data/history/"
else
    echo "❌ SEO проверка завершилась с ошибкой (код: $exit_code)"
    echo "📋 Проверьте логи в папке logs/"
fi

echo ""
echo "🔄 Следующий запуск: когда потребуется"
echo ""