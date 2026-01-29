#!/usr/bin/env python3
"""
Миграция данных из CSV в SQLite базу
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import sqlite3
import logging

# Добавляем путь к core
sys.path.append(str(Path(__file__).parent.parent))

from core.database import SEODatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_csv_to_sqlite():
    """Переносит данные из CSV файлов в SQLite базу"""
    
    print("🔄 МИГРАЦИЯ ДАННЫХ ИЗ CSV В SQLITE")
    print("=" * 50)
    
    # 1. Создаём/подключаем основную базу
    db_path = Path("data/seo_data.db")
    if db_path.exists():
        print(f"⚠️  База данных уже существует: {db_path}")
        print("   Хотите продолжить? (существующие данные будут сохранены)")
        response = input("   [y/N]: ").strip().lower()
        if response != 'y':
            print("❌ Миграция отменена")
            return
    
    db = SEODatabase(str(db_path))
    print(f"✅ База данных подключена: {db_path}")
    
    # 2. Читаем проекты из конфигурации
    import yaml
    config_path = Path("config/projects.yaml")
    
    if not config_path.exists():
        print(f"❌ Файл конфигурации не найден: {config_path}")
        return
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    projects = config.get('projects', [])
    print(f"📋 Найдено проектов в конфиге: {len(projects)}")
    
    migrated_count = 0
    
    for project in projects:
        project_name = project.get('name', 'Без имени')
        domain = project.get('domain', '')
        
        if not domain:
            print(f"⚠️  Пропускаем проект без домена: {project_name}")
            continue
        
        print(f"\n📊 Проект: {project_name}")
        print(f"🌐 Домен: {domain}")
        
        # 3. Создаём проект в базе
        project_id = db.get_or_create_project(project_name, domain)
        
        # 4. Добавляем ключевые слова
        keywords = project.get('keywords', [])
        print(f"🔑 Ключевых слов: {len(keywords)}")
        
        keyword_ids = {}
        for keyword in keywords:
            keyword_id = db.get_or_create_keyword(project_id, keyword)
            keyword_ids[keyword] = keyword_id
        
        # 5. Мигрируем данные из CSV
        csv_files = list(Path("data/history").glob(f"positions_*.csv"))
        
        for csv_file in csv_files:
            # Проверяем, относится ли файл к этому домену
            file_domain = csv_file.stem.replace("positions_", "").replace("_", ".")
            
            # Простая проверка совпадения домена
            if domain not in file_domain and file_domain not in domain:
                continue
            
            print(f"   📁 Мигрируем файл: {csv_file.name}")
            
            try:
                df = pd.read_csv(csv_file)
                
                for _, row in df.iterrows():
                    # Парсим дату и время
                    date_str = row.get('date', '')
                    time_str = row.get('time', '')
                    
                    if not date_str or not time_str:
                        continue
                    
                    # Находим keyword_id
                    keyword = row.get('keyword', '')
                    if keyword not in keyword_ids:
                        # Если ключевое слово не в конфиге, пропускаем
                        continue
                    
                    keyword_id = keyword_ids[keyword]
                    
                    # Сохраняем позицию
                    position = row.get('position')
                    if pd.isna(position):
                        position = None
                    
                    db.save_position(
                        project_id=project_id,
                        keyword_id=keyword_id,
                        check_date=date_str,
                        check_time=time_str,
                        position=int(position) if position else None,
                        url=row.get('url', ''),
                        total_results=row.get('total_results', 100),
                        search_engine=row.get('search_engine', 'yandex')
                    )
                
                migrated_count += 1
                print(f"   ✅ Файл мигрирован: {len(df)} записей")
                
            except Exception as e:
                print(f"   ❌ Ошибка миграции {csv_file.name}: {e}")
    
    # 6. Выводим статистику
    print(f"\n" + "=" * 50)
    print("📈 СТАТИСТИКА БАЗЫ ДАННЫХ ПОСЛЕ МИГРАЦИИ:")
    
    stats = db.get_database_stats()
    print(f"   Проектов: {stats.get('projects_count', 0)}")
    print(f"   Ключевых слов: {stats.get('keywords_count', 0)}")
    print(f"   Позиций: {stats.get('positions_count', 0)}")
    print(f"   Конкурентов: {stats.get('competitors_count', 0)}")
    print(f"   Размер базы: {stats.get('database_size_mb', 0):.2f} МБ")
    
    if migrated_count > 0:
        print(f"\n✅ Миграция завершена успешно!")
        print(f"   Перенесено файлов: {migrated_count}")
        print(f"   База данных: data/seo_data.db")
    else:
        print(f"\n⚠️  Данные для миграции не найдены")
        print(f"   База данных создана, но пуста")

if __name__ == "__main__":
    migrate_csv_to_sqlite()
