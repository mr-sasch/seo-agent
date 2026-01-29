#!/usr/bin/env python3
"""
Диагностика и исправление проблемы с competitors
"""

import sqlite3
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def diagnose_competitors_issue():
    """Диагностирует проблему с таблицей competitors"""
    print("🔍 ДИАГНОСТИКА ПРОБЛЕМЫ С КОНКУРЕНТАМИ")
    print("=" * 70)
    
    db_path = Path("data/seo_data.db")
    if not db_path.exists():
        print("❌ База данных не найдена")
        return
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    try:
        # 1. Проверим общую статистику
        cursor = conn.execute("""
            SELECT 
                'positions' as table_name,
                COUNT(*) as total_records,
                MIN(check_date) as first_date,
                MAX(check_date) as last_date
            FROM positions
            UNION ALL
            SELECT 
                'competitors',
                COUNT(*),
                MIN(check_date),
                MAX(check_date)
            FROM competitors
        """)
        
        stats = cursor.fetchall()
        
        print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
        for stat in stats:
            table = stat['table_name']
            count = stat['total_records']
            first = stat['first_date'] or "нет данных"
            last = stat['last_date'] or "нет данных"
            print(f"   • {table:12}: {count:4} записей | с {first} по {last}")
        
        # 2. Проверим последние данные
        print("\n📅 ПОСЛЕДНИЕ ДАННЫЕ ПО КЛЮЧЕВЫМ СЛОВАМ:")
        
        cursor.execute("""
            SELECT 
                k.keyword,
                MAX(p.check_date) as last_position_date,
                COUNT(DISTINCT p.check_date) as position_checks,
                MAX(c.check_date) as last_competitor_date,
                COUNT(DISTINCT c.check_date) as competitor_checks
            FROM keywords k
            LEFT JOIN positions p ON k.id = p.keyword_id
            LEFT JOIN competitors c ON k.id = c.keyword_id
            GROUP BY k.id, k.keyword
            ORDER BY k.keyword
        """)
        
        keywords_stats = cursor.fetchall()
        
        for kw in keywords_stats:
            keyword = kw['keyword']
            pos_date = kw['last_position_date'] or "нет данных"
            pos_count = kw['position_checks'] or 0
            comp_date = kw['last_competitor_date'] or "нет данных"
            comp_count = kw['competitor_checks'] or 0
            
            status = "✅" if comp_count > 0 else "❌"
            print(f"   {status} '{keyword[:30]:30}': позиций={pos_count:2} ({pos_date}), конкурентов={comp_count:2} ({comp_date})")
        
        # 3. Проверим, есть ли хоть какие-то конкуренты
        cursor.execute("SELECT COUNT(DISTINCT competitor_domain) as unique_domains FROM competitors")
        unique_domains = cursor.fetchone()['unique_domains']
        
        print(f"\n🌐 Уникальных доменов конкурентов: {unique_domains}")
        
        if unique_domains > 0:
            cursor.execute("""
                SELECT 
                    competitor_domain,
                    COUNT(*) as count,
                    MIN(competitor_position) as best_position,
                    MAX(competitor_position) as worst_position
                FROM competitors
                GROUP BY competitor_domain
                ORDER BY count DESC
                LIMIT 10
            """)
            
            print("🏆 ТОП-10 ДОМЕНОВ КОНКУРЕНТОВ:")
            domains = cursor.fetchall()
            for i, domain in enumerate(domains, 1):
                print(f"   {i:2}. {domain['competitor_domain'][:40]:40} | записей: {domain['count']:3} | позиции: {domain['best_position']}-{domain['worst_position']}")
        
        # 4. Проверим конфигурацию сбора данных
        print("\n⚙️  КОНФИГУРАЦИЯ:")
        
        # Проверим projects.yaml
        config_path = Path("config/projects.yaml")
        if config_path.exists():
            print(f"   ✅ Файл конфигурации найден: {config_path}")
            try:
                import yaml
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                if config and 'projects' in config:
                    for project in config['projects']:
                        print(f"   • Проект: {project.get('name')} ({project.get('domain')})")
            except:
                print("   ⚠️  Не удалось прочитать конфигурацию")
        else:
            print(f"   ❌ Файл конфигурации не найден: {config_path}")
        
        # 5. Рекомендации
        print("\n🎯 РЕКОМЕНДАЦИИ:")
        
        if stats[1]['total_records'] == 0:  # competitors пуста
            print("   1. 🚨 Таблица competitors ПУСТА!")
            print("   2. Проверьте параметр 'track_competitors' в seo_agent.py")
            print("   3. Запустите сбор данных с включенным отслеживанием конкурентов:")
            print("      python seo_agent.py")
            print("   4. Или используйте data_collector напрямую:")
            print("""
      from core.data_collector import DataCollector
      collector = DataCollector()
      collector.check_positions(
          domain="aquamoney.by",
          keywords=["водомат", "вендинговые аппараты купить"],
          track_competitors=True,  # ← ВАЖНО!
          competitors_limit=20
      )
            """)
        else:
            print("   1. Данные есть, но возможно устарели")
            print("   2. Запустите обновление данных:")
            print("      python seo_agent.py")
        
    finally:
        conn.close()

def fix_competitors_query():
    """Создает временный SQL-запрос для проверки данных"""
    print("\n🔧 ТЕСТОВЫЙ SQL-ЗАПРОС ДЛЯ ПРОВЕРКИ:")
    
    sql = """
-- 1. Простой запрос для проверки наличия данных
SELECT 
    c.keyword_id,
    k.keyword,
    c.check_date,
    c.competitor_position,
    c.competitor_domain,
    c.competitor_url
FROM competitors c
JOIN keywords k ON c.keyword_id = k.id
WHERE k.keyword LIKE '%водомат%'
   OR k.keyword LIKE '%источник%'
ORDER BY c.check_date DESC
LIMIT 10;

-- 2. Проверка всех записей за последний месяц
SELECT 
    check_date,
    COUNT(*) as total_competitors,
    COUNT(DISTINCT keyword_id) as keywords_with_competitors
FROM competitors
WHERE check_date >= DATE('now', '-30 days')
GROUP BY check_date
ORDER BY check_date DESC;
"""
    
    print(sql)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    diagnose_competitors_issue()
    fix_competitors_query()
    
    print("\n" + "=" * 70)
    print("✅ Диагностика завершена.")
    print("📋 Выполните SQL-запросы из раздела 'ТЕСТОВЫЙ SQL-ЗАПРОС'")
    print("   и пришлите результаты для точного решения проблемы.")
