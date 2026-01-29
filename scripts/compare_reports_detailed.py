#!/usr/bin/env python3
"""
Скрипт для корректного сравнения данных из двух отчетов
"""

import sqlite3
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def compare_reports_correct():
    """Корректное сравнение данных из двух отчетов"""
    print("🔍 КОРРЕКТНОЕ СРАВНЕНИЕ ОТЧЕТОВ")
    print("=" * 70)
    
    try:
        db_path = Path("data/seo_data.db")
        if not db_path.exists():
            print("❌ База данных не найдена")
            return
        
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        
        # Получаем первый проект
        cursor = conn.execute("SELECT id, name, domain FROM projects LIMIT 1")
        project = cursor.fetchone()
        
        if not project:
            print("❌ Нет проектов в БД")
            return
        
        project_id = project['id']
        project_name = project['name']
        domain = project['domain']
        
        print(f"📁 Проект: {project_name} ({domain})")
        print()
        
        # Для каждого ключевого слова показываем полную картину
        cursor.execute("""
            SELECT 
                k.id as keyword_id,
                k.keyword,
                MAX(p.check_date) as last_check_date
            FROM keywords k
            LEFT JOIN positions p ON k.id = p.keyword_id
            WHERE k.project_id = ?
            GROUP BY k.id, k.keyword
            ORDER BY k.keyword
        """, (project_id,))
        
        keywords = cursor.fetchall()
        
        print("📊 ПОЛНАЯ КАРТИНА ПО КЛЮЧЕВЫМ СЛОВАМ:")
        print("=" * 70)
        
        all_matches = []
        
        for kw in keywords:
            keyword_id = kw['keyword_id']
            keyword = kw['keyword']
            last_date = kw['last_check_date']
            
            print(f"\n📌 Ключевое слово: '{keyword}'")
            print("-" * 50)
            
            if not last_date:
                print("   ❌ Нет данных о позициях")
                continue
            
            # Получаем наши позиции за последние проверки
            cursor.execute("""
                SELECT 
                    check_date,
                    SUBSTR(check_time, 1, 8) as check_time_short,
                    position
                FROM positions
                WHERE keyword_id = ?
                ORDER BY check_date DESC, check_time DESC
                LIMIT 3
            """, (keyword_id,))
            
            our_positions = cursor.fetchall()
            
            print("   🏠 НАШИ ПОЗИЦИИ (из таблицы positions):")
            if our_positions:
                for pos in our_positions:
                    print(f"     • {pos['check_date']} {pos['check_time_short']}: позиция {pos['position']}")
            else:
                print("     ❌ Нет данных")
            
            # Получаем конкурентов за последние проверки
            cursor.execute("""
                SELECT DISTINCT
                    c.check_date,
                    p.check_time,
                    c.competitor_position,
                    c.competitor_domain
                FROM competitors c
                JOIN positions p ON c.keyword_id = p.keyword_id 
                    AND c.check_date = p.check_date
                WHERE c.keyword_id = ?
                ORDER BY c.check_date DESC, p.check_time DESC, c.competitor_position
                LIMIT 15
            """, (keyword_id,))
            
            competitors = cursor.fetchall()
            
            print(f"\n   🏆 КОНКУРЕНТЫ (топ-5 за {last_date}):")
            
            # Группируем конкурентов по дате
            competitors_by_date = {}
            for comp in competitors:
                date_key = comp['check_date']
                if date_key not in competitors_by_date:
                    competitors_by_date[date_key] = []
                competitors_by_date[date_key].append(comp)
            
            # Показываем только последнюю дату
            if competitors_by_date:
                last_comp_date = max(competitors_by_date.keys())
                last_comps = competitors_by_date[last_comp_date]
                
                # Находим нашу позицию в этой дате
                cursor.execute("""
                    SELECT position
                    FROM positions
                    WHERE keyword_id = ? AND check_date = ?
                    ORDER BY check_time DESC
                    LIMIT 1
                """, (keyword_id, last_comp_date))
                
                our_pos_result = cursor.fetchone()
                our_position_in_comp_date = our_pos_result['position'] if our_pos_result else None
                
                if our_position_in_comp_date:
                    print(f"     📅 Дата: {last_comp_date}")
                    print(f"     🎯 Наша позиция в этой дате: {our_position_in_comp_date}")
                    print()
                
                # Показываем топ-5 конкурентов
                shown = 0
                for comp in sorted(last_comps, key=lambda x: x['competitor_position'])[:5]:
                    shown += 1
                    time_short = comp['check_time'][:8] if comp['check_time'] else "??:??:??"
                    print(f"     {comp['competitor_position']:2}. {comp['competitor_domain'][:40]:40} ({time_short})")
                
                if shown < len(last_comps):
                    print(f"     ... и еще {len(last_comps) - shown} конкурентов")
                
                # Проверяем, есть ли расхождения
                if our_positions and our_position_in_comp_date:
                    # Ищем нашу позицию в отчёте
                    latest_our_pos = our_positions[0]['position']
                    
                    if latest_our_pos == our_position_in_comp_date:
                        all_matches.append((keyword, True, latest_our_pos))
                    else:
                        all_matches.append((keyword, False, latest_our_pos, our_position_in_comp_date))
            else:
                print("     ❌ Нет данных о конкурентах")
        
        conn.close()
        
        # Сводка
        print("\n" + "=" * 70)
        print("📊 СВОДКА ПО СОВПАДЕНИЯМ:")
        print("=" * 70)
        
        matches = [m for m in all_matches if m[1]]
        mismatches = [m for m in all_matches if not m[1]]
        
        print(f"✅ Совпадает: {len(matches)} ключевых слов")
        print(f"⚠️  Расхождения: {len(mismatches)} ключевых слов")
        
        if mismatches:
            print("\n🔍 КЛЮЧЕВЫЕ СЛОВА С РАСХОЖДЕНИЯМИ:")
            for mismatch in mismatches:
                keyword = mismatch[0]
                pos_in_report = mismatch[2]
                pos_in_competitors = mismatch[3]
                print(f"   • '{keyword}': в отчёте={pos_in_report}, в данных конкурентов={pos_in_competitors}")
            
            print("\n🎯 ВОЗМОЖНЫЕ ПРИЧИНЫ РАСХОЖДЕНИЙ:")
            print("   1. Разное время проверки в таблицах positions и competitors")
            print("   2. Несколько проверок в один день с разными результатами")
            print("   3. Ошибка в JOIN между таблицами")
        
        print("\n" + "=" * 70)
        print("🔧 ДЛЯ ДАЛЬНЕЙШЕЙ ДИАГНОСТИКИ:")
        print("""
-- Проверьте JOIN между таблицами:
SELECT 
    p.check_date,
    p.check_time as pos_time,
    MAX(c.check_time) as comp_time,
    p.position as our_position,
    COUNT(c.id) as competitors_count
FROM positions p
LEFT JOIN competitors c ON p.keyword_id = c.keyword_id 
    AND p.check_date = c.check_date
WHERE p.keyword_id = [ID_КЛЮЧА]
GROUP BY p.check_date, p.check_time, p.position
ORDER BY p.check_date DESC, p.check_time DESC;

-- Проверьте все проверки за последний день:
SELECT 
    p.check_time,
    p.position,
    COUNT(c.id) as competitors_found
FROM positions p
LEFT JOIN competitors c ON p.keyword_id = c.keyword_id 
    AND p.check_date = c.check_date
    AND ABS(strftime('%s', p.check_time) - strftime('%s', c.check_time)) < 60
WHERE p.keyword_id = [ID_КЛЮЧА] 
    AND p.check_date = '2026-01-28'
GROUP BY p.check_time, p.position
ORDER BY p.check_time DESC;
""")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    compare_reports_correct()
    
    print("\n✅ Сравнение завершено.")
    print("📋 Пришлите вывод этого скрипта и результаты SQL-запросов.")