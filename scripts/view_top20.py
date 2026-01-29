# view_top20.py
import sqlite3
from pathlib import Path

db_path = Path("data/seo_data.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

cursor = conn.cursor()

# Получаем список ключевых слов
cursor.execute("SELECT id, keyword FROM keywords")
keywords = cursor.fetchall()

for keyword in keywords:
    print(f"\n🔍 Ключ: {keyword['keyword']}")
    print("-" * 80)
    
    # Получаем последнюю дату проверки для этого ключа
    cursor.execute("""
        SELECT MAX(check_date) as last_date
        FROM competitors
        WHERE keyword_id = ?
    """, (keyword['id'],))
    last_date = cursor.fetchone()['last_date']
    
    if not last_date:
        print("Нет данных")
        continue
    
    # Получаем топ-20 за последнюю проверку
    cursor.execute("""
        SELECT 
            competitor_position as поз,
            competitor_domain as домен,
            CASE 
                WHEN LENGTH(competitor_title) > 50 
                THEN SUBSTR(competitor_title, 1, 47) || '...' 
                ELSE competitor_title 
            END as заголовок
        FROM competitors
        WHERE keyword_id = ? AND check_date = ?
        ORDER BY competitor_position
        LIMIT 20
    """, (keyword['id'], last_date))
    
    results = cursor.fetchall()
    
    for row in results:
        print(f"{row['поз']:2}. {row['домен']:30} | {row['заголовок']}")
    
    print(f"Всего результатов: {len(results)}")

conn.close()
