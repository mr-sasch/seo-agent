import sqlite3
from pathlib import Path

db_path = Path("data/seo_data.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row  # Чтобы возвращались словари

cursor = conn.cursor()

print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
print("=" * 50)

# Проекты
cursor.execute("SELECT COUNT(*) as count FROM projects")
projects = cursor.fetchone()['count']
print(f"Проектов: {projects}")

# Ключевые слова
cursor.execute("SELECT COUNT(*) as count FROM keywords")
keywords = cursor.fetchone()['count']
print(f"Ключевых слов: {keywords}")

# Наши позиции
cursor.execute("SELECT COUNT(*) as count FROM positions")
positions = cursor.fetchone()['count']
print(f"Позиций (наших): {positions}")

# Конкуренты
cursor.execute("SELECT COUNT(*) as count FROM competitors")
competitors = cursor.fetchone()['count']
print(f"Конкурентов: {competitors}")

print("\n📈 ПОСЛЕДНИЕ ДАННЫЕ:")
print("=" * 50)

# Последние 3 наших позиции
print("\nПоследние наши позиции:")
cursor.execute("""
    SELECT k.keyword, p.position, p.check_date, p.check_time 
    FROM positions p
    JOIN keywords k ON p.keyword_id = k.id
    ORDER BY p.check_date DESC, p.check_time DESC 
    LIMIT 3
""")
for row in cursor.fetchall():
    print(f"  {row['keyword']}: позиция {row['position']} ({row['check_date']} {row['check_time']})")

# Топ-5 конкурентов по частоте
print("\nЧастые конкуренты:")
cursor.execute("""
    SELECT competitor_domain, COUNT(*) as count
    FROM competitors 
    GROUP BY competitor_domain 
    ORDER BY count DESC 
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"  {row['competitor_domain']}: {row['count']} раз")

conn.close()
