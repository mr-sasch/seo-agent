def compare_reports():
    """Сравнивает данные из двух отчетов"""
    import sqlite3
    
    conn = sqlite3.connect("data/seo_data.db")
    conn.row_factory = sqlite3.Row
    
    # Получаем проект
    cursor = conn.execute("SELECT id, domain FROM projects LIMIT 1")
    project = cursor.fetchone()
    
    if not project:
        print("Нет проектов в БД")
        return
    
    project_id = project['id']
    domain = project['domain']
    
    # Получаем ключевые слова
    cursor = conn.execute(
        "SELECT id, keyword FROM keywords WHERE project_id = ?",
        (project_id,)
    )
    keywords = cursor.fetchall()
    
    print(f"\n🔍 СРАВНЕНИЕ ДАННЫХ ДЛЯ: {domain}")
    print("=" * 60)
    
    for kw in keywords:
        keyword_id = kw['id']
        keyword = kw['keyword']
        
        print(f"\n📌 Ключевое слово: {keyword}")
        print("-" * 40)
        
        # Данные из html_reporter.py (все позиции)
        cursor.execute("""
            SELECT check_date, check_time, position, url
            FROM positions 
            WHERE keyword_id = ?
            ORDER BY check_date DESC, check_time DESC
        """, (keyword_id,))
        
        positions = cursor.fetchall()
        
        if positions:
            for pos in positions[:3]:  # Последние 3 проверки
                print(f"  📅 {pos['check_date']} {pos['check_time']}: позиция {pos['position']}")
        else:
            print("  ❌ Нет данных о позициях")
    
    conn.close()

if __name__ == "__main__":
    compare_reports()
