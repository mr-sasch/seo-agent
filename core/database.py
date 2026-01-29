#!/usr/bin/env python3
"""
Управление SQLite базой данных для SEO-агента
Оптимизировано для проектов 1000-5000 запросов
Добавлена поддержка сессий мониторинга
"""

import sqlite3
import json
from datetime import datetime, date, time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def _date_to_str(d: date) -> str:
    """Конвертирует date в строку YYYY-MM-DD"""
    return d.isoformat() if isinstance(d, date) else str(d)

def _time_to_str(t: time) -> str:
    """Конвертирует time в строку HH:MM:SS"""
    return t.isoformat() if isinstance(t, time) else str(t)

def _str_to_date(s: str) -> date:
    """Конвертирует строку в date"""
    return date.fromisoformat(s) if isinstance(s, str) else s

def _str_to_time(s: str) -> time:
    """Конвертирует строку в time"""
    return time.fromisoformat(s) if isinstance(s, str) else s

class SEODatabase:
    """Управление SQLite базой SEO-данных"""
    
    def __init__(self, db_path: str = "data/seo_data.db"):
        """
        Инициализация базы данных
        
        Args:
            db_path: Путь к файлу базы данных
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self._init_database()
        logger.info(f"SEODatabase инициализирован: {self.db_path}")
    
    def _init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            # ... существующий код ...
            
            # 4. Таблица конкурентов (НОВАЯ ВЕРСИЯ) - С check_time
            conn.execute("DROP TABLE IF EXISTS competitors")  # ← УДАЛИТЬ СТАРУЮ
            
            conn.execute("""
                CREATE TABLE competitors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    keyword_id INTEGER NOT NULL,
                    session_id INTEGER,
                    check_date DATE NOT NULL,
                    check_time TIME NOT NULL,  -- ← НОВОЕ ПОЛЕ
                    competitor_domain TEXT NOT NULL,
                    competitor_position INTEGER NOT NULL CHECK(competitor_position >= 1 AND competitor_position <= 100),
                    competitor_url TEXT,
                    competitor_title TEXT,
                    competitor_snippet TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
                    FOREIGN KEY (keyword_id) REFERENCES keywords (id) ON DELETE CASCADE,
                    FOREIGN KEY (session_id) REFERENCES monitoring_sessions (session_id) ON DELETE SET NULL,
                    UNIQUE(project_id, keyword_id, check_date, check_time, competitor_domain, competitor_position)
                )
            """)
            
            # Индексы для новой таблицы
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitors_project_date_time ON competitors(project_id, check_date, check_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitors_domain_date ON competitors(competitor_domain, check_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitors_keyword_pos ON competitors(keyword_id, competitor_position)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_competitors_session ON competitors(session_id)")
    
    def _add_column_if_not_exists(self, conn: sqlite3.Connection, table: str, column: str, column_type: str):
        """
        Добавляет столбец в таблицу если он не существует
        """
        try:
            # Проверяем, существует ли столбец
            cursor = conn.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]  # col[1] это имя столбца
            
            if column not in columns:
                logger.info(f"Добавляем столбец {column} в таблицу {table}")
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
            else:
                logger.debug(f"Столбец {column} уже существует в {table}")
        except Exception as e:
            logger.error(f"Ошибка при проверке/добавлении столбца {column} в {table}: {e}")
    
    def _add_column_if_not_exists(self, conn: sqlite3.Connection, table: str, column: str, column_type: str):
        """
        Добавляет столбец в таблицу если он не существует
        """
        try:
            # Проверяем, существует ли столбец
            cursor = conn.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]  # col[1] это имя столбца
            
            if column not in columns:
                logger.info(f"Добавляем столбец {column} в таблицу {table}")
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
            else:
                logger.debug(f"Столбец {column} уже существует в {table}")
        except Exception as e:
            logger.error(f"Ошибка при проверке/добавлении столбца {column} в {table}: {e}")
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ПРОЕКТАМИ ==========
    
    def get_or_create_project(self, name: str, domain: str) -> int:
        """
        Получает или создаёт проект
        
        Returns:
            ID проекта
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Пробуем найти существующий проект
            cursor = conn.execute(
                "SELECT id FROM projects WHERE domain = ?",
                (domain,)
            )
            row = cursor.fetchone()
            
            if row:
                project_id = row['id']
                # Обновляем имя если изменилось
                conn.execute(
                    "UPDATE projects SET name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (name, project_id)
                )
                logger.debug(f"Проект найден: {name} (id: {project_id})")
                return project_id
            
            # Создаём новый проект
            cursor = conn.execute(
                "INSERT INTO projects (name, domain) VALUES (?, ?) RETURNING id",
                (name, domain)
            )
            project_id = cursor.fetchone()[0]
            logger.info(f"Создан новый проект: {name} (id: {project_id})")
            return project_id
    
    def get_or_create_keyword(self, project_id: int, keyword: str) -> int:
        """
        Получает или создаёт ключевое слово
        
        Returns:
            ID ключевого слова
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute(
                "SELECT id FROM keywords WHERE project_id = ? AND keyword = ?",
                (project_id, keyword)
            )
            row = cursor.fetchone()
            
            if row:
                keyword_id = row['id']
                # Активируем если был неактивен
                conn.execute(
                    "UPDATE keywords SET is_active = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (keyword_id,)
                )
                return keyword_id
            
            cursor = conn.execute(
                "INSERT INTO keywords (project_id, keyword) VALUES (?, ?) RETURNING id",
                (project_id, keyword)
            )
            keyword_id = cursor.fetchone()[0]
            logger.debug(f"Добавлено ключевое слово: '{keyword}' (id: {keyword_id})")
            return keyword_id
    
    # ========== МЕТОДЫ ДЛЯ УПРАВЛЕНИЯ СЕССИЯМИ ==========
    
    def create_monitoring_session(self, project_id: int, session_name: str = None) -> int:
        """
        Создаёт новую сессию мониторинга
        
        Args:
            project_id: ID проекта
            session_name: Название сессии (опционально)
            
        Returns:
            ID созданной сессии
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO monitoring_sessions 
                (project_id, session_name, start_time, status)
                VALUES (?, ?, CURRENT_TIMESTAMP, 'running')
                RETURNING session_id
            """, (project_id, session_name))
            
            session_id = cursor.fetchone()[0]
            logger.info(f"Создана сессия мониторинга: {session_id} для проекта {project_id}")
            return session_id
    
    def complete_monitoring_session(self, session_id: int, 
                                  total_keywords: int = None,
                                  completed_keywords: int = None):
        """
        Завершает сессию мониторинга
        
        Args:
            session_id: ID сессии
            total_keywords: Общее количество ключевых слов
            completed_keywords: Количество успешно проверенных ключевых слов
        """
        with sqlite3.connect(self.db_path) as conn:
            update_fields = ["end_time = CURRENT_TIMESTAMP", "status = 'completed'"]
            params = []
            
            if total_keywords is not None:
                update_fields.append("total_keywords = ?")
                params.append(total_keywords)
            
            if completed_keywords is not None:
                update_fields.append("completed_keywords = ?")
                params.append(completed_keywords)
            
            params.append(session_id)
            
            query = f"""
                UPDATE monitoring_sessions 
                SET {', '.join(update_fields)}
                WHERE session_id = ?
            """
            
            conn.execute(query, params)
            logger.debug(f"Сессия {session_id} завершена")
    
    def fail_monitoring_session(self, session_id: int, error_message: str = None):
        """
        Помечает сессию как неудачную
        
        Args:
            session_id: ID сессии
            error_message: Сообщение об ошибке (опционально)
        """
        with sqlite3.connect(self.db_path) as conn:
            update_fields = ["end_time = CURRENT_TIMESTAMP", "status = 'failed'"]
            params = []
            
            if error_message:
                update_fields.append("session_name = ?")
                params.append(f"FAILED: {error_message[:100]}")
            
            params.append(session_id)
            
            query = f"""
                UPDATE monitoring_sessions 
                SET {', '.join(update_fields)}
                WHERE session_id = ?
            """
            
            conn.execute(query, params)
            logger.warning(f"Сессия {session_id} помечена как неудачная")
    
    def get_latest_session(self, project_id: int) -> Optional[Dict]:
        """
        Получает последнюю сессию мониторинга для проекта
        
        Args:
            project_id: ID проекта
            
        Returns:
            Словарь с данными сессии или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM monitoring_sessions
                WHERE project_id = ?
                ORDER BY start_time DESC
                LIMIT 1
            """, (project_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_session_positions(self, session_id: int) -> List[Dict]:
        """
        Получает все позиции, собранные в указанной сессии
        
        Args:
            session_id: ID сессии
            
        Returns:
            Список позиций
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT 
                    p.check_date,
                    p.check_time,
                    k.keyword,
                    p.position,
                    p.url,
                    p.search_engine,
                    p.total_results
                FROM positions p
                JOIN keywords k ON p.keyword_id = k.id
                WHERE p.session_id = ?
                ORDER BY p.check_time, k.keyword
            """, (session_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== МЕТОДЫ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ==========
    
    def save_position(self, project_id: int, keyword_id: int, 
                 check_date, check_time,
                 position: Optional[int], url: Optional[str],
                 total_results: int = 100,
                 search_engine: str = "yandex",
                 session_id: Optional[int] = None) -> int:  # НОВЫЙ ПАРАМЕТР
        """
        Сохраняет нашу позицию
        
        Args:
            check_date: Дата проверки (date объект или строка 'YYYY-MM-DD')
            check_time: Время проверки (time объект или строка 'HH:MM:SS')
            session_id: ID сессии мониторинга (опционально)
        
        Returns:
            ID записи о позиции
        """
        # Конвертируем в строки для SQLite
        date_str = check_date.isoformat() if hasattr(check_date, 'isoformat') else str(check_date)
        time_str = check_time.isoformat() if hasattr(check_time, 'isoformat') else str(check_time)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Проверяем, не сохраняли ли уже сегодня
            cursor = conn.execute("""
                SELECT id FROM positions 
                WHERE project_id = ? AND keyword_id = ? AND check_date = ? 
                AND search_engine = ?
            """, (project_id, keyword_id, date_str, search_engine))
            
            row = cursor.fetchone()
            if row:
                # Обновляем существующую запись (включая session_id)
                conn.execute("""
                    UPDATE positions 
                    SET position = ?, url = ?, total_results = ?, check_time = ?,
                        session_id = COALESCE(?, session_id)  -- Обновляем если передан
                    WHERE id = ?
                """, (position, url, total_results, time_str, session_id, row['id']))
                position_id = row['id']
                logger.debug(f"Обновлена позиция (id: {position_id})")
            else:
                # Создаём новую запись
                cursor = conn.execute("""
                    INSERT INTO positions 
                    (project_id, keyword_id, session_id, check_date, check_time, 
                     position, url, total_results, search_engine)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                """, (project_id, keyword_id, session_id, date_str, time_str, 
                      position, url, total_results, search_engine))
                
                position_id = cursor.fetchone()[0]
                logger.debug(f"Сохранена новая позиция (id: {position_id}): {position}")
            
            return position_id
    
    def save_competitors(self, project_id: int, keyword_id: int, 
                    check_date, check_time,  # ← ОБА ПАРАМЕТРА
                    competitors: List[Dict],
                    session_id: Optional[int] = None):
        """
        Сохраняет конкурентов с точным временем проверки
        
        Уникальность гарантирует, что одна и та же позиция конкурента
        в одно и то же время не будет сохранена дважды.
        """
        if not competitors:
            return
        
        # Конвертируем в строки для SQLite
        date_str = check_date.isoformat() if hasattr(check_date, 'isoformat') else str(check_date)
        time_str = check_time.isoformat() if hasattr(check_time, 'isoformat') else str(check_time)
        
        with sqlite3.connect(self.db_path) as conn:
            saved_count = 0
            for comp in competitors:
                if comp is None:
                    continue
                
                domain = comp.get('domain') or ''
                position = comp.get('position') or 0
                
                if not domain or position == 0:
                    continue
                
                try:
                    # INSERT OR IGNORE для избежания дублей
                    conn.execute("""
                        INSERT OR IGNORE INTO competitors 
                        (project_id, keyword_id, session_id, check_date, check_time, 
                        competitor_domain, competitor_position, competitor_url, 
                        competitor_title, competitor_snippet)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        project_id, keyword_id, session_id, date_str, time_str,
                        domain, position,
                        comp.get('url', ''),
                        (comp.get('title') or '')[:500],
                        (comp.get('snippet') or '')[:1000]
                    ))
                    
                    # Если вставка прошла (affected_rows > 0)
                    if conn.total_changes > 0:
                        self._update_domain_reference(conn, domain, check_date, position)
                        saved_count += 1
                        
                except Exception as e:
                    logger.warning(f"Ошибка сохранения конкурента {domain}: {e}")
                    continue
            
            logger.info(f"Сохранено {saved_count} конкурентов для {date_str} {time_str}, сессия: {session_id}")
    
    def _update_domain_reference(self, conn: sqlite3.Connection, 
                                domain: str, seen_date: date, position: int):
        """Обновляет справочник доменов"""
        if not domain:
            return
        
        cursor = conn.execute(
            "SELECT id, first_seen, total_appearances, avg_position FROM domains WHERE domain = ?",
            (domain,)
        )
        row = cursor.fetchone()
        
        if row:
            # Обновляем существующий домен
            domain_id, first_seen, total_appearances, avg_position = row
            new_total = total_appearances + 1
            
            # Пересчитываем среднюю позицию
            if avg_position is None:
                new_avg = position
            else:
                new_avg = (avg_position * total_appearances + position) / new_total
            
            conn.execute("""
                UPDATE domains 
                SET last_seen = ?, total_appearances = ?, avg_position = ?, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (seen_date, new_total, new_avg, domain_id))
        else:
            # Создаём новую запись
            conn.execute("""
                INSERT INTO domains (domain, first_seen, last_seen, total_appearances, avg_position)
                VALUES (?, ?, ?, 1, ?)
            """, (domain, seen_date, seen_date, position))
    
    def save_snapshot_if_changed(self, project_id, keyword_id, snapshot_date, top_10):
        """
        Сохраняет снимок выдачи только если он изменился
        """
        import json
        import hashlib
        
        date_str = snapshot_date.isoformat() if hasattr(snapshot_date, 'isoformat') else str(snapshot_date)
        top_10_json = json.dumps(top_10, ensure_ascii=False)
        current_hash = hashlib.md5(top_10_json.encode('utf-8')).hexdigest()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 1. Проверяем существующую запись
                cursor = conn.execute("""
                    SELECT id, previous_top_10_hash 
                    FROM snapshots 
                    WHERE project_id = ? AND keyword_id = ? AND snapshot_date = ?
                """, (project_id, keyword_id, date_str))
                
                existing = cursor.fetchone()
                
                if existing:
                    # 2. Если запись существует
                    previous_hash = existing[1]
                    has_changes = (current_hash != previous_hash) if previous_hash else True
                    
                    if has_changes:
                        # Обновляем существующую запись БЕЗ updated_at
                        conn.execute("""
                            UPDATE snapshots 
                            SET top_10_json = ?, 
                                previous_top_10_hash = ?,
                                has_changes = ?
                            WHERE id = ?
                        """, (top_10_json, current_hash, True, existing[0]))
                        logger.debug(f"Обновлён снимок выдачи (изменения) для {date_str}")
                        return True  # Были изменения
                    else:
                        # Без изменений - обновляем has_changes
                        conn.execute("""
                            UPDATE snapshots 
                            SET has_changes = ?
                            WHERE id = ?
                        """, (False, existing[0]))
                        logger.debug(f"Снимок выдачи без изменений для {date_str}")
                        return False  # Не было изменений
                else:
                    # 3. Если записи нет - создаём новую БЕЗ created_at (автоматически)
                    conn.execute("""
                        INSERT INTO snapshots 
                        (project_id, keyword_id, snapshot_date, top_10_json, previous_top_10_hash, has_changes)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (project_id, keyword_id, date_str, top_10_json, current_hash, True))
                    logger.debug(f"Создан новый снимок выдачи для {date_str}")
                    return True  # Новая запись = "изменение"
                        
        except sqlite3.IntegrityError as e:
            # Если вдруг уникальность нарушена (параллельный запрос)
            logger.warning(f"IntegrityError в save_snapshot_if_changed: {e}")
            # Используем INSERT OR REPLACE как запасной вариант
            return self._force_update_snapshot(project_id, keyword_id, date_str, top_10_json, current_hash)
    
    def _force_update_snapshot(self, project_id, keyword_id, date_str, top_10_json, current_hash):
        """Принудительное обновление при ошибке IntegrityError"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO snapshots 
                    (project_id, keyword_id, snapshot_date, top_10_json, previous_top_10_hash, has_changes)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (project_id, keyword_id, date_str, top_10_json, current_hash, True))
                logger.debug(f"Принудительно сохранён снимок (INSERT OR REPLACE) для {date_str}")
                return True
        except Exception as e:
            logger.error(f"Ошибка в _force_update_snapshot: {e}")
            return False
    
    # ========== МЕТОДЫ ДЛЯ ЧТЕНИЯ ДАННЫХ ==========
    
    def get_position_history(self, domain: str, days: int = 7) -> List[Dict]:
        """
        Получает историю позиций за указанный период
        
        Внимание: Возвращает данные как раньше, но теперь может содержать session_id
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = """
                SELECT 
                    p.check_date,
                    p.check_time,
                    k.keyword,
                    p.position,
                    p.url,
                    p.search_engine,
                    p.total_results,
                    p.session_id  -- НОВОЕ ПОЛЕ
                FROM positions p
                JOIN keywords k ON p.keyword_id = k.id
                JOIN projects pr ON p.project_id = pr.id
                WHERE pr.domain = ?
                AND p.check_date >= date('now', ? || ' days')
                ORDER BY p.check_date DESC, p.check_time DESC, k.keyword
            """
            
            cursor = conn.execute(query, (domain, f"-{days}"))
            results = [dict(row) for row in cursor.fetchall()]
            
            logger.debug(f"Загружено {len(results)} записей истории для {domain}")
            return results
    
    def get_competitors_for_date(self, domain: str, check_date: date, 
                                keyword: Optional[str] = None) -> List[Dict]:
        """
        Получает конкурентов на указанную дату
        
        Returns:
            Список конкурентов
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = """
                SELECT 
                    c.check_date,
                    k.keyword,
                    c.competitor_domain,
                    c.competitor_position,
                    c.competitor_url,
                    c.competitor_title,
                    c.session_id  -- НОВОЕ ПОЛЕ
                FROM competitors c
                JOIN keywords k ON c.keyword_id = k.id
                JOIN projects pr ON c.project_id = pr.id
                WHERE pr.domain = ? AND c.check_date = ?
            """
            params = [domain, check_date]
            
            if keyword:
                query += " AND k.keyword = ?"
                params.append(keyword)
            
            query += " ORDER BY k.keyword, c.competitor_position"
            
            cursor = conn.execute(query, params)
            results = [dict(row) for row in cursor.fetchall()]
            
            logger.debug(f"Загружено {len(results)} конкурентов для {domain} на {check_date}")
            return results
    
    def get_top_competitors(self, domain: str, limit: int = 10) -> List[Dict]:
        """
        Получает топ конкурентов по количеству появлений
        
        Returns:
            Список конкурентов с метриками
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = """
                SELECT 
                    c.competitor_domain as domain,
                    COUNT(*) as appearances,
                    MIN(c.competitor_position) as best_position,
                    AVG(c.competitor_position) as avg_position,
                    d.category,
                    d.first_seen,
                    d.last_seen
                FROM competitors c
                JOIN projects pr ON c.project_id = pr.id
                LEFT JOIN domains d ON c.competitor_domain = d.domain
                WHERE pr.domain = ?
                GROUP BY c.competitor_domain
                ORDER BY appearances DESC, avg_position ASC
                LIMIT ?
            """
            
            cursor = conn.execute(query, (domain, limit))
            results = [dict(row) for row in cursor.fetchall()]
            
            logger.debug(f"Загружено {len(results)} топ конкурентов для {domain}")
            return results
    
    # ========== УТИЛИТНЫЕ МЕТОДЫ ==========
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику базы данных
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            stats = {}
            
            # Количество записей в каждой таблице
            tables = ['monitoring_sessions', 'projects', 'keywords', 'positions', 'competitors', 'domains', 'snapshots']
            for table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) as count FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()['count']
            
            # Даты покрытия данных
            cursor = conn.execute("SELECT MIN(check_date) as first_date, MAX(check_date) as last_date FROM positions")
            dates = cursor.fetchone()
            stats['data_range'] = {
                'first_date': dates['first_date'],
                'last_date': dates['last_date']
            }
            
            # Статистика по сессиям
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_sessions,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_sessions,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_sessions,
                    AVG(total_keywords) as avg_keywords_per_session
                FROM monitoring_sessions
            """)
            session_stats = cursor.fetchone()
            stats['session_stats'] = {
                'total_sessions': session_stats['total_sessions'],
                'completed_sessions': session_stats['completed_sessions'],
                'failed_sessions': session_stats['failed_sessions'],
                'avg_keywords_per_session': session_stats['avg_keywords_per_session']
            }
            
            # Размер базы данных
            import os
            stats['database_size_mb'] = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            
            return stats
    
    def export_to_csv(self, export_dir: str = "data/exports"):
        """
        Экспортирует данные в CSV файлы
        """
        export_path = Path(export_dir)
        export_path.mkdir(exist_ok=True, parents=True)
        
        import pandas as pd
        
        with sqlite3.connect(self.db_path) as conn:
            # Экспорт позиций
            df_positions = pd.read_sql_query("""
                SELECT p.check_date, p.check_time, k.keyword, p.position, p.url, 
                       p.search_engine, p.session_id
                FROM positions p
                JOIN keywords k ON p.keyword_id = k.id
                ORDER BY p.check_date DESC
            """, conn)
            df_positions.to_csv(export_path / "positions_export.csv", index=False, encoding='utf-8')
            
            # Экспорт конкурентов
            df_competitors = pd.read_sql_query("""
                SELECT c.check_date, k.keyword, c.competitor_domain, 
                       c.competitor_position, c.competitor_url, c.session_id
                FROM competitors c
                JOIN keywords k ON c.keyword_id = k.id
                ORDER BY c.check_date DESC, k.keyword, c.competitor_position
            """, conn)
            df_competitors.to_csv(export_path / "competitors_export.csv", index=False, encoding='utf-8')
            
            # Экспорт сессий
            df_sessions = pd.read_sql_query("""
                SELECT s.session_id, p.name as project_name, s.session_name, 
                       s.start_time, s.end_time, s.status, s.total_keywords, s.completed_keywords
                FROM monitoring_sessions s
                JOIN projects p ON s.project_id = p.id
                ORDER BY s.start_time DESC
            """, conn)
            df_sessions.to_csv(export_path / "sessions_export.csv", index=False, encoding='utf-8')
            
            logger.info(f"Данные экспортированы в {export_path}")
    
    def backup_database(self, backup_dir: str = "data/backups"):
        """
        Создаёт резервную копию базы данных
        """
        backup_path = Path(backup_dir)
        backup_path.mkdir(exist_ok=True, parents=True)
        
        import shutil
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_path / f"seo_data_backup_{timestamp}.db"
        
        shutil.copy2(self.db_path, backup_file)
        logger.info(f"Создана резервная копия: {backup_file}")
        
        # Удаляем старые бэкапы (оставляем последние 5)
        backup_files = sorted(backup_path.glob("seo_data_backup_*.db"))
        for old_file in backup_files[:-5]:
            old_file.unlink()
            logger.debug(f"Удалён старый бэкап: {old_file}")

# ========== ТЕСТОВЫЙ КОД ==========

def test_database():
    """
    Тестирует работу базы данных (обновлённый тест)
    """
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 ТЕСТИРУЕМ БАЗУ ДАННЫХ (СЕССИОННАЯ ВЕРСИЯ)")
    print("=" * 50)
    
    try:
        # Создаём/подключаем базу
        db = SEODatabase("test_seo_data.db")
        
        # Тест 1: Создание проекта
        print("\n1. Тестируем создание проекта...")
        project_id = db.get_or_create_project("Тестовый проект", "test-domain.ru")
        print(f"   ✅ Проект создан, id: {project_id}")
        
        # Тест 2: Создание сессии
        print("\n2. Тестируем создание сессии...")
        session_id = db.create_monitoring_session(project_id, "Тестовая сессия")
        print(f"   ✅ Сессия создана, id: {session_id}")
        
        # Тест 3: Добавление ключевых слов
        print("\n3. Тестируем добавление ключевых слов...")
        keywords = ["тестовый запрос 1", "тестовый запрос 2"]
        keyword_ids = []
        
        for keyword in keywords:
            keyword_id = db.get_or_create_keyword(project_id, keyword)
            keyword_ids.append(keyword_id)
            print(f"   ✅ Ключевое слово: '{keyword}' (id: {keyword_id})")
        
        # Тест 4: Сохранение позиций с сессией
        print("\n4. Тестируем сохранение позиций с сессией...")
        from datetime import date, time
        
        test_date = date.today()
        test_time = time(10, 30, 0)
        
        for i, (keyword, keyword_id) in enumerate(zip(keywords, keyword_ids), 1):
            position_id = db.save_position(
                project_id=project_id,
                keyword_id=keyword_id,
                check_date=test_date,
                check_time=test_time,
                position=i * 5,
                url=f"https://test-domain.ru/page{i}",
                total_results=100,
                session_id=session_id  # ← ПЕРЕДАЁМ session_id
            )
            print(f"   ✅ Позиция сохранена с сессией (id: {position_id}): {i * 5}")
        
        # Тест 5: Сохранение конкурентов с сессией
        print("\n5. Тестируем сохранение конкурентов с сессией...")
        competitors = [
            {
                'domain': 'competitor1.ru',
                'position': 1,
                'url': 'https://competitor1.ru/page1',
                'title': 'Тестовый заголовок 1',
                'snippet': 'Тестовое описание 1'
            },
            {
                'domain': 'competitor2.ru',
                'position': 2,
                'url': 'https://competitor2.ru/page2',
                'title': 'Тестовый заголовок 2',
                'snippet': 'Тестовое описание 2'
            }
        ]
        
        db.save_competitors(
            project_id=project_id,
            keyword_id=keyword_ids[0],
            check_date=test_date,
            competitors=competitors,
            session_id=session_id  # ← ПЕРЕДАЁМ session_id
        )
        print(f"   ✅ Сохранено {len(competitors)} конкурентов с сессией")
        
        # Тест 6: Получение позиций сессии
        print("\n6. Тестируем получение позиций сессии...")
        session_positions = db.get_session_positions(session_id)
        print(f"   ✅ Загружено позиций из сессии: {len(session_positions)}")
        
        # Тест 7: Завершение сессии
        print("\n7. Тестируем завершение сессии...")
        db.complete_monitoring_session(session_id, total_keywords=2, completed_keywords=2)
        print(f"   ✅ Сессия {session_id} завершена")
        
        # Тест 8: Получение последней сессии
        print("\n8. Тестируем получение последней сессии...")
        latest_session = db.get_latest_session(project_id)
        if latest_session:
            print(f"   ✅ Последняя сессия: {latest_session['session_id']}, статус: {latest_session['status']}")
        
        # Тест 9: Статистика базы
        print("\n9. Тестируем получение статистики...")
        stats = db.get_database_stats()
        print(f"   ✅ Проектов: {stats.get('projects_count')}")
        print(f"   ✅ Сессий: {stats.get('monitoring_sessions_count')}")
        print(f"   ✅ Ключевых слов: {stats.get('keywords_count')}")
        print(f"   ✅ Позиций: {stats.get('positions_count')}")
        print(f"   ✅ Конкурентов: {stats.get('competitors_count')}")
        
        # Тест 10: Экспорт
        print("\n10. Тестируем экспорт в CSV...")
        db.export_to_csv("test_exports")
        print("   ✅ Экспорт завершён")
        
        # Тест 11: Резервное копирование
        print("\n11. Тестируем резервное копирование...")
        db.backup_database("test_backups")
        print("   ✅ Резервная копия создана")
        
        print("\n" + "=" * 50)
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО")
        
        # Удаляем тестовую базу и файлы
        import os
        os.remove("test_seo_data.db")
        if os.path.exists("test_exports/positions_export.csv"):
            os.remove("test_exports/positions_export.csv")
        if os.path.exists("test_exports/competitors_export.csv"):
            os.remove("test_exports/competitors_export.csv")
        if os.path.exists("test_exports/sessions_export.csv"):
            os.remove("test_exports/sessions_export.csv")
        if os.path.exists("test_exports"):
            os.rmdir("test_exports")
        
        # Удаляем последний бэкап
        backup_files = list(Path("test_backups").glob("*.db"))
        if backup_files:
            backup_files[0].unlink()
        if os.path.exists("test_backups"):
            os.rmdir("test_backups")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ТЕСТИРОВАНИИ: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_database()