#!/usr/bin/env python3
"""
Модуль для генерации HTML отчетов по топ-10 конкурентам
Добавлена поддержка сессий мониторинга
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class CompetitorHTMLReporter:
    """
    Генератор HTML отчетов для топ-10 конкурентов
    """
    
    def __init__(self, db_path: str = "data/seo_data.db"):
        """
        Инициализация генератора отчетов
        
        Args:
            db_path: Путь к базе данных SQLite
        """
        self.db_path = Path(db_path)
        self.reports_dir = Path("data/reports/competitors_html")
        self.reports_dir.mkdir(exist_ok=True, parents=True)
        
        logger.info(f"CompetitorHTMLReporter инициализирован. База: {db_path}")
    
    def generate_top10_report(self, project_name: str, domain: str, session_id: Optional[int] = None) -> str:
        """
        Генерирует HTML отчет с топ-10 конкурентами
        
        Args:
            project_name: Название проекта
            domain: Домен сайта
            session_id: ID сессии мониторинга (опционально)
                - Если None: показывает все сессии (как сейчас)
                - Если указан: показывает только указанную сессию
        
        Returns:
            Путь к сохраненному HTML файлу
        """
        if session_id:
            logger.info(f"Генерация HTML отчета по конкурентам для {project_name} ({domain}), сессия: {session_id}")
        else:
            logger.info(f"Генерация HTML отчета по конкурентам для {project_name} ({domain}), все сессии")
        
        # Получаем данные из БД
        data = self._get_top10_data(domain, session_id)
        
        if not data:
            logger.warning(f"Нет данных для отчета {project_name}")
            return ""
        
        # Генерируем HTML
        html_content = self._create_html_report(project_name, domain, data, session_id)
        
        # Сохраняем файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if session_id:
            filename = f"competitors_{project_name.lower().replace(' ', '_')}_session{session_id}_{timestamp}.html"
        else:
            filename = f"competitors_{project_name.lower().replace(' ', '_')}_{timestamp}.html"
            
        filepath = self.reports_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML отчет по конкурентам сохранён: {filepath}")
        
        # Также создаем симлинк на latest
        if session_id:
            latest_file = self.reports_dir / f"latest_competitors_{project_name.lower().replace(' ', '_')}_session{session_id}.html"
        else:
            latest_file = self.reports_dir / f"latest_competitors_{project_name.lower().replace(' ', '_')}.html"
            
        if latest_file.exists():
            latest_file.unlink()
        latest_file.symlink_to(filepath.name)
        
        return str(filepath)
    
    def _get_top10_data(self, domain: str, session_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Получает данные о топ-10 конкурентах из базы данных
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # 1. Получаем проект
                cursor = conn.execute(
                    "SELECT id FROM projects WHERE domain = ?",
                    (domain,)
                )
                project_row = cursor.fetchone()
                
                if not project_row:
                    logger.warning(f"Проект с доменом {domain} не найден")
                    return {}
                
                project_id = project_row['id']
                
                # 2. Получаем ключевые слова проекта
                cursor = conn.execute(
                    "SELECT id, keyword FROM keywords WHERE project_id = ? AND is_active = TRUE ORDER BY keyword",
                    (project_id,)
                )
                keywords = {row['id']: row['keyword'] for row in cursor.fetchall()}
                
                if not keywords:
                    logger.warning(f"Нет активных ключевых слов для проекта {domain}")
                    return {}
                
                # 3. Получаем данные о сессиях
                keyword_ids_str = ','.join(str(kid) for kid in keywords.keys())
                
                if session_id:
                    # Фильтруем по конкретной сессии
                    query = f"""
                        SELECT DISTINCT
                            p.session_id,
                            s.session_name,
                            s.start_time as session_start_time,
                            DATE(s.start_time) as session_date,
                            TIME(s.start_time) as session_time
                        FROM positions p
                        JOIN monitoring_sessions s ON p.session_id = s.session_id
                        WHERE p.keyword_id IN ({keyword_ids_str})
                        AND p.session_id = ?
                        ORDER BY s.start_time DESC
                    """
                    params = (session_id,)
                else:
                    # Берем все сессии
                    query = f"""
                        SELECT DISTINCT
                            p.session_id,
                            s.session_name,
                            s.start_time as session_start_time,
                            DATE(s.start_time) as session_date,
                            TIME(s.start_time) as session_time
                        FROM positions p
                        JOIN monitoring_sessions s ON p.session_id = s.session_id
                        WHERE p.keyword_id IN ({keyword_ids_str})
                        ORDER BY s.start_time DESC
                    """
                    params = ()
                
                cursor.execute(query, params)
                session_rows = cursor.fetchall()
                
                if not session_rows:
                    logger.warning(f"Нет данных о сессиях для проекта {domain}" + 
                                (f" с session_id={session_id}" if session_id else ""))
                    return {}
                
                # 4. Подготавливаем структуру для данных
                data = {
                    'keywords': keywords,
                    'top10': {},  # {keyword: {session_key: [{position, domain, url, title}]}}
                    'sessions': [],  # Список сессий с информацией
                    'stats': {},
                    'has_session_id': session_id is not None
                }
                
                # 5. Формируем список сессий (аналогично html_reporter.py)
                for row in session_rows:
                    session_id_from_db = row['session_id']
                    session_name = row['session_name']
                    session_start_time = row['session_start_time']
                    session_date = row['session_date']
                    session_time = row['session_time']
                    
                    # Обработка времени сессии (убираем миллисекунды)
                    if session_time and '.' in session_time:
                        session_time = session_time.split('.')[0]
                    
                    # Формируем ключ сессии (как в html_reporter.py)
                    if session_id_from_db:
                        session_key = f"Сессия {session_id_from_db}"
                    else:
                        session_key = f"{session_date} {session_time}"
                    
                    data['sessions'].append({
                        'session_id': session_id_from_db,
                        'session_name': session_name,
                        'session_start_time': session_start_time,
                        'date': session_date,
                        'time': session_time,
                        'session_key': session_key
                    })
                
                # 6. Получаем топ-10 конкурентов для каждой сессии
                for session in data['sessions']:
                    session_key = session['session_key']
                    session_id_from_key = session['session_id']
                    
                    # Получаем конкурентов для этой сессии
                    query_competitors = f"""
                        SELECT 
                            c.keyword_id,
                            c.competitor_position as position,
                            c.competitor_domain as domain,
                            c.competitor_url as url,
                            c.competitor_title as title
                        FROM competitors c
                        WHERE c.keyword_id IN ({keyword_ids_str})
                        AND c.session_id = ?
                        AND c.competitor_position <= 10
                        ORDER BY c.keyword_id, c.competitor_position
                    """
                    
                    cursor.execute(query_competitors, (session_id_from_key,))
                    competitor_rows = cursor.fetchall()
                    
                    # Получаем наши позиции для этой сессии
                    query_our_position = f"""
                        SELECT 
                            p.keyword_id,
                            p.position,
                            p.url
                        FROM positions p
                        WHERE p.keyword_id IN ({keyword_ids_str})
                        AND p.session_id = ?
                        AND p.position <= 10
                        ORDER BY p.keyword_id, p.position
                    """
                    cursor.execute(query_our_position, (session_id_from_key,))
                    our_position_rows = cursor.fetchall()
                    
                    # Обрабатываем конкурентов
                    for row in competitor_rows:
                        keyword_id = row['keyword_id']
                        keyword = keywords[keyword_id]
                        position = row['position']
                        
                        # Инициализируем структуры
                        if keyword not in data['top10']:
                            data['top10'][keyword] = {}
                        
                        if session_key not in data['top10'][keyword]:
                            data['top10'][keyword][session_key] = []
                        
                        # Добавляем конкурента
                        data['top10'][keyword][session_key].append({
                            'position': position,
                            'domain': row['domain'],
                            'url': row['url'],
                            'title': row['title'] or "",
                            'display_url': self._shorten_url(row['url']),
                            'is_our_site': False
                        })
                    
                    # Обрабатываем наши позиции
                    for row in our_position_rows:
                        keyword_id = row['keyword_id']
                        keyword = keywords[keyword_id]
                        position = row['position']
                        
                        # Инициализируем структуры
                        if keyword not in data['top10']:
                            data['top10'][keyword] = {}
                        
                        if session_key not in data['top10'][keyword]:
                            data['top10'][keyword][session_key] = []
                        
                        # Проверяем, не добавили ли мы уже эту позицию как конкурента
                        existing_positions = [c['position'] for c in data['top10'][keyword][session_key]]
                        
                        if position not in existing_positions:
                            # Добавляем наш сайт
                            data['top10'][keyword][session_key].append({
                                'position': position,
                                'domain': domain,  # Наш домен
                                'url': row['url'],
                                'title': "Наш сайт",
                                'display_url': self._shorten_url(row['url']),
                                'is_our_site': True
                            })
                
                # 7. Сортируем и заполняем пропуски
                for keyword in data['top10']:
                    for session_key in data['top10'][keyword]:
                        competitors_list = data['top10'][keyword][session_key]
                        
                        # Сортируем по позиции
                        competitors_list.sort(key=lambda x: x['position'])
                        
                        # Заполняем пропущенные позиции (1-10)
                        existing_positions = {c['position'] for c in competitors_list}
                        
                        for pos in range(1, 11):
                            if pos not in existing_positions:
                                # Добавляем заглушку для пропущенной позиции
                                competitors_list.append({
                                    'position': pos,
                                    'domain': '???',
                                    'url': '',
                                    'title': 'Позиция отсутствует в выдаче',
                                    'display_url': '(нет данных)',
                                    'is_our_site': False,
                                    'is_missing': True
                                })
                        
                        # Снова сортируем и оставляем только топ-10
                        competitors_list.sort(key=lambda x: x['position'])
                        data['top10'][keyword][session_key] = competitors_list[:10]
                
                # 8. Рассчитываем статистику (оставляем как было)
                for keyword in data['top10']:
                    sessions_list = list(data['top10'][keyword].keys())
                    if sessions_list:
                        # Берем последнюю сессию
                        latest_session = sessions_list[0]  # Уже отсортированы по времени
                        latest_top10 = data['top10'][keyword].get(latest_session, [])
                        
                        # Считаем уникальные домены в топ-10
                        unique_domains = set()
                        our_positions = []
                        
                        for comp in latest_top10:
                            unique_domains.add(comp['domain'])
                            if comp.get('is_our_site'):
                                our_positions.append(comp['position'])
                        
                        data['stats'][keyword] = {
                            'latest_session': latest_session,
                            'total_competitors': len(latest_top10),
                            'unique_domains': len(unique_domains),
                            'top_position': latest_top10[0]['position'] if latest_top10 else None,
                            'our_domain_in_top10': len(our_positions) > 0,
                            'our_positions': our_positions
                        }
                    else:
                        data['stats'][keyword] = {
                            'latest_session': None,
                            'total_competitors': 0,
                            'unique_domains': 0,
                            'top_position': None,
                            'our_domain_in_top10': False,
                            'our_positions': []
                        }
                
                return data
                
        except Exception as e:
            logger.error(f"Ошибка получения данных из БД: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
    
    def _shorten_url(self, url: str) -> str:
        """
        Сокращает URL для отображения
        
        Args:
            url: Полный URL
            
        Returns:
            Сокращенный URL
        """
        if not url:
            return ""
        
        try:
            parsed = urlparse(url)
            path = parsed.path
            
            # Если путь длинный, сокращаем его
            if len(path) > 30:
                path = path[:27] + "..."
            
            # Комбинируем домен и сокращенный путь
            return parsed.netloc + path
            
        except:
            # Если не удалось распарсить, возвращаем как есть (но обрезаем)
            return url[:40] + ("..." if len(url) > 40 else "")
    
    def _create_html_report(self, project_name: str, domain: str, data: Dict, session_id: Optional[int] = None) -> str:
        """
        Создает HTML отчет
        
        Args:
            project_name: Название проекта
            domain: Домен сайта
            data: Данные для отчета
            session_id: ID сессии (для заголовка)
            
        Returns:
            HTML строка
        """
        keywords = data['keywords']
        top10 = data['top10']
        sessions = data['sessions']  # Теперь это список сессий с информацией
        stats = data['stats']
        has_session_id = data.get('has_session_id', False)
        
        # Сортируем ключевые слова
        keyword_names = list(keywords.values())
        sorted_keywords = sorted(keyword_names)
        
        # Подсчитываем общую статистику
        total_keywords = len(sorted_keywords)
        total_checks = len(sessions)
        keywords_with_data = sum(1 for kw in sorted_keywords if kw in top10)
        
        # Считаем сколько ключевых слов имеют наш домен в топ-10
        our_domain_count = 0
        our_positions_summary = []
        for keyword in sorted_keywords:
            if stats.get(keyword, {}).get('our_domain_in_top10'):
                our_domain_count += 1
                our_positions = stats.get(keyword, {}).get('our_positions', [])
                if our_positions:
                    our_positions_summary.append(f"{keyword}: {', '.join(map(str, our_positions))}")
        
        # Заголовок с информацией о сессии
        session_title = ""
        if session_id:
            # Находим название сессии
            session_name = ""
            for session in sessions:
                if session.get('session_id') == session_id:
                    session_name = session.get('session_name', f"Сессия {session_id}")
                    break
            
            session_title = f'<div class="session-info">Сессия мониторинга: <strong>{session_name}</strong> (ID: {session_id})</div>'
        
        # HTML шаблон (используем подход с html_parts для удобства)
        html_parts = []
        
        # 1. Начало HTML и заголовок
        html_parts.append(f"""<!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Конкуренты: {project_name}</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }}
            
            body {{
                background-color: #f5f7fa;
                color: #333;
                line-height: 1.6;
                padding: 20px;
            }}
            
            .container {{
                max-width: 1800px;
                margin: 0 auto;
                background: white;
                border-radius: 10px;
                box-shadow: 0 2px 20px rgba(0, 0, 0, 0.1);
                overflow: hidden;
            }}
            
            .header {{
                background: linear-gradient(135deg, #2c3e50, #4a6491);
                color: white;
                padding: 25px 30px;
                border-bottom: 4px solid #3498db;
            }}
            
            .header h1 {{
                font-size: 28px;
                margin-bottom: 5px;
                font-weight: 600;
            }}
            
            .header .subtitle {{
                font-size: 16px;
                opacity: 0.9;
                margin-bottom: 10px;
            }}
            
            .session-info {{
                background: rgba(52, 152, 219, 0.2);
                padding: 8px 12px;
                border-radius: 4px;
                font-size: 14px;
                margin-bottom: 10px;
                border-left: 4px solid #3498db;
            }}
            
            .header .meta {{
                display: flex;
                gap: 20px;
                font-size: 14px;
                opacity: 0.8;
            }}
            
            .stats {{
                background: #f8f9fa;
                padding: 20px 30px;
                border-bottom: 1px solid #e9ecef;
                display: flex;
                justify-content: space-between;
                flex-wrap: wrap;
                gap: 15px;
            }}
            
            .stat-item {{
                text-align: center;
                min-width: 120px;
            }}
            
            .stat-value {{
                font-size: 24px;
                font-weight: bold;
                color: #2c3e50;
            }}
            
            .stat-label {{
                font-size: 13px;
                color: #6c757d;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            
            .table-container {{
                padding: 20px 30px;
                overflow-x: auto;
                position: relative;
            }}
            
            table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                min-width: 1200px;
            }}
            
            thead {{
                background: #f1f3f4;
                position: sticky;
                top: 0;
                z-index: 20;
            }}
            
            th {{
                padding: 15px 12px;
                text-align: left;
                font-weight: 600;
                color: #2c3e50;
                border-bottom: 2px solid #dee2e6;
                font-size: 14px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                position: relative;
            }}
            
            td {{
                padding: 12px;
                border-bottom: 1px solid #e9ecef;
                font-size: 13px;
                vertical-align: top;
                position: relative;
            }}
            
            tbody tr:hover {{
                background-color: #f8f9fa;
                transition: background-color 0.2s;
            }}
            
            .keyword-cell {{
                font-weight: 500;
                color: #2c3e50;
                white-space: nowrap;
                min-width: 180px;
                width: 180px;
                position: sticky;
                left: 0;
                background: white;
                border-right: 2px solid #dee2e6;
                z-index: 15;
                box-shadow: 2px 0 5px rgba(0, 0, 0, 0.05);
            }}
            
            th.keyword-cell {{
                z-index: 25;
                background: #f1f3f4;
            }}
            
            .session-header {{
                white-space: nowrap;
                min-width: 150px;
                text-align: center;
            }}
            
            .session-name {{
                font-weight: 600;
                color: #2c3e50;
                margin-bottom: 3px;
                font-size: 13px;
            }}
            
            .session-date {{
                font-size: 12px;
                color: #6c757d;
                margin-bottom: 2px;
            }}
            
            .session-time {{
                font-size: 11px;
                color: #999;
            }}
            
            .competitor-list {{
                max-height: 300px;
                overflow-y: auto;
                padding-right: 5px;
            }}
            
            .competitor-list::-webkit-scrollbar {{
                width: 6px;
            }}
            
            .competitor-list::-webkit-scrollbar-track {{
                background: #f1f1f1;
                border-radius: 3px;
            }}
            
            .competitor-list::-webkit-scrollbar-thumb {{
                background: #c1c1c1;
                border-radius: 3px;
            }}
            
            .competitor-list::-webkit-scrollbar-thumb:hover {{
                background: #a8a8a8;
            }}
            
            .competitor-item {{
                margin-bottom: 8px;
                padding: 6px 8px;
                background: #f8f9fa;
                border-radius: 4px;
                border-left: 3px solid #3498db;
                position: relative;
            }}
            
            .competitor-item:hover {{
                background: #e9ecef;
                transform: translateX(2px);
                transition: all 0.2s ease;
            }}
            
            .our-site {{
                border-left: 3px solid #e74c3c !important;
                background: #fff5f5 !important;
            }}
            
            .our-site:hover {{
                background: #ffeaea !important;
            }}
            
            .missing-position {{
                border-left: 3px solid #95a5a6 !important;
                background: #f8f9fa !important;
                opacity: 0.8;
            }}
            
            .position-badge {{
                display: inline-block;
                min-width: 24px;
                padding: 2px 6px;
                background: #6c757d;
                color: white;
                border-radius: 10px;
                font-size: 11px;
                font-weight: bold;
                text-align: center;
                margin-right: 8px;
            }}
            
            .position-1 .position-badge {{
                background: #28a745;
            }}
            
            .position-2 .position-badge {{
                background: #20c997;
            }}
            
            .position-3 .position-badge {{
                background: #17a2b8;
            }}
            
            .our-site .position-badge {{
                background: #e74c3c;
            }}
            
            .competitor-url {{
                display: block;
                color: #0066cc;
                text-decoration: none;
                font-size: 12px;
                margin-top: 2px;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}
            
            .competitor-url:hover {{
                text-decoration: underline;
                color: #0056b3;
            }}
            
            .competitor-title {{
                display: block;
                color: #495057;
                font-size: 12px;
                margin-top: 2px;
                font-style: italic;
                overflow: hidden;
                text-overflow: ellipsis;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                max-height: 32px;
            }}
            
            .missing-position .competitor-title {{
                color: #95a5a6;
                font-style: normal;
            }}
            
            .empty-cell {{
                text-align: center;
                color: #6c757d;
                font-style: italic;
                padding: 20px;
                background: #f8f9fa;
            }}
            
            .footer {{
                padding: 20px 30px;
                text-align: center;
                color: #6c757d;
                font-size: 13px;
                border-top: 1px solid #e9ecef;
                background: #f8f9fa;
            }}
            
            .legend {{
                display: flex;
                justify-content: center;
                gap: 20px;
                margin-top: 10px;
                flex-wrap: wrap;
            }}
            
            .legend-item {{
                display: flex;
                align-items: center;
                gap: 5px;
                font-size: 12px;
            }}
            
            .legend-color {{
                width: 15px;
                height: 15px;
                border-radius: 3px;
            }}
            
            .session-id-badge {{
                background: #3498db;
                color: white;
                font-size: 10px;
                padding: 1px 4px;
                border-radius: 3px;
                margin-left: 4px;
            }}
            
            @media (max-width: 768px) {{
                .container {{
                    border-radius: 0;
                }}
                
                .header, .stats, .table-container, .footer {{
                    padding: 15px;
                }}
                
                .stats {{
                    flex-direction: column;
                    align-items: flex-start;
                }}
                
                .stat-item {{
                    min-width: auto;
                    text-align: left;
                }}
                
                .keyword-cell {{
                    position: relative;
                    z-index: auto;
                }}
                
                th.keyword-cell {{
                    position: relative;
                    z-index: auto;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏆 Топ-10 Конкуренты: {project_name}</h1>
                <div class="subtitle">Мониторинг конкурентов в поисковой выдаче</div>
                {session_title}
                <div class="meta">
                    <div>🌐 Домен: {domain}</div>
                    <div>📅 Дата отчета: {datetime.now().strftime('%d.%m.%Y %H:%M')}</div>
                    <div>🔍 Проверок в отчете: {total_checks}</div>
                    <div>{'🎯 Режим: отдельная сессия' if session_id else '🎯 Режим: все сессии'}</div>
                </div>
            </div>
            
            <div class="stats">
                <div class="stat-item">
                    <div class="stat-value">{total_keywords}</div>
                    <div class="stat-label">Ключевых слов</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{keywords_with_data}</div>
                    <div class="stat-label">С данными</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{total_checks}</div>
                    <div class="stat-label">Проверок</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{our_domain_count}</div>
                    <div class="stat-label">Наш домен в топ-10</div>
                </div>
            </div>""")
        
        # 2. Блок с нашими позициями (если есть)
        if our_positions_summary:
            positions_html = "<br>".join(our_positions_summary)
            html_parts.append(f"""
            <div style="padding: 10px 30px; background: #fff5f5; border-left: 4px solid #e74c3c; margin: 0 30px 15px 30px; border-radius: 4px;">
                <strong>🏆 Наши позиции в топ-10:</strong><br>
                {positions_html}
            </div>""")
        
        # 3. Начало таблицы
        html_parts.append("""
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th class="keyword-cell">Ключевое слово</th>""")
        
        # 4. Заголовки с информацией о сессиях (как в html_reporter.py)
        for session in sessions:
            session_name = session.get('session_name', f"Запуск {session['date']}")
            date_part = session['date']
            time_part = session['time']
            session_id_from_data = session.get('session_id')
            
            html_parts.append(f'''                        <th class="session-header">
                                <div class="session-name">{session_name}</div>
                                <div class="session-date">{date_part}</div>
                                <div class="session-time">{time_part}</div>''')
            
            if session_id_from_data:
                html_parts.append(f'''                            <div class="session-time">ID: {session_id_from_data}</div>''')
            
            html_parts.append('''                        </th>''')
        
        html_parts.append("""
                        </tr>
                    </thead>
                    <tbody>""")
        
        # 5. Строки с данными по ключевым словам
        for idx, keyword in enumerate(sorted_keywords):
            row_class = "even" if idx % 2 == 0 else "odd"
            
            # Ячейка с ключевым словом
            html_parts.append(f"""
                        <tr class="{row_class}">
                            <td class="keyword-cell">
                                <div style="font-weight: 500; margin-bottom: 5px;">{keyword}</div>""")
            
            # Добавляем статистику по ключевому слову
            keyword_stat = stats.get(keyword, {})
            if keyword_stat.get('latest_session'):
                html_parts.append(f"""
                                <div style="font-size: 11px; color: #6c757d; line-height: 1.3;">
                                    <div>📅 {keyword_stat["latest_session"]}</div>
                                    <div>🏢 Доменов: {keyword_stat["unique_domains"]}</div>""")
                if keyword_stat.get('top_position'):
                    html_parts.append(f"""
                                    <div>🏆 Лучший конкурент: {keyword_stat["top_position"]} позиция</div>""")
                if keyword_stat.get('our_positions'):
                    html_parts.append(f"""
                                    <div style="color: #e74c3c; font-weight: 500;">🎯 Наши позиции: {", ".join(map(str, keyword_stat["our_positions"]))}</div>""")
                html_parts.append("""
                                </div>""")
            
            html_parts.append("""
                            </td>""")
            
            # Ячейки с данными по сессиям
            for session in sessions:
                session_key = session['session_key']
                competitors = top10.get(keyword, {}).get(session_key, [])
                
                html_parts.append("""
                            <td>""")
                
                if competitors:
                    html_parts.append("""
                                <div class="competitor-list">""")
                    
                    for competitor in competitors:
                        # Определяем классы для позиции
                        position_class = ""
                        if competitor['position'] <= 3:
                            position_class = f"position-{competitor['position']}"
                        
                        # Добавляем класс для нашего сайта
                        if competitor.get('is_our_site'):
                            position_class += " our-site"
                        
                        # Добавляем класс для пропущенных позиций
                        if competitor.get('is_missing'):
                            position_class += " missing-position"
                        
                        # Форматируем заголовок
                        title_display = competitor['title'][:60] + "..." if len(competitor['title']) > 60 else competitor['title']
                        
                        html_parts.append(f"""
                                    <div class="competitor-item {position_class.strip()}">
                                        <span class="position-badge">{competitor["position"]}</span>""")
                        
                        # Особое оформление в зависимости от типа
                        if competitor.get('is_our_site'):
                            html_parts.append(f"""
                                        <span style="font-weight: 600; color: #e74c3c;">🏆 {competitor["domain"]}</span>""")
                        elif competitor.get('is_missing'):
                            html_parts.append(f"""
                                        <span style="font-weight: 500; color: #95a5a6;">❓ {competitor["domain"]}</span>""")
                        else:
                            html_parts.append(f"""
                                        <span style="font-weight: 500;">{competitor["domain"]}</span>""")
                        
                        # URL (если есть)
                        if competitor['url'] and not competitor.get('is_missing'):
                            html_parts.append(f"""
                                        <a href="{competitor["url"]}" target="_blank" class="competitor-url" title="{competitor["url"]}">
                                            {competitor["display_url"]}
                                        </a>""")
                        
                        # Заголовок (если есть)
                        if competitor['title']:
                            html_parts.append(f"""
                                        <span class="competitor-title" title="{competitor["title"]}">{title_display}</span>""")
                        elif competitor.get('is_missing'):
                            html_parts.append(f"""
                                        <span class="competitor-title">Позиция отсутствует в данных выдачи</span>""")
                        
                        html_parts.append("""
                                    </div>""")
                    
                    html_parts.append("""
                                </div>""")
                else:
                    html_parts.append("""
                                <div class="empty-cell">
                                    Нет данных
                                </div>""")
                
                html_parts.append("""
                            </td>""")
        
            # Закрываем строку таблицы
            html_parts.append("""
                        </tr>""")
        
        # 6. Закрываем таблицу и добавляем футер
        html_parts.append("""
                    </tbody>
                </table>
            </div>
            
            <div class="footer">
                <div>Отчет сгенерирован SEO-агентом • """ + datetime.now().strftime('%d.%m.%Y %H:%M') + """</div>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #28a745;"></div>
                        <span>1-я позиция</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #20c997;"></div>
                        <span>2-я позиция</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #17a2b8;"></div>
                        <span>3-я позиция</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #e74c3c;"></div>
                        <span>Наш сайт</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #95a5a6;"></div>
                        <span>Пропущенная позиция</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #6c757d;"></div>
                        <span>4-10 позиции</span>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            document.addEventListener('DOMContentLoaded', function() {{
                const competitorLists = document.querySelectorAll('.competitor-list');
                competitorLists.forEach(list => {{
                    list.addEventListener('wheel', function(e) {{
                        if (e.deltaY !== 0) {{
                            this.scrollTop += e.deltaY;
                            e.preventDefault();
                        }}
                    }}, {{ passive: false }});
                }});
                
                document.querySelectorAll('.competitor-item').forEach(item => {{
                    item.addEventListener('mouseenter', function() {{
                        const row = this.closest('tr');
                        if (row) {{
                            row.style.backgroundColor = '#f0f7ff';
                        }}
                    }});
                    
                    item.addEventListener('mouseleave', function() {{
                        const row = this.closest('tr');
                        if (row && !row.matches(':hover')) {{
                            row.style.backgroundColor = '';
                        }}
                    }});
                }});
                
                const sessionHeaders = document.querySelectorAll('th.session-header');
                if (sessionHeaders.length > 2) {{
                    const tableContainer = document.querySelector('.table-container');
                    setTimeout(() => {{
                        tableContainer.scrollLeft = tableContainer.scrollWidth;
                    }}, 100);
                }}
                
                document.querySelectorAll('th.session-header').forEach(header => {{
                    header.style.cursor = 'pointer';
                    header.title = 'Клик для сортировки по этой дате';
                    header.addEventListener('click', function() {{
                        const sessionName = this.querySelector('.session-name').textContent;
                        const dateText = this.querySelector('.session-date').textContent;
                        const timeText = this.querySelector('.session-time').textContent;
                        console.log('Сортировка по сессии:', sessionName, dateText, timeText);
                    }});
                }});
                
                const tableContainer = document.querySelector('.table-container');
                if (tableContainer) {{
                    tableContainer.addEventListener('scroll', function() {{
                        const keywordCells = document.querySelectorAll('td.keyword-cell, th.keyword-cell');
                        const scrollLeft = this.scrollLeft;
                        
                        if (scrollLeft > 10) {{
                            keywordCells.forEach(cell => {{
                                cell.style.boxShadow = '3px 0 8px rgba(0, 0, 0, 0.1)';
                            }});
                        }} else {{
                            keywordCells.forEach(cell => {{
                                cell.style.boxShadow = '2px 0 5px rgba(0, 0, 0, 0.05)';
                            }});
                        }}
                    }});
                }}
                
                document.querySelectorAll('.our-site').forEach(ourSite => {{
                    ourSite.title = 'Это наш сайт! Кликните для перехода';
                    ourSite.style.cursor = 'pointer';
                    ourSite.addEventListener('click', function(e) {{
                        if (e.target.tagName !== 'A') {{
                            const link = this.querySelector('a.competitor-url');
                            if (link) {{
                                window.open(link.href, '_blank');
                            }}
                        }}
                    }});
                }});
                
                document.querySelectorAll('.missing-position').forEach(missing => {{
                    missing.title = 'Эта позиция отсутствует в данных выдачи. Возможно, это рекламный блок или специальный результат.';
                }});
            }});
        </script>
    </body>
    </html>""")
        
        return "".join(html_parts)