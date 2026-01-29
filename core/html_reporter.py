#!/usr/bin/env python3
"""
Модуль для генерации HTML отчетов по позициям
Добавлена поддержка сессий мониторинга
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import json
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class HTMLReporter:
    """
    Генератор HTML отчетов для SEO-агента
    """
    
    def __init__(self, db_path: str = "data/seo_data.db"):
        """
        Инициализация генератора отчетов
        
        Args:
            db_path: Путь к базе данных SQLite
        """
        self.db_path = Path(db_path)
        self.reports_dir = Path("data/reports/html")
        self.reports_dir.mkdir(exist_ok=True, parents=True)
        
        logger.info(f"HTMLReporter инициализирован. База: {db_path}")
    
    def generate_positions_report(self, project_name: str, domain: str, session_id: Optional[int] = None) -> str:
        """
        Генерирует HTML отчет с таблицей позиций
        
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
            logger.info(f"Генерация HTML отчета для {project_name} ({domain}), сессия: {session_id}")
        else:
            logger.info(f"Генерация HTML отчета для {project_name} ({domain}), все сессии")
        
        # Получаем данные из БД
        data = self._get_positions_data(domain, session_id)
        
        if not data:
            logger.warning(f"Нет данных для отчета {project_name}")
            return ""
        
        # Генерируем HTML
        html_content = self._create_html_report(project_name, domain, data, session_id)
        
        # Сохраняем файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if session_id:
            filename = f"positions_{project_name.lower().replace(' ', '_')}_session{session_id}_{timestamp}.html"
        else:
            filename = f"positions_{project_name.lower().replace(' ', '_')}_{timestamp}.html"
            
        filepath = self.reports_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML отчет сохранён: {filepath}")
        
        # Также создаем симлинк на latest
        if session_id:
            latest_file = self.reports_dir / f"latest_{project_name.lower().replace(' ', '_')}_session{session_id}.html"
        else:
            latest_file = self.reports_dir / f"latest_{project_name.lower().replace(' ', '_')}.html"
            
        if latest_file.exists():
            latest_file.unlink()
        latest_file.symlink_to(filepath.name)
        
        return str(filepath)
    
    def _get_positions_data(self, domain: str, session_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Получает данные о позициях из базы данных
        
        Args:
            domain: Домен проекта
            session_id: ID сессии для фильтрации (опционально)
            
        Returns:
            Словарь с данными для отчета
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
                
                # 3. Подготавливаем запрос в зависимости от наличия session_id
                keyword_ids_str = ','.join(str(kid) for kid in keywords.keys())
                
                if session_id:
                    # Фильтруем по конкретной сессии
                    query = f"""
                        SELECT 
                            p.keyword_id,
                            p.check_date,
                            p.check_time,
                            p.position,
                            p.search_engine,
                            p.session_id,
                            s.start_time as session_start_time
                        FROM positions p
                        LEFT JOIN monitoring_sessions s ON p.session_id = s.session_id
                        WHERE p.keyword_id IN ({keyword_ids_str})
                        AND p.session_id = ?
                        ORDER BY p.check_date DESC, p.check_time DESC
                    """
                    params = (session_id,)
                else:
                    # Берем все данные
                    query = f"""
                        SELECT 
                            p.keyword_id,
                            p.check_date,
                            p.check_time,
                            p.position,
                            p.search_engine,
                            p.session_id,
                            s.start_time as session_start_time
                        FROM positions p
                        LEFT JOIN monitoring_sessions s ON p.session_id = s.session_id
                        WHERE p.keyword_id IN ({keyword_ids_str})
                        ORDER BY p.check_date DESC, p.check_time DESC
                    """
                    params = ()
                
                cursor = conn.execute(query, params)
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning(f"Нет данных позиций для проекта {domain}" + 
                                  (f" в сессии {session_id}" if session_id else ""))
                    return {}
                
                # 4. Определяем сессии на основе session_id или времени
                sessions = {}  # {session_key: {date, start_time, session_id, session_name, keywords: []}}
                
                for row in rows:
                    keyword_id = row['keyword_id']
                    check_date = row['check_date']
                    check_time = row['check_time']
                    position = row['position']
                    session_id_from_db = row['session_id']
                    session_start_time = row['session_start_time']
                    
                    # Определяем ключ сессии
                    if session_id_from_db:
                        # Используем реальный session_id
                        session_key = f"session_{session_id_from_db}"
                        
                        if session_key not in sessions:
                            # Получаем информацию о сессии
                            cursor_session = conn.execute(
                                "SELECT session_name, start_time FROM monitoring_sessions WHERE session_id = ?",
                                (session_id_from_db,)
                            )
                            session_info = cursor_session.fetchone()
                            
                            session_name = session_info['session_name'] if session_info else f"Сессия {session_id_from_db}"
                            session_display_time = session_info['start_time'] if session_info else check_time
                            
                            # Извлекаем только время из timestamp
                            if session_display_time:
                                if ' ' in session_display_time:
                                    # Формат: '2026-01-28 18:45:30'
                                    session_time_part = session_display_time.split(' ')[1]
                                    if '.' in session_time_part:
                                        session_time_part = session_time_part.split('.')[0]
                                else:
                                    session_time_part = session_display_time
                            else:
                                session_time_part = check_time.split('.')[0] if '.' in check_time else check_time
                            
                            sessions[session_key] = {
                                'date': check_date,
                                'time': session_time_part,
                                'session_id': session_id_from_db,
                                'session_name': session_name,
                                'keywords': {}
                            }
                    else:
                        # Старая логика для записей без session_id
                        if '.' in check_time:
                            time_without_ms = check_time.split('.')[0]
                        else:
                            time_without_ms = check_time
                        
                        session_key = f"{check_date} {time_without_ms}"
                        
                        if session_key not in sessions:
                            sessions[session_key] = {
                                'date': check_date,
                                'time': time_without_ms,
                                'session_id': None,
                                'session_name': f"Запуск {check_date} {time_without_ms}",
                                'keywords': {}
                            }
                    
                    # Добавляем позицию для этого ключевого слова в сессию
                    sessions[session_key]['keywords'][keyword_id] = {
                        'position': position,
                        'exact_time': check_time,
                        'search_engine': row['search_engine']
                    }
                
                # 5. Формируем структуру данных для отчета
                data = {
                    'keywords': keywords,
                    'positions': {},
                    'sessions': [],  # Список сессий
                    'stats': {},
                    'has_session_id': session_id is not None
                }
                
                # Сортируем сессии по дате и времени (новые сверху)
                sorted_sessions = sorted(
                    sessions.keys(), 
                    key=lambda k: (sessions[k]['date'], sessions[k]['time']), 
                    reverse=True
                )
                
                for session_key in sorted_sessions:
                    session = sessions[session_key]
                    data['sessions'].append({
                        'date': session['date'],
                        'time': session['time'],
                        'session_id': session['session_id'],
                        'session_name': session['session_name']
                    })
                
                # 6. Заполняем позиции для каждой сессии
                for session_key in sorted_sessions:
                    session = sessions[session_key]
                    
                    if session['session_id']:
                        session_display = f"Сессия {session['session_id']}"
                    else:
                        session_display = f"{session['date']} {session['time']}"
                    
                    for keyword_id, pos_data in session['keywords'].items():
                        keyword = keywords[keyword_id]
                        
                        if keyword not in data['positions']:
                            data['positions'][keyword] = {}
                        
                        data['positions'][keyword][session_display] = {
                            'position': pos_data['position'],
                            'search_engine': pos_data['search_engine'],
                            'exact_time': pos_data['exact_time'],
                            'session_id': session['session_id']
                        }
                
                # 7. Рассчитываем статистику
                for keyword in keywords.values():
                    if keyword in data['positions']:
                        positions = [p['position'] for p in data['positions'][keyword].values() if p['position'] is not None]
                        
                        if positions:
                            data['stats'][keyword] = {
                                'best': min(positions),
                                'worst': max(positions),
                                'avg': round(sum(positions) / len(positions), 1),
                                'count': len(positions),
                                'sessions_count': len(data['positions'][keyword])
                            }
                        else:
                            data['stats'][keyword] = {
                                'best': None,
                                'worst': None,
                                'avg': None,
                                'count': 0,
                                'sessions_count': 0
                            }
                    else:
                        data['stats'][keyword] = {
                            'best': None,
                            'worst': None,
                            'avg': None,
                            'count': 0,
                            'sessions_count': 0
                        }
                
                return data
                
        except Exception as e:
            logger.error(f"Ошибка получения данных из БД: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
    
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
        keywords = data.get('keywords', {})  # Это словарь {id: keyword}
        positions = data.get('positions', {})  # Это {keyword: {session: {position: X}}}
        sessions = data.get('sessions', [])  # Это список сессий
        stats = data.get('stats', {})  # Это {keyword: {stats...}}
        has_session_id = data.get('has_session_id', False)
        
        # Сортируем ключевые слова по их названиям
        keyword_names = list(keywords.values())
        sorted_keywords = sorted(keyword_names)
        
        # Заголовок с информацией о сессии
        session_title = ""
        if session_id:
            # Находим название сессии
            session_name = ""
            for s in sessions:
                if s.get('session_id') == session_id:
                    session_name = s.get('session_name', f"Сессия {session_id}")
                    break
            
            session_title = f"<div class=\"session-info\">Сессия мониторинга: <strong>{session_name}</strong> (ID: {session_id})</div>"
        
        # HTML шаблон
        html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SEO Отчет: {project_name}</title>
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
            max-width: 1400px;
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
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            min-width: 800px;
        }}
        
        thead {{
            background: #f1f3f4;
            position: sticky;
            top: 0;
            z-index: 10;
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
        }}
        
        td {{
            padding: 14px 12px;
            border-bottom: 1px solid #e9ecef;
            font-size: 14px;
        }}
        
        tbody tr:hover {{
            background-color: #f8f9fa;
            transition: background-color 0.2s;
        }}
        
        .keyword-cell {{
            font-weight: 500;
            color: #2c3e50;
            white-space: nowrap;
            min-width: 200px;
            position: sticky;
            left: 0;
            background: white;
        }}
        
        .position-cell {{
            text-align: center;
            min-width: 70px;
            font-weight: 500;
        }}
        
        .position-1 {{
            background-color: #d4edda;
            color: #155724;
            font-weight: bold;
        }}
        
        .position-3 {{
            background-color: #fff3cd;
            color: #856404;
        }}
        
        .position-10 {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        
        .position-null {{
            background-color: #e2e3e5;
            color: #383d41;
            font-style: italic;
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
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 SEO Отчет: {project_name}</h1>
            <div class="subtitle">Мониторинг позиций в поисковой выдаче</div>
            {session_title}
            <div class="meta">
                <div>🌐 Домен: {domain}</div>
                <div>📅 Дата отчета: {datetime.now().strftime('%d.%m.%Y %H:%M')}</div>
                <div>🔍 Проверок в отчете: {len(sessions)}</div>
                {'<div>🎯 Режим: отдельная сессия</div>' if session_id else '<div>🎯 Режим: все сессии</div>'}
            </div>
        </div>
        
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">{len(sorted_keywords)}</div>
                <div class="stat-label">Ключевых слов</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{len(sessions)}</div>
                <div class="stat-label">Проверок</div>
            </div>
"""
        
        # Считаем статистику
        top1_count = 0
        top10_count = 0
        
        for keyword in sorted_keywords:
            keyword_stats = stats.get(keyword, {})
            best_pos = keyword_stats.get('best')
            avg_pos = keyword_stats.get('avg', 100)
            
            if best_pos == 1:
                top1_count += 1
            if avg_pos and avg_pos <= 10:
                top10_count += 1
        
        html += f"""
                <div class="stat-item">
                    <div class="stat-value">{top1_count}</div>
                    <div class="stat-label">Топ-1 позиций</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{top10_count}</div>
                    <div class="stat-label">В топ-10 в среднем</div>
                </div>
            </div>
            
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th class="keyword-cell">Ключевое слово</th>
"""
        
        # Добавляем заголовки с сессиями проверок
        for session in sessions:
            date_part = session['date']
            time_part = session['time']
            session_name = session.get('session_name', f"Запуск {date_part}")
            session_id_from_data = session.get('session_id')
            
            html += f'''                        <th class="session-header">
                            <div class="session-name">{session_name}</div>
                            <div class="session-date">{date_part}</div>
                            <div class="session-time">{time_part}</div>
'''
            if session_id_from_data:
                html += f'''                            <div class="session-time">ID: {session_id_from_data}</div>
'''
            html += '''                        </th>
'''
        
        html += """                    </tr>
                    </thead>
                    <tbody>
"""
        
        # Добавляем строки с позициями
        for idx, keyword in enumerate(sorted_keywords):
            row_class = "even" if idx % 2 == 0 else "odd"
            html += f'                    <tr class="{row_class}">\n'
            html += f'                        <td class="keyword-cell">{keyword}</td>\n'
            
            keyword_stats = stats.get(keyword, {})
            if keyword_stats.get('count', 0) > 0:
                html += f'                        <!-- Статистика: лучшая {keyword_stats.get("best")}, худшая {keyword_stats.get("worst")}, средняя {keyword_stats.get("avg")} -->\n'
            
            for session in sessions:
                date_part = session['date']
                time_part = session['time']
                session_id_from_data = session.get('session_id')
                
                # Определяем ключ для поиска позиции
                if session_id_from_data:
                    session_key = f"Сессия {session_id_from_data}"
                else:
                    session_key = f"{date_part} {time_part}"
                
                position_data = positions.get(keyword, {}).get(session_key, {})
                position = position_data.get('position')
                
                # Определяем CSS класс для позиции
                position_class = ""
                if position is None:
                    position_class = "position-null"
                    display_value = "—"
                else:
                    if position == 1:
                        position_class = "position-1"
                    elif position <= 3:
                        position_class = "position-3"
                    elif position > 10:
                        position_class = "position-10"
                    display_value = str(position)
                
                # Добавляем подсказку
                exact_time = position_data.get('exact_time', '')
                title_attr = f'title="Позиция: {display_value}"'
                if exact_time and exact_time != time_part:
                    title_attr = f'title="Позиция: {display_value}\\nТочное время: {exact_time}"'
                
                html += f'                        <td class="position-cell {position_class}" {title_attr}>{display_value}</td>\n'
            
            html += '                    </tr>\n'
        
        html += f"""                </tbody>
                </table>
            </div>
            
            <div class="footer">
                <div>Отчет сгенерирован SEO-агентом • {datetime.now().strftime("%d.%m.%Y %H:%M")}</div>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #d4edda;"></div>
                        <span>Топ-1</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #fff3cd;"></div>
                        <span>Топ-3</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #f8d7da;"></div>
                        <span>Ниже топ-10</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background-color: #e2e3e5;"></div>
                        <span>Нет данных</span>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            // Скрипт для динамического обновления заголовков при прокрутке
            document.addEventListener('DOMContentLoaded', function() {{
                const table = document.querySelector('table');
                const keywordCells = document.querySelectorAll('td.keyword-cell');
                
                // Фиксируем заголовки при горизонтальной прокрутке
                table.addEventListener('scroll', function() {{
                    const scrollLeft = table.scrollLeft;
                    
                    // Обновляем позицию sticky-колонки с ключевыми словами
                    keywordCells.forEach(cell => {{
                        cell.style.transform = `translateX(${{scrollLeft}}px)`;
                    }});
                }});
                
                // Добавляем подсказки для позиций
                document.querySelectorAll('.position-cell').forEach(cell => {{
                    if (cell.textContent !== '—') {{
                        if (!cell.hasAttribute('title')) {{
                            cell.title = 'Позиция: ' + cell.textContent;
                        }}
                    }}
                }});
            }});
        </script>
    </body>
</html>"""
        
        return html
    
    def generate_all_projects_report(self) -> List[str]:
        """
        Генерирует отчеты для всех проектов
        
        Returns:
            Список путей к созданным файлам
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT name, domain FROM projects"
                )
                projects = cursor.fetchall()
                
                report_paths = []
                for project in projects:
                    try:
                        path = self.generate_positions_report(
                            project_name=project[0],
                            domain=project[1]
                        )
                        if path:
                            report_paths.append(path)
                    except Exception as e:
                        logger.error(f"Ошибка генерации отчета для {project[0]}: {e}")
                        continue
                
                return report_paths
                
        except Exception as e:
            logger.error(f"Ошибка генерации отчетов: {e}")
            return []


# ========== ТЕСТОВАЯ ФУНКЦИЯ ==========
def test_html_reporter():
    """Тестирование HTML репортера"""
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 ТЕСТИРУЕМ HTML REPORTER (СЕССИОННАЯ ВЕРСИЯ)")
    print("=" * 50)
    
    try:
        # Создаём репортер
        reporter = HTMLReporter()
        print("✅ HTMLReporter создан")
        
        # Пробуем найти первый проект в БД
        db_path = Path("data/seo_data.db")
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute("SELECT name, domain FROM projects LIMIT 1")
                project = cursor.fetchone()
                
                if project:
                    print(f"   Найден проект: {project[0]} ({project[1]})")
                    
                    # Тест 1: отчет без сессии (все данные)
                    print("\n1. 📊 Генерация отчета без сессии (все данные)...")
                    report_path = reporter.generate_positions_report(
                        project_name=project[0],
                        domain=project[1]
                    )
                    
                    if report_path:
                        print(f"✅ Отчет сгенерирован: {report_path}")
                    else:
                        print("⚠️  Отчет не сгенерирован (нет данных)")
                    
                    # Тест 2: отчет с сессией (если есть сессии)
                    print("\n2. 🎯 Генерация отчета с сессией...")
                    cursor_sessions = conn.execute("SELECT session_id FROM monitoring_sessions LIMIT 1")
                    session_row = cursor_sessions.fetchone()
                    
                    if session_row:
                        session_id = session_row[0]
                        print(f"   Найдена сессия: {session_id}")
                        
                        report_path_session = reporter.generate_positions_report(
                            project_name=project[0],
                            domain=project[1],
                            session_id=session_id
                        )
                        
                        if report_path_session:
                            print(f"✅ Отчет по сессии сгенерирован: {report_path_session}")
                        else:
                            print("⚠️  Отчет по сессии не сгенерирован (нет данных в этой сессии)")
                    else:
                        print("ℹ️  Нет сессий в базе данных")
                        
                else:
                    print("ℹ️  Нет проектов в базе данных")
                    print("   Сначала запустите сбор данных: python seo_agent.py")
        else:
            print("❌ База данных не найдена: data/seo_data.db")
            print("   Сначала запустите сбор данных: python seo_agent.py")
        
        print("\n" + "=" * 50)
        print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ТЕСТИРОВАНИИ: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_html_reporter()