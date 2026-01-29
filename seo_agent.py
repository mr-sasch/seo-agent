#!/usr/bin/env python3
"""
Главный файл SEO-агента
Запускает полный цикл мониторинга для всех проектов
Добавлена поддержка сессий мониторинга
"""

import sys
from pathlib import Path
import logging
import yaml
from datetime import datetime

sys.path.append(str(Path(__file__).parent))

from core.data_collector import DataCollector
from core.threat_detector import ThreatDetector
from core.html_reporter import HTMLReporter
from core.html_reporter_competitor import CompetitorHTMLReporter


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/seo-agent.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def load_projects():
    """Загружает проекты из конфигурации"""
    try:
        config_path = Path("config/projects.yaml")
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        projects = config.get('projects', [])
        logger.info(f"Загружено проектов: {len(projects)}")
        return projects
        
    except Exception as e:
        logger.error(f"Ошибка загрузки конфигурации: {e}")
        return []

def run_seo_check():
    """Запускает полный SEO-чек с сессиями мониторинга"""
    logger.info("🚀 ЗАПУСК SEO-АГЕНТА (СЕССИОННАЯ ВЕРСИЯ)")
    logger.info("=" * 50)
    
    projects = load_projects()
    
    if not projects:
        logger.error("❌ Нет проектов для анализа")
        return
    
    # Общее время запуска скрипта
    script_start_time = datetime.now()
    logger.info(f"⏰ Время запуска скрипта: {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for project in projects:
        try:
            logger.info(f"\n📋 АНАЛИЗ ПРОЕКТА: {project.get('name')}")
            logger.info(f"🌐 Домен: {project.get('domain')}")
            
            # 1. Собираем данные с созданием сессии
            collector = DataCollector()
            keywords = project.get('keywords', [])
            
            if not keywords:
                logger.warning(f"Нет ключевых слов для проекта {project['name']}")
                continue
            
            logger.info(f"🔍 Проверяем {len(keywords)} ключевых слов...")
            
            # ИСПРАВЛЕННЫЙ ВЫЗОВ: используем метод с сессией
            positions, session_id = collector.check_positions_with_session(
                domain=project['domain'],
                keywords=keywords,
                session_name=f"SEO-мониторинг {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                search_engine="yandex",
                use_cache=True,
                track_competitors=True,
                competitors_limit=20
            )
            
            logger.info(f"📊 Создана сессия мониторинга: {session_id}")
            
            # 2. Анализируем угрозы (ThreatDetector тоже нужно будет обновить для работы с сессиями)
            detector = ThreatDetector()
            
            try:
                # Пробуем вызвать с session_id если метод поддерживает
                analysis = detector.analyze_project(
                    project_name=project['name'],
                    domain=project['domain'],
                    session_id=session_id
                )
            except TypeError as e:
                # Если не поддерживает session_id, вызываем без него
                logger.warning(f"ThreatDetector не поддерживает session_id: {e}")
                analysis = detector.analyze_project(
                    project_name=project['name'],
                    domain=project['domain']
                )
            
            # 3. Выводим результат
            threats = analysis.get('threats', [])
            
            if threats:
                logger.warning(f"⚠️  Обнаружено угроз: {len(threats)}")
                for threat in threats:
                    logger.warning(
                        f"   • {threat.get('keyword')}: "
                        f"{threat.get('type')} (уровень: {threat.get('threat_level')})"
                    )
            else:
                logger.info("✅ Значительных угроз не обнаружено")
            
            # 4. Сохраняем расширенный отчёт с информацией о сессии
            report_file = f"data/reports/session_{session_id}_{project['name'].replace(' ', '_')}.txt"
            session_info = collector.db.get_latest_session(
                collector.db.get_or_create_project(project['name'], project['domain'])
            )
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write(f"ОТЧЁТ ПО ПРОЕКТУ: {project['name']}\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"📋 СЕССИЯ МОНИТОРИНГА\n")
                f.write(f"   ID: {session_id}\n")
                if session_info:
                    f.write(f"   Название: {session_info.get('session_name', 'Нет названия')}\n")
                    f.write(f"   Начало: {session_info.get('start_time')}\n")
                    f.write(f"   Статус: {session_info.get('status', 'unknown')}\n")
                    f.write(f"   Ключевых слов: {session_info.get('completed_keywords', 0)}/{session_info.get('total_keywords', 0)}\n")
                
                f.write(f"\n🌐 ПРОЕКТ\n")
                f.write(f"   Домен: {project['domain']}\n")
                f.write(f"   Проверено ключевых слов: {len(keywords)}\n")
                f.write(f"   Успешно проверено: {len([p for p in positions if p.get('position') is not None])}\n\n")
                
                f.write(f"📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ\n")
                for pos in positions:
                    if pos.get('position') is not None:
                        f.write(f"   • {pos['keyword']}: позиция {pos['position']}\n")
                    elif pos.get('error'):
                        f.write(f"   • {pos['keyword']}: ОШИБКА - {pos['error']}\n")
                
                f.write(f"\n📈 АНАЛИЗ БЕЗОПАСНОСТИ\n")
                f.write(f"   Общий статус: {analysis.get('overall_status', 'unknown')}\n")
                f.write(f"   Дата анализа: {analysis.get('analysis_date', 'Н/Д')}\n")
                
                threats = analysis.get('threats', [])
                if threats:
                    f.write(f"   Обнаружено угроз: {len(threats)}\n")
                    for threat in threats:
                        f.write(f"   • {threat.get('keyword')}: {threat.get('type')} ({threat.get('threat_level')})\n")
                        if threat.get('recommendation'):
                            f.write(f"     Рекомендация: {threat.get('recommendation')}\n")
                else:
                    f.write(f"   ✅ Значительных угроз не обнаружено\n")
                
                f.write(f"\n⏰ ВРЕМЕННЫЕ МЕТКИ\n")
                f.write(f"   Запуск скрипта: {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"   Время сессии: {positions[0].get('time', 'Н/Д') if positions else 'Н/Д'}\n")
                f.write(f"   Дата проверки: {positions[0].get('date', 'Н/Д') if positions else 'Н/Д'}\n")
            
            logger.info(f"📄 Отчёт сохранён: {report_file}")
            
            # 5. Сохраняем HTML отчет по позициям
            try:
                reporter = HTMLReporter()
                # Пробуем вызвать с session_id если метод поддерживает
                try:
                    html_report_path = reporter.generate_positions_report(
                        project_name=project['name'],
                        domain=project['domain'],
                        session_id=session_id
                    )
                except TypeError:
                    # Если не поддерживает session_id, вызываем без него
                    html_report_path = reporter.generate_positions_report(
                        project_name=project['name'],
                        domain=project['domain']
                    )
                
                if html_report_path:
                    logger.info(f"🌐 HTML отчёт сохранён: {html_report_path}")
            except Exception as e:
                logger.error(f"Ошибка генерации HTML отчёта: {e}")
            
            # 6. Сохраняем HTML отчет по выдаче   
            try:
                competitor_reporter = CompetitorHTMLReporter()
                # Пробуем вызвать с session_id если метод поддерживает
                try:
                    competitor_report_path = competitor_reporter.generate_top10_report(
                        project_name=project['name'],
                        domain=project['domain'],
                        session_id=session_id
                    )
                except TypeError:
                    # Если не поддерживает session_id, вызываем без него
                    competitor_report_path = competitor_reporter.generate_top10_report(
                        project_name=project['name'],
                        domain=project['domain']
                    )
                
                if competitor_report_path:
                    logger.info(f"🏆 HTML отчет по конкурентам сохранён: {competitor_report_path}")
            except Exception as e:
                logger.error(f"Ошибка генерации отчёта по конкурентам: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка анализа проекта {project.get('name')}: {e}")
            continue
    
    script_end_time = datetime.now()
    duration = script_end_time - script_start_time
    logger.info(f"\n⏰ Время завершения скрипта: {script_end_time.strftime('%H:%M:%S')}")
    logger.info(f"⏱️  Общее время выполнения: {duration.total_seconds():.1f} секунд")
    logger.info("✅ SEO-ПРОВЕРКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    run_seo_check()