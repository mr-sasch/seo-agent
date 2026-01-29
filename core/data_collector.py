#!/usr/bin/env python3
"""
Модуль сбора данных для SEO-агента (SQLite версия)
Добавлена поддержка сессий мониторинга
"""

import os
import sys
import json
import time
import logging
import sqlite3
from datetime import datetime, date, time as time_type
from pathlib import Path
from typing import Dict, List, Optional, Any

# Добавляем путь для импорта модулей core
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Теперь импортируем модули core
try:
    from core.database import SEODatabase
except ImportError:
    # Альтернативный путь для тестов
    from database import SEODatabase

# Настройка логирования
logger = logging.getLogger(__name__)

class DataCollector:
    """
    Основной класс для сбора SEO-данных (SQLite версия)
    
    Пример использования:
    collector = DataCollector()
    
    # Старый вариант (обратная совместимость)
    positions = collector.check_positions(
        domain="example.com",
        keywords=["купить ардуино", "датчики движения"]
    )
    
    # Новый вариант с сессией
    positions = collector.check_positions(
        domain="example.com",
        keywords=["купить ардуино", "датчики движения"],
        session_id=45  # ID существующей сессии
    )
    
    # Автоматическое создание сессии
    positions, session_id = collector.check_positions_with_session(
        domain="example.com",
        keywords=["купить ардуино", "датчики движения"],
        session_name="Ежедневная проверка"
    )
    """
    
    def __init__(self, config_path: str = "config/projects.yaml"):
        """
        Инициализация сборщика данных
        
        Args:
            config_path: Путь к файлу конфигурации проектов
        """
        self.config_path = Path(config_path)
        self.cache_dir = Path("data/cache")
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        
        # Инициализируем базу данных
        from core.database import SEODatabase
        self.db = SEODatabase()
        
        # Загружаем конфигурацию
        self.config = self._load_config()
        
        logger.info(f"DataCollector (SQLite) инициализирован. Проектов: {len(self.config.get('projects', []))}")
    
    def _load_config(self) -> Dict:
        """Загружает конфигурацию из YAML файла"""
        try:
            import yaml
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            return {"projects": []}
    
    def check_positions(self, 
                   domain: str, 
                   keywords: List[str],
                   session_id: Optional[int] = None,  # НОВЫЙ ПАРАМЕТР: ID сессии
                   search_engine: str = "yandex",
                   use_cache: bool = True,
                   track_competitors: bool = True,
                   competitors_limit: int = 20) -> List[Dict]:
        """
        Проверяет позиции сайта по ключевым словам и сохраняет в SQLite
        
        Args:
            domain: Домен сайта
            keywords: Список ключевых слов для проверки
            session_id: ID сессии мониторинга (опционально)
                - Если None: данные сохраняются без привязки к сессии (обратная совместимость)
                - Если передан: все данные привязываются к указанной сессии
            search_engine: Поисковая система (по умолчанию yandex)
            use_cache: Использовать кеширование
            track_competitors: Собирать данные о конкурентах
            competitors_limit: Максимальное количество конкурентов для сбора
            
        Returns:
            Список результатов проверки
        """
        logger.info(f"Проверяем позиции для {domain}. Ключевых слов: {len(keywords)}")
        
        # Находим проект в конфиге
        project = self._find_project_by_domain(domain)
        if not project:
            logger.error(f"Проект с доменом {domain} не найден в конфигурации")
            return []
        
        project_name = project.get('name', domain)
        
        # Получаем или создаём проект в базе
        project_id = self.db.get_or_create_project(project_name, domain)
        
        # Если передан session_id, проверяем что сессия существует
        if session_id is not None:
            session_info = self._get_session_info(session_id, project_id)
            if not session_info:
                logger.warning(f"Сессия {session_id} не найдена или не принадлежит проекту {project_id}")
                session_id = None  # Работаем без сессии
        
        # Определяем время проверки
        if session_id is not None:
            # Если есть сессия, используем её время начала как время проверки
            session_info = self._get_session_info(session_id, project_id)
            session_start_time = datetime.fromisoformat(session_info['start_time'].replace('Z', '+00:00'))
            check_date = session_start_time.date()
            check_time = session_start_time.time()
            logger.info(f"Используем сессию {session_id}, время запуска: {check_time}")
        else:
            # Без сессии - текущее время для каждой проверки (старая логика)
            logger.info("Работаем без сессии (обратная совместимость)")
        
        results = []
        all_competitors = []
        keyword_id = None
        
        for i, keyword in enumerate(keywords):
            try:
                logger.debug(f"Проверка ключа {i+1}/{len(keywords)}: '{keyword}'")
                
                # Если нет сессии, определяем время для каждого ключа отдельно
                if session_id is None:
                    current_time = datetime.now()
                    check_date = current_time.date()
                    check_time = current_time.time()
                
                # Проверяем кеш
                cache_key = f"{search_engine}_{domain}_{keyword}"
                cached_result = self._get_from_cache(cache_key) if use_cache else None
                
                if cached_result:
                    logger.debug(f"Используем кешированный результат для '{keyword}'")
                    
                    # Сохраняем кешированные данные в базу
                    keyword_id = self.db.get_or_create_keyword(project_id, keyword)
                    
                    self.db.save_position(
                        project_id=project_id,
                        keyword_id=keyword_id,
                        check_date=check_date,
                        check_time=check_time,
                        position=cached_result.get("position"),
                        url=cached_result.get("url", ""),
                        total_results=cached_result.get("total_results", 100),
                        search_engine=search_engine,
                        session_id=session_id  # ← Передаём session_id (может быть None)
                    )
                    
                    results.append(cached_result)
                    continue
                
                # Получаем позицию (реальная логика)
                position_data = self._get_position_from_search(
                    domain=domain,
                    keyword=keyword,
                    search_engine=search_engine,
                    include_competitors=track_competitors,
                    competitors_limit=competitors_limit
                )
                
                # Получаем или создаём ключевое слово в базе
                keyword_id = self.db.get_or_create_keyword(project_id, keyword)

                # Сохраняем нашу позицию в базу С session_id
                self.db.save_position(
                    project_id=project_id,
                    keyword_id=keyword_id,
                    check_date=check_date,
                    check_time=check_time,
                    position=position_data.get("position"),
                    url=position_data.get("url", ""),
                    total_results=position_data.get("total_results", 100),
                    search_engine=search_engine,
                    session_id=session_id  # ← Передаём session_id (может быть None)
                )

                # Сохраняем конкурентов если есть
                competitors = position_data.get("competitors", [])
                if competitors and track_competitors:
                    # Ограничиваем количество конкурентов
                    competitors = competitors[:competitors_limit]
                    
                    logger.info(f"Сохранение {len(competitors)} конкурентов для '{keyword}'")
                    
                    for comp in competitors[:3]:  # Логируем первые 3 конкурента
                        logger.debug(f"  - Позиция {comp.get('position')}: {comp.get('domain')}")
                    
                    self.db.save_competitors(
                        project_id=project_id,
                        keyword_id=keyword_id,
                        check_date=check_date,
                        check_time=check_time,
                        competitors=competitors,
                        session_id=session_id  # ← Передаём session_id (может быть None)
                    )
                    all_competitors.extend(competitors)
                
                # Формируем результат для возврата
                result = {
                    "keyword": keyword,
                    "position": position_data.get("position"),
                    "date": check_date.isoformat(),
                    "time": check_time.isoformat(),
                    "session_id": session_id,  # ← Добавляем ID сессии в результат
                    "search_engine": search_engine,
                    "url": position_data.get("url", ""),
                    "total_results": position_data.get("total_results", 100),
                    "method": position_data.get("method", "unknown")
                }
                
                results.append(result)
                
                # Сохраняем в кеш
                if use_cache:
                    self._save_to_cache(cache_key, result)
                
                # Пауза между запросами чтобы не заблокировали
                if i < len(keywords) - 1:
                    time.sleep(2)  # 2 секунды между запросами
                    
            except Exception as e:
                logger.error(f"Ошибка при проверке ключа '{keyword}': {e}")
                # Добавляем запись об ошибке
                results.append({
                    "keyword": keyword,
                    "position": None,
                    "error": str(e),
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "session_id": session_id
                })
                continue  # ← Продолжаем со следующим ключевым словом
        
        logger.info(f"Проверка завершена. Успешно: {len([r for r in results if r.get('position')])}")
        
        # Сохраняем снимок выдачи если были изменения
        if track_competitors and all_competitors:
            # Берем топ-10 конкурентов
            top_10 = sorted(all_competitors, key=lambda x: x.get('position', 101))[:10]
            
            # Нужен keyword_id для сохранения снапшота
            if keyword_id:
                self.db.save_snapshot_if_changed(
                    project_id=project_id,
                    keyword_id=keyword_id,
                    snapshot_date=check_date,
                    top_10=top_10
                )
        
        return results
    
    def check_positions_with_session(self,
                                   domain: str,
                                   keywords: List[str],
                                   session_name: Optional[str] = None,
                                   search_engine: str = "yandex",
                                   use_cache: bool = True,
                                   track_competitors: bool = True,
                                   competitors_limit: int = 20) -> tuple[List[Dict], int]:
        """
        Проверяет позиции с автоматическим созданием сессии
        
        Args:
            domain: Домен сайта
            keywords: Список ключевых слов для проверки
            session_name: Название сессии (опционально)
            search_engine: Поисковая система
            use_cache: Использовать кеширование
            track_competitors: Собирать данные о конкурентах
            competitors_limit: Максимальное количество конкурентов
            
        Returns:
            Кортеж (результаты_проверки, id_созданной_сессии)
        """
        logger.info(f"Проверяем позиции для {domain} с созданием сессии")
        
        # Находим проект
        project = self._find_project_by_domain(domain)
        if not project:
            logger.error(f"Проект с доменом {domain} не найден в конфигурации")
            return [], -1
        
        project_name = project.get('name', domain)
        project_id = self.db.get_or_create_project(project_name, domain)
        
        # Создаём сессию
        if not session_name:
            session_name = f"Проверка {domain} {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        session_id = self.db.create_monitoring_session(project_id, session_name)
        logger.info(f"Создана сессия: {session_id} - '{session_name}'")
        
        try:
            # Проверяем позиции с созданной сессией
            results = self.check_positions(
                domain=domain,
                keywords=keywords,
                session_id=session_id,
                search_engine=search_engine,
                use_cache=use_cache,
                track_competitors=track_competitors,
                competitors_limit=competitors_limit
            )
            
            # Завершаем сессию
            successful_checks = len([r for r in results if r.get('position') is not None])
            self.db.complete_monitoring_session(
                session_id,
                total_keywords=len(keywords),
                completed_keywords=successful_checks
            )
            
            logger.info(f"Сессия {session_id} завершена. Успешно: {successful_checks}/{len(keywords)}")
            
            return results, session_id
            
        except Exception as e:
            # Если ошибка - помечаем сессию как неудачную
            logger.error(f"Ошибка при проверке: {e}")
            self.db.fail_monitoring_session(session_id, str(e))
            return [], session_id
    
    def _get_session_info(self, session_id: int, project_id: int) -> Optional[Dict]:
        """
        Получает информацию о сессии и проверяет, что она принадлежит проекту
        """
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM monitoring_sessions 
                    WHERE session_id = ? AND project_id = ?
                """, (session_id, project_id))
                
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения информации о сессии {session_id}: {e}")
            return None
    
    def _find_project_by_domain(self, domain: str) -> Optional[Dict]:
        """Находит проект по домену в конфигурации"""
        for project in self.config.get('projects', []):
            project_domain = project.get('domain', '')
            # Убираем протокол для сравнения
            clean_domain = domain.replace('http://', '').replace('https://', '').strip('/')
            clean_project_domain = project_domain.replace('http://', '').replace('https://', '').strip('/')
            
            if clean_domain == clean_project_domain:
                return project
        return None
    
    def _get_position_from_search(self, 
                                domain: str, 
                                keyword: str,
                                search_engine: str,
                                include_competitors: bool = False,
                                competitors_limit: int = 20) -> Dict[str, Any]:
        """
        Реальная логика получения позиции из поисковой выдачи
        """
        logger.debug(f"Получение позиции для '{keyword}' в {search_engine}")
        
        # Проверяем поисковую систему
        if search_engine.lower() == "yandex":
            # Используем реальный парсер
            return self._get_real_yandex_position(
                domain, keyword, include_competitors, competitors_limit
            )
        elif search_engine.lower() == "google":
            # TODO: Добавить Google парсер позже
            logger.warning("Google парсинг пока не реализован, используем заглушку")
            return self._get_stub_position(domain, keyword, search_engine)
        else:
            logger.error(f"Неподдерживаемая поисковая система: {search_engine}")
            return self._get_stub_position(domain, keyword, search_engine)
    
    def _get_real_yandex_position(self, 
                                domain: str, 
                                keyword: str,
                                include_competitors: bool = False,
                                competitors_limit: int = 20) -> Dict[str, Any]:
        """
        Получает реальную позицию из Яндекс через xmlstock.com
        """
        try:
            # Импортируем здесь, чтобы не загружать модуль если не используется
            from core.real_position_parser import RealPositionParser
            
            # Создаём парсер при первом использовании
            if not hasattr(self, '_real_parser'):
                self._real_parser = RealPositionParser()
            
            # Получаем позицию с конкурентами
            result = self._real_parser.get_yandex_position(
                domain=domain,
                keyword=keyword,
                include_competitors=include_competitors,
                competitors_limit=competitors_limit
            )
            
            # ИСПРАВЛЕНИЕ: Используем конкурентов из результата парсера
            competitors = []
            if include_competitors and result.get("competitors"):
                competitors = result.get("competitors", [])
                
                # Проверяем, что действительно собрались конкуренты
                if competitors:
                    logger.debug(f"Собрано {len(competitors)} конкурентов для '{keyword}'")
                else:
                    logger.debug(f"Конкуренты не собрались для '{keyword}', проверьте API или парсинг")
            
            # Форматируем результат для совместимости
            return {
                "position": result.get("position"),
                "url": result.get("url", ""),
                "title": result.get("title", ""),
                "search_engine": "yandex",
                "method": "xmlstock_api",
                "found": result.get("found", False),
                "total_results": result.get("total_results", 0),
                "competitors": competitors,
                "error": result.get("error")
            }
            
        except Exception as e:
            logger.error(f"Ошибка реального парсера для '{keyword}': {e}")
            # Возвращаем заглушку в случае ошибки
            return self._get_stub_position(domain, keyword, "yandex")
    
    def _get_stub_position(self, domain: str, keyword: str, search_engine: str) -> Dict[str, Any]:
        """
        Заглушка для неподдерживаемых поисковых систем
        """
        import random
        
        # Для обратной совместимости с тестами
        if "ардуино" in keyword.lower():
            position = random.randint(1, 10)
        elif "датчик" in keyword.lower():
            position = random.randint(5, 20)
        else:
            position = random.randint(10, 30)
        
        url = f"https://{domain}/search?q={keyword.replace(' ', '+')}"
        
        return {
            "position": position,
            "url": url,
            "search_engine": search_engine,
            "method": "stub",
            "found": position <= 100,
            "total_results": 100,
            "competitors": [],
            "error": None
        }
    
    def _save_to_cache(self, key: str, data: Dict):
        """Сохраняет данные в кеш (JSON файлы)"""
        cache_file = self.cache_dir / f"{hash(key) & 0xFFFFFFFF}.json"
        try:
            cache_data = {
                "key": key,
                "data": data,
                "timestamp": datetime.now().isoformat(),
                "ttl": 3600  # Time to live: 1 час
            }
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Не удалось сохранить в кеш: {e}")
    
    def _get_from_cache(self, key: str) -> Optional[Dict]:
        """Получает данные из кеша"""
        cache_file = self.cache_dir / f"{hash(key) & 0xFFFFFFFF}.json"
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Проверяем TTL (время жизни кеша)
            cache_time = datetime.fromisoformat(cache_data["timestamp"])
            current_time = datetime.now()
            ttl_seconds = cache_data.get("ttl", 3600)
            
            if (current_time - cache_time).total_seconds() > ttl_seconds:
                # Кеш устарел
                cache_file.unlink()  # Удаляем файл
                return None
            
            return cache_data["data"]
            
        except Exception as e:
            logger.warning(f"Ошибка чтения кеша: {e}")
            return None
    
    def get_traffic_data(self, 
                        metrica_id: Optional[str] = None,
                        days: int = 7) -> Dict[str, Any]:
        """
        Получает данные о трафике (заглушка)
        """
        logger.info(f"Получение данных о трафике. Дней: {days}")
        
        import random
        
        return {
            "visits": random.randint(800, 1200),
            "pageviews": random.randint(2000, 3000),
            "bounce_rate": round(random.uniform(30.0, 60.0), 1),
            "avg_session_duration": random.randint(120, 300),
            "new_users": random.randint(400, 600),
            "source": "stub",
            "date_range": f"last_{days}_days"
        }
    
    def get_webmaster_issues(self, host_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Получает информацию об ошибках (заглушка)
        """
        logger.info("Проверка Яндекс.Вебмастера")
        
        import random
        from datetime import datetime, timedelta
        
        return {
            "critical_errors": random.randint(0, 2),
            "warnings": random.randint(0, 5),
            "indexed_pages": random.randint(100, 500),
            "last_crawl": (datetime.now() - timedelta(days=random.randint(0, 3))).strftime("%Y-%m-%d"),
            "source": "stub"
        }