#!/usr/bin/env python3
"""
Парсер для xmlstock.com с поддержкой сбора данных о конкурентах
"""

import requests
import xml.etree.ElementTree as ET
import logging
import time
from urllib.parse import quote, urlparse
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class RealPositionParser:
    """
    Парсер для XMLStock API с поддержкой сбора конкурентов
    """
    
    def __init__(self, 
                 user: str = None, 
                 key: str = None,
                 config_path: str = "config/api_keys.yaml"):
        """
        Инициализация парсера
        """
        self.base_url = "https://xmlstock.com/yandex/xml/"
        
        # Получаем ключи
        if user and key:
            self.user = user
            self.key = key
        else:
            self.user, self.key = self._load_api_keys(config_path)
        
        # Настройки
        self.settings = {
            'timeout': 30,
            'max_retries': 3,
            'retry_delay': 5,
            'region': 157,  # Минск (по умолчанию)
            'max_results': 100  # Максимальное количество результатов для парсинга
        }
        
        # Кеш
        self.cache = {}
        
        logger.info(f"RealPositionParser инициализирован. User: {self.user}")
    
    def _load_api_keys(self, config_path: str) -> Tuple[str, str]:
        """Загружает API ключи"""
        try:
            # Сначала пробуем из .env
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            user = os.getenv('XMLSTOCK_USER', '8349')
            key = os.getenv('XMLSTOCK_KEY', '06ec5436d8dec99990036d862f29ea6d')
            
            return user, key
            
        except Exception as e:
            logger.error(f"Ошибка загрузки API ключей: {e}")
            return "8349", "06ec5436d8dec99990036d862f29ea6d"
    
    def get_search_results(self, 
                          keyword: str, 
                          region: int = None,
                          limit: int = 20) -> List[Dict]:
        """
        Получает топ-N результатов поиска по ключевому слову
        
        Args:
            keyword: Ключевое слово
            region: Регион поиска (по умолчанию из настроек)
            limit: Сколько результатов вернуть (макс 100)
            
        Returns:
            Список результатов поиска
        """
        region = region or self.settings['region']
        
        logger.info(f"Запрос выдачи для '{keyword}' (регион: {region}, лимит: {limit})")
        
        # Формируем запрос
        query_encoded = quote(keyword)
        url = (f"{self.base_url}?user={self.user}&key={self.key}"
               f"&query={query_encoded}&lr={region}")
        
        # Пытаемся с повторными попытками
        for attempt in range(self.settings['max_retries']):
            try:
                response = requests.get(url, timeout=self.settings['timeout'])
                
                if response.status_code != 200:
                    logger.warning(f"HTTP {response.status_code} для '{keyword}'")
                    if attempt < self.settings['max_retries'] - 1:
                        time.sleep(self.settings['retry_delay'])
                        continue
                    else:
                        return []
                
                # Парсим XML и извлекаем все результаты
                results = self._parse_all_search_results(response.text, limit)
                
                logger.info(f"Получено результатов для '{keyword}': {len(results)}")
                return results
                
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут для '{keyword}' (попытка {attempt + 1})")
                if attempt < self.settings['max_retries'] - 1:
                    time.sleep(self.settings['retry_delay'])
                    continue
                else:
                    return []
                    
            except Exception as e:
                logger.error(f"Ошибка для '{keyword}': {e}")
                return []
    
    def get_yandex_position(self, 
                           domain: str, 
                           keyword: str,
                           region: int = None,
                           include_competitors: bool = False,
                           competitors_limit: int = 20) -> Dict[str, any]:
        """
        Получает позицию домена в Яндекс + конкурентов
        
        Args:
            domain: Домен для поиска
            keyword: Ключевое слово
            region: Регион поиска
            include_competitors: Включать ли данные о конкурентах
            competitors_limit: Сколько конкурентов вернуть
            
        Returns:
            Словарь с результатами
        """
        region = region or self.settings['region']
        
        # Проверяем кеш
        cache_key = f"{domain}_{keyword}_{region}_{include_competitors}_{competitors_limit}"
        if cache_key in self.cache:
            logger.debug(f"Используем кеш для '{keyword}'")
            result = self.cache[cache_key].copy()
            result['cache_used'] = True
            return result
        
        logger.info(f"Запрос позиции для '{keyword}' (домен: {domain}, регион: {region})")
        
        # Получаем все результаты поиска
        all_results = self.get_search_results(keyword, region, limit=self.settings['max_results'])
        
        # Ищем наш домен
        our_position = None
        our_result = None
        competitors = []
        
        for result in all_results:
            result_domain = result.get('domain', '')
            
            # Проверяем совпадение домена
            if self._domains_match(result_domain, domain):
                our_position = result.get('position')
                our_result = result
                break
        
        # Формируем список конкурентов
        if include_competitors and competitors_limit > 0:
            # Берём топ-N результатов, исключая наш домен
            for result in all_results[:competitors_limit + 10]:  # Берём с запасом
                result_domain = result.get('domain', '')
                
                # Пропускаем наш домен
                if our_result and self._domains_match(result_domain, domain):
                    continue
                
                # Добавляем конкурента
                competitors.append({
                    'position': result.get('position'),
                    'domain': result_domain,
                    'url': result.get('url', ''),
                    'title': result.get('title', ''),
                    'snippet': result.get('snippet', '')
                })
                
                # Останавливаемся когда набрали нужное количество
                if len(competitors) >= competitors_limit:
                    break
        
        # Формируем результат
        result = {
            "position": our_position if our_position is not None else 101,
            "url": our_result.get('url', '') if our_result else '',
            "title": our_result.get('title', '') if our_result else '',
            "domain": domain,
            "found": our_position is not None and our_position <= 100,
            "total_results": len(all_results),
            "region": region,
            "keyword": keyword,
            "timestamp": time.time()
        }
        
        if include_competitors:
            result["competitors"] = competitors
            result["top_competitors"] = competitors[:10]  # Топ-10 конкурентов
        
        # Сохраняем в кеш
        result['cache_used'] = False
        self.cache[cache_key] = result.copy()
        
        if our_position:
            logger.info(f"Найдено: '{keyword}' - позиция {our_position}")
        else:
            logger.info(f"Не найдено: '{keyword}' - позиция >100")
        
        return result
    
    def batch_get_positions(self, 
                           domain: str, 
                           keywords: List[str],
                           region: int = None,
                           include_competitors: bool = False,
                           competitors_limit: int = 10) -> List[Dict]:
        """
        Пакетное получение позиций
        
        Args:
            domain: Домен для поиска
            keywords: Список ключевых слов
            region: Регион поиска
            include_competitors: Включать ли конкурентов
            competitors_limit: Лимит конкурентов на запрос
            
        Returns:
            Список результатов
        """
        results = []
        
        logger.info(f"Пакетный запрос для {domain}. Ключевых слов: {len(keywords)}")
        
        for i, keyword in enumerate(keywords):
            try:
                logger.debug(f"Запрос {i+1}/{len(keywords)}: '{keyword}'")
                
                result = self.get_yandex_position(
                    domain=domain,
                    keyword=keyword,
                    region=region,
                    include_competitors=include_competitors,
                    competitors_limit=competitors_limit
                )
                results.append(result)
                
                # Пауза между запросами
                if i < len(keywords) - 1:
                    time.sleep(1.5)  # 1.5 секунды между запросами
                    
            except Exception as e:
                logger.error(f"Ошибка для '{keyword}': {e}")
                results.append(self._create_error_result(str(e)))
                continue
        
        return results
    
    def _parse_all_search_results(self, xml_text: str, limit: int = 100) -> List[Dict]:
        """
        Парсит все результаты поиска из XML
        
        Args:
            xml_text: XML ответ от API
            limit: Максимальное количество результатов
            
        Returns:
            Список результатов
        """
        results = []
        
        try:
            root = ET.fromstring(xml_text)
            
            # Проверяем ошибки
            error_elem = root.find('.//error')
            if error_elem is not None:
                error_msg = error_elem.text or "Неизвестная ошибка API"
                logger.error(f"Ошибка API: {error_msg}")
                return results
            
            # Находим все группы (каждая группа = 1 результат в выдаче)
            groups = root.findall('.//group')
            
            for position, group in enumerate(groups, 1):
                if position > limit:
                    break
                
                # Извлекаем данные из группы
                result = self._extract_result_from_group(group, position)
                if result:
                    results.append(result)
            
            logger.debug(f"Извлечено результатов из XML: {len(results)}")
            
        except ET.ParseError as e:
            logger.error(f"Ошибка парсинга XML: {e}")
        
        return results
    
    def _extract_result_from_group(self, group: ET.Element, position: int) -> Optional[Dict]:
        """
        Извлекает данные одного результата из группы
        
        Args:
            group: Элемент group из XML
            position: Позиция в выдаче
            
        Returns:
            Словарь с данными результата или None
        """
        try:
            doc_elem = group.find('doc')
            if doc_elem is None:
                return None
            
            # Извлекаем URL
            url_elem = doc_elem.find('url')
            url = url_elem.text if url_elem is not None else ""
            
            # Извлекаем заголовок
            title_elem = doc_elem.find('title')
            title = title_elem.text if title_elem is not None else ""
            
            # Извлекаем домен
            domain_elem = doc_elem.find('domain')
            domain = domain_elem.text if domain_elem is not None else ""
            
            # Если домен пустой, пытаемся извлечь из URL
            if not domain and url:
                parsed_url = urlparse(url)
                domain = parsed_url.netloc
            
            # Извлекаем сниппет
            snippet_elem = doc_elem.find('snippet')
            snippet = snippet_elem.text if snippet_elem is not None else ""
            
            # Ищем в категории (дополнительный источник домена)
            categ_elem = group.find('categ')
            if categ_elem is not None:
                categ_domain = categ_elem.get('name', '')
                if categ_domain and not domain:
                    domain = categ_domain
            
            return {
                "position": position,
                "url": url,
                "title": title,
                "domain": domain,
                "snippet": snippet
            }
            
        except Exception as e:
            logger.debug(f"Ошибка извлечения данных из группы: {e}")
            return None
    
    def _domains_match(self, domain1: str, domain2: str) -> bool:
        """
        Проверяет совпадение доменов (с учётом www и без)
        
        Args:
            domain1: Первый домен
            domain2: Второй домен
            
        Returns:
            True если домены совпадают
        """
        if not domain1 or not domain2:
            return False
        
        # Очищаем домены
        d1 = domain1.lower().replace('www.', '').strip()
        d2 = domain2.lower().replace('www.', '').strip()
        
        # Простое сравнение
        if d1 == d2:
            return True
        
        # Проверяем вхождение одного в другой (для поддоменов)
        if d1 in d2 or d2 in d1:
            logger.debug(f"Частичное совпадение доменов: '{domain1}' и '{domain2}'")
            return True
        
        return False
    
    def _create_error_result(self, error_msg: str) -> Dict[str, any]:
        """Создаёт результат с ошибкой"""
        return {
            "position": None,
            "url": "",
            "title": "",
            "domain": "",
            "found": False,
            "total_results": 0,
            "region": self.settings['region'],
            "keyword": "",
            "error": error_msg,
            "timestamp": time.time()
        }


# ========== ТЕСТОВЫЙ КОД ==========
def test_parser_with_competitors():
    """
    Тестируем парсер с поддержкой конкурентов
    """
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 ТЕСТИРУЕМ PARSER С КОНКУРЕНТАМИ")
    print("=" * 50)
    
    try:
        # 1. Создаём парсер
        parser = RealPositionParser()
        print("✅ Парсер создан")
        
        # 2. Тестируем получение выдачи
        print("\n1. Тестируем получение выдачи...")
        keyword = "вендинговые аппараты купить"
        
        results = parser.get_search_results(keyword, limit=10)
        print(f"   Результатов для '{keyword}': {len(results)}")
        
        if results:
            print(f"   Топ-5 результатов:")
            for i, result in enumerate(results[:5], 1):
                print(f"   {i}. [{result.get('position')}] {result.get('domain')}")
                title = result.get('title', '')
                if title:
                    print(f"      {title[:50]}...")
                else:
                    print(f"      (без заголовка)")
        
        # 3. Тестируем поиск позиции с конкурентами
        print("\n2. Тестируем поиск позиции с конкурентами...")
        
        result = parser.get_yandex_position(
            domain="aquamoney.by",
            keyword=keyword,
            include_competitors=True,
            competitors_limit=10
        )
        
        print(f"   Наша позиция: {result.get('position')}")
        print(f"   Найдено: {result.get('found')}")
        
        competitors = result.get('competitors', [])
        print(f"   Конкурентов собрано: {len(competitors)}")
        
        if competitors:
            print(f"\n3. Топ-5 конкурентов:")
            for i, comp in enumerate(competitors[:5], 1):
                print(f"   {i}. [{comp.get('position')}] {comp.get('domain')}")
                if comp.get('title'):
                    title = comp.get('title', '')
                    if title:
                        print(f"      {title[:40]}...")
        
        # 4. Тестируем пакетный запрос
        print("\n4. Тестируем пакетный запрос...")
        
        keywords = ["водомат", "вендинговые аппараты купить"]
        batch_results = parser.batch_get_positions(
            domain="aquamoney.by",
            keywords=keywords,
            include_competitors=True,
            competitors_limit=5
        )
        
        print(f"   Обработано ключевых слов: {len(batch_results)}")
        for i, res in enumerate(batch_results):
            kw = keywords[i]
            pos = res.get('position', 'N/A')
            comp_count = len(res.get('competitors', []))
            print(f"   • '{kw}': позиция {pos}, конкурентов: {comp_count}")
        
        print("\n" + "=" * 50)
        print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ТЕСТИРОВАНИИ: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_parser_with_competitors()
