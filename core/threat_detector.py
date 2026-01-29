#!/usr/bin/env python3
"""
Модуль обнаружения угроз для SEO-агента (SQLite версия)
"""

import sys
from pathlib import Path
import logging

# Добавляем путь для импорта модулей core
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

try:
    from core.database import SEODatabase
except ImportError:
    from database import SEODatabase

logger = logging.getLogger(__name__)

class ThreatDetector:
    """
    Детектор угроз для SEO-проектов (SQLite версия)
    """
    
    def __init__(self):
        """
        Инициализация детектора угроз
        """
        self.db = SEODatabase()
        self.thresholds = {
            "critical_drop": 10,     # Падение на 10+ позиций = критично
            "significant_drop": 3,    # Падение на 3+ позиций = важно
            "days_to_analyze": 7,     # Анализировать данные за 7 дней
            "min_check_frequency": 2  # Минимум 2 проверки для анализа
        }
        
        logger.info("ThreatDetector (SQLite) инициализирован")
    
    def analyze_project(self, project_name: str, domain: str) -> dict:
        """
        Полный анализ проекта на угрозы
        
        Args:
            project_name: Название проекта
            domain: Домен сайта
            
        Returns:
            Словарь с результатами анализа
        """
        logger.info(f"Анализируем проект: {project_name} ({domain})")
        
        results = {
            "project_name": project_name,
            "domain": domain,
            "analysis_date": self._current_datetime(),
            "threats": [],
            "warnings": [],
            "recommendations": [],
            "overall_status": "stable",
            "trend": "neutral",
            "metrics": {}
        }
        
        try:
            # 1. Анализируем позиции
            position_threats = self.analyze_position_changes(domain)
            if position_threats:
                results["threats"].extend(position_threats)
            
            # 2. Ищем вытеснения
            displacements = self.detect_displacements(domain)
            if displacements:
                results["threats"].extend(displacements)
            
            # 3. Оцениваем общую ситуацию
            overall_assessment = self.assess_overall_situation(domain)
            results.update(overall_assessment)
            
            # 4. Генерируем рекомендации
            if results["threats"]:
                recommendations = self.generate_recommendations(results["threats"])
                results["recommendations"] = recommendations
            
            logger.info(f"Анализ завершён. Найдено угроз: {len(results['threats'])}")
            
        except Exception as e:
            logger.error(f"Ошибка анализа проекта {project_name}: {e}")
            results["error"] = str(e)
        
        # Сохраняем результаты анализа
        self._save_analysis_results(project_name, results)
        
        return results
    
    def analyze_position_changes(self, domain: str) -> list:
        """
        Анализирует изменения позиций за период
        
        Args:
            domain: Домен сайта
            
        Returns:
            Список угроз связанных с изменениями позиций
        """
        threats = []
        
        try:
            # Загружаем исторические данные из базы
            history = self.db.get_position_history(domain, self.thresholds['days_to_analyze'])
            
            if not history:
                logger.warning(f"Нет данных для анализа позиций {domain}")
                return threats
            
            # Группируем по ключевым словам
            from collections import defaultdict
            keyword_data = defaultdict(list)
            
            for record in history:
                keyword = record['keyword']
                keyword_data[keyword].append(record)
            
            # Анализируем каждое ключевое слово
            for keyword, records in keyword_data.items():
                # Сортируем по дате и времени (последние записи в конце)
                sorted_records = sorted(
                    records,
                    key=lambda x: (x['check_date'], x['check_time'])
                )
                
                if len(sorted_records) >= self.thresholds['min_check_frequency']:
                    # Берем последние N записей
                    recent_data = sorted_records[-self.thresholds['min_check_frequency']:]
                    
                    # Сравниваем последнюю и предпоследнюю позицию
                    latest = recent_data[-1]
                    previous = recent_data[-2]
                    
                    latest_pos = latest['position']
                    previous_pos = previous['position']
                    
                    # Пропускаем если позиции нет
                    if latest_pos is None or previous_pos is None:
                        continue
                    
                    # Вычисляем изменение
                    change = latest_pos - previous_pos
                    
                    # Определяем уровень угрозы
                    if change >= self.thresholds['critical_drop']:
                        threat_level = "critical"
                    elif change >= self.thresholds['significant_drop']:
                        threat_level = "warning"
                    else:
                        continue  # Незначительное изменение, пропускаем
                    
                    # Формируем угрозу
                    threat = {
                        "type": "position_drop",
                        "keyword": keyword,
                        "previous_position": int(previous_pos),
                        "current_position": int(latest_pos),
                        "change": int(change),
                        "threat_level": threat_level,
                        "detected_at": self._current_datetime(),
                        "timeframe_hours": self._hours_between_dates(
                            f"{previous['check_date']} {previous['check_time']}",
                            f"{latest['check_date']} {latest['check_time']}"
                        )
                    }
                    
                    threats.append(threat)
                    
                    logger.info(
                        f"Обнаружено падение: '{keyword}' "
                        f"{previous_pos} → {latest_pos} "
                        f"(изменение: {change}, уровень: {threat_level})"
                    )
        
        except Exception as e:
            logger.error(f"Ошибка анализа позиций для {domain}: {e}")
        
        return threats
    
    def detect_displacements(self, domain: str) -> list:
        """
        Обнаруживает вытеснение конкурентами
        
        Args:
            domain: Домен сайта
            
        Returns:
            Список обнаруженных вытеснений
        """
        displacements = []
        
        try:
            # Загружаем исторические данные
            history = self.db.get_position_history(domain, 30)  # За 30 дней
            
            if len(history) < 2:
                return displacements
            
            # Группируем по ключевым словам
            from collections import defaultdict
            keyword_data = defaultdict(list)
            
            for record in history:
                keyword = record['keyword']
                keyword_data[keyword].append(record)
            
            # Ищем ключевые слова, где мы были в топ-20, а теперь нет
            for keyword, records in keyword_data.items():
                if len(records) < 2:
                    continue
                
                # Сортируем по дате
                sorted_records = sorted(
                    records,
                    key=lambda x: (x['check_date'], x['check_time'])
                )
                
                # Берем самую старую и самую новую позицию
                oldest = sorted_records[0]
                latest = sorted_records[-1]
                
                oldest_pos = oldest['position']
                latest_pos = latest['position']
                
                # Пропускаем если нет данных
                if oldest_pos is None or latest_pos is None:
                    continue
                
                # Проверяем вытеснение из топ-20
                if oldest_pos <= 20 and latest_pos > 20:
                    displacement = {
                        "type": "displacement",
                        "keyword": keyword,
                        "old_position": int(oldest_pos),
                        "new_position": int(latest_pos),
                        "dropped_from_top20": True,
                        "positions_lost": int(latest_pos - oldest_pos),
                        "time_period_days": self._days_between_dates(
                            oldest['check_date'],
                            latest['check_date']
                        ),
                        "threat_level": "critical" if latest_pos > 50 else "warning"
                    }
                    
                    displacements.append(displacement)
                    
                    logger.warning(
                        f"Вытеснение из топ-20: '{keyword}' "
                        f"{oldest_pos} → {latest_pos}"
                    )
        
        except Exception as e:
            logger.error(f"Ошибка обнаружения вытеснений для {domain}: {e}")
        
        return displacements
    
    def assess_overall_situation(self, domain: str) -> dict:
        """
        Оценивает общую ситуацию по проекту
        
        Args:
            domain: Домен сайта
            
        Returns:
            Словарь с общей оценкой
        """
        assessment = {
            "overall_status": "stable",
            "trend": "neutral",
            "metrics": {}
        }
        
        try:
            history = self.db.get_position_history(domain, 7)
            
            if not history or len(history) < 3:
                return assessment
            
            # Группируем по ключевым словам
            from collections import defaultdict
            keyword_data = defaultdict(list)
            
            for record in history:
                keyword = record['keyword']
                keyword_data[keyword].append(record)
            
            keyword_trends = []
            
            for keyword, records in keyword_data.items():
                if len(records) >= 3:
                    # Сортируем по дате
                    sorted_records = sorted(
                        records,
                        key=lambda x: (x['check_date'], x['check_time'])
                    )
                    
                    # Берем три последние позиции
                    recent = sorted_records[-3:]
                    positions = [r['position'] for r in recent if r['position'] is not None]
                    
                    if len(positions) == 3:
                        # Определяем тренд
                        if positions[0] > positions[1] > positions[2]:  # Улучшение
                            trend = "improving"
                        elif positions[0] < positions[1] < positions[2]:  # Ухудшение
                            trend = "declining"
                        else:
                            trend = "fluctuating"
                        
                        keyword_trends.append({
                            "keyword": keyword,
                            "trend": trend,
                            "current_position": int(positions[-1]),
                            "change_3_checks": int(positions[-1] - positions[0])
                        })
            
            # Оцениваем общую ситуацию
            improving = len([t for t in keyword_trends if t['trend'] == 'improving'])
            declining = len([t for t in keyword_trends if t['trend'] == 'declining'])
            total = len(keyword_trends)
            
            if total > 0:
                improvement_ratio = improving / total
                decline_ratio = declining / total
                
                if decline_ratio > 0.5:  # Более 50% ключей ухудшаются
                    assessment["overall_status"] = "critical"
                    assessment["trend"] = "negative"
                elif decline_ratio > 0.3:  # Более 30% ухудшаются
                    assessment["overall_status"] = "warning"
                    assessment["trend"] = "slightly_negative"
                elif improvement_ratio > 0.5:  # Более 50% улучшаются
                    assessment["overall_status"] = "good"
                    assessment["trend"] = "positive"
                
                assessment["metrics"] = {
                    "total_keywords_tracked": total,
                    "improving": improving,
                    "declining": declining,
                    "stable": total - improving - declining,
                    "improvement_ratio": round(improvement_ratio, 2),
                    "decline_ratio": round(decline_ratio, 2)
                }
        
        except Exception as e:
            logger.error(f"Ошибка оценки общей ситуации для {domain}: {e}")
        
        return assessment
    
    def generate_recommendations(self, threats: list) -> list:
        """
        Генерирует рекомендации на основе обнаруженных угроз
        
        Args:
            threats: Список угроз
            
        Returns:
            Список рекомендаций
        """
        recommendations = []
        
        # Группируем угрозы по типу
        position_drops = [t for t in threats if t.get('type') == 'position_drop']
        displacements = [t for t in threats if t.get('type') == 'displacement']
        
        # Рекомендации по падениям позиций
        if position_drops:
            critical_drops = [t for t in position_drops if t.get('threat_level') == 'critical']
            warning_drops = [t for t in position_drops if t.get('threat_level') == 'warning']
            
            if critical_drops:
                keywords = [t['keyword'] for t in critical_drops[:3]]
                rec = f"Критическое падение по запросам: {', '.join(keywords)}. "
                rec += "Требуется немедленный анализ конкурентов."
                recommendations.append(rec)
            
            if warning_drops:
                keywords = [t['keyword'] for t in warning_drops[:5]]
                rec = f"Значительное падение по запросам: {', '.join(keywords)}. "
                rec += "Рекомендуется анализ страниц-конкурентов."
                recommendations.append(rec)
        
        # Рекомендации по вытеснениям
        if displacements:
            top20_displacements = [t for t in displacements if t.get('dropped_from_top20')]
            if top20_displacements:
                keywords = [t['keyword'] for t in top20_displacements[:3]]
                rec = f"Вытеснение из топ-20 по запросам: {', '.join(keywords)}. "
                rec += "Необходимо срочное улучшение контента."
                recommendations.append(rec)
        
        # Общие рекомендации
        if threats and not recommendations:
            recommendations.append(
                "Обнаружены изменения в позициях. "
                "Рекомендуется провести анализ конкурентов."
            )
        
        # Если угроз нет
        if not threats:
            recommendations.append(
                "Значительных угроз не обнаружено. "
                "Продолжайте мониторинг."
            )
        
        return recommendations
    
    def _current_datetime(self) -> str:
        """Возвращает текущую дату-время в ISO формате"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def _hours_between_dates(self, date1_str: str, date2_str: str) -> float:
        """Вычисляет разницу в часах между двумя датами"""
        from datetime import datetime
        
        try:
            dt1 = datetime.fromisoformat(date1_str.replace('Z', '+00:00'))
            dt2 = datetime.fromisoformat(date2_str.replace('Z', '+00:00'))
            delta = abs(dt2 - dt1)
            return delta.total_seconds() / 3600
        except:
            return 0.0
    
    def _days_between_dates(self, date1_str: str, date2_str: str) -> int:
        """Вычисляет разницу в днях между двумя датами"""
        from datetime import datetime
        
        try:
            dt1 = datetime.fromisoformat(date1_str)
            dt2 = datetime.fromisoformat(date2_str)
            delta = abs(dt2 - dt1)
            return delta.days
        except:
            return 0
    
    def _save_analysis_results(self, project_name: str, results: dict):
        """
        Сохраняет результаты анализа в JSON файл
        
        Args:
            project_name: Название проекта
            results: Результаты анализа
        """
        try:
            import json
            from datetime import datetime
            
            reports_dir = Path("data/reports")
            reports_dir.mkdir(exist_ok=True, parents=True)
            
            # Создаём безопасное имя файла
            safe_name = project_name.lower().replace(' ', '_').replace('/', '_')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = reports_dir / f"threats_{safe_name}_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Результаты анализа сохранены: {filename}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения результатов анализа: {e}")


# ========== ТЕСТОВЫЙ КОД ==========
def test_threat_detector_sqlite():
    """
    Тестируем ThreatDetector с SQLite
    """
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 ТЕСТИРУЕМ THREAT_DETECTOR (SQLite)")
    print("=" * 50)
    
    try:
        # 1. Создаём детектор
        detector = ThreatDetector()
        print("✅ ThreatDetector создан")
        
        # 2. Анализируем проект
        print("\n1. Анализируем проект Аквамани...")
        
        analysis = detector.analyze_project(
            project_name="Аквамани",
            domain="aquamoney.by"
        )
        
        # 3. Выводим результаты
        print(f"\n2. Результаты анализа:")
        print(f"   Проект: {analysis.get('project_name')}")
        print(f"   Домен: {analysis.get('domain')}")
        print(f"   Статус: {analysis.get('overall_status')}")
        print(f"   Тренд: {analysis.get('trend')}")
        print(f"   Найдено угроз: {len(analysis.get('threats', []))}")
        
        threats = analysis.get('threats', [])
        if threats:
            print(f"\n3. Обнаруженные угрозы:")
            for i, threat in enumerate(threats, 1):
                print(f"   {i}. {threat.get('type', 'unknown').upper()} - '{threat.get('keyword')}'")
                print(f"      Было: {threat.get('old_position', threat.get('previous_position'))}")
                print(f"      Стало: {threat.get('new_position', threat.get('current_position'))}")
                print(f"      Уровень: {threat.get('threat_level')}")
        
        recommendations = analysis.get('recommendations', [])
        if recommendations:
            print(f"\n4. Рекомендации:")
            for i, rec in enumerate(recommendations, 1):
                print(f"   {i}. {rec}")
        
        # 4. Проверяем метрики
        metrics = analysis.get('metrics', {})
        if metrics:
            print(f"\n5. Метрики:")
            print(f"   Ключевых слов отслеживается: {metrics.get('total_keywords_tracked')}")
            print(f"   Улучшаются: {metrics.get('improving')}")
            print(f"   Ухудшаются: {metrics.get('declining')}")
        
        print("\n" + "=" * 50)
        print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ТЕСТИРОВАНИИ: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_threat_detector_sqlite()
