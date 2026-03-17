#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт оперативной статистики релизов.
Читает таблицу релизов со страницы Confluence, группирует по неделям (суббота–пятница),
обогащает данными из JIRA и публикует отчёт на целевую страницу Confluence.
"""

import os
import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

import requests
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Статусы строк таблицы, которые попадают в отчёт (точное совпадение после нормализации)
RELEASE_STATUSES = frozenset([
    'Установлен на ПРОМ',
    'Готов',
    'Установка на ПРОМ',
])
# Варианты статусов для мягкого совпадения (если строка содержит подстроку — считаем подходящей)
RELEASE_STATUS_SUBSTRINGS = [
    'Установлен на ПРОМ',
    'Готов',
    'Установка на ПРОМ',
    'ПРОМ',
]


def normalize_status(status: str) -> str:
    """Убирает лишние пробелы в статусе."""
    return ' '.join(status.split()) if status else ""


class ConfluenceReleaseStats:
    """Формирование отчёта по релизам из Confluence с группировкой по неделям."""

    def __init__(self) -> None:
        self.config = {
            'confluence': {
                'url': os.getenv('CONFLUENCE_URL', 'https://confluence.sberbank.ru'),
                'token': os.getenv('CONFLUENCE_TOKEN'),
                'space': os.getenv('CONFLUENCE_SPACE', 'HRTECH'),
                'source_page_id': os.getenv('SOURCE_PAGE_ID', '18588013525'),
                'target_page_id': os.getenv('TARGET_PAGE_ID', '20906115139'),
                'verify_ssl': False,
            },
            'jira': {
                'url': os.getenv('JIRA_URL', 'https://jira.sberbank.ru'),
                'token': os.getenv('JIRA_TOKEN'),
                'verify_ssl': False,
            },
        }
        self.headers = {
            'Authorization': f'Bearer {self.config["confluence"]["token"]}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        jira_token = self.config['jira']['token']
        self.jira_headers = (
            {
                'Authorization': f'Bearer {jira_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
            if jira_token else None
        )
        if os.getenv('DEBUG_MODE', 'false').lower() == 'true':
            logging.getLogger().setLevel(logging.DEBUG)

    # --- Статистика и форматирование ---

    def count_legend_statistics(self, releases: List[Dict]) -> Dict[str, int]:
        planned = sum(1 for r in releases if r.get('type') == 'Плановый релиз')
        hotfix = sum(1 for r in releases if r.get('type') == 'Hotfix')
        ready = sum(1 for r in releases if r.get('status') == 'Готов')
        return {'Плановый релиз': planned, 'Hotfix': hotfix, 'Готов': ready}

    def format_legend_statistics(self, legend_stats: Dict[str, int]) -> str:
        p = legend_stats.get('Плановый релиз', 0)
        h = legend_stats.get('Hotfix', 0)
        g = legend_stats.get('Готов', 0)
        return f"""<ul>
<li>🟢 Плановый релиз: <strong>{p}</strong></li>
<li>⬆️ Hotfix: <strong>{h}</strong></li>
<li>☁️ Статус "Готов": <strong>{g}</strong></li>
</ul>"""

    def get_status_icon(self, release_type: str, status: str) -> str:
        if status == "Готов":
            return "☁️"
        return {"Плановый релиз": "🟢", "Hotfix": "⬆️"}.get(release_type, "🔸")

    # --- JIRA ---

    def get_jira_release_details(self, release_key: str) -> Dict:
        if not self.jira_headers:
            logger.debug("JIRA_TOKEN не задан — детали из JIRA не загружаются")
            return {}
        try:
            logger.info(f"🔍 Запрашиваем детали {release_key} из JIRA...")
            url = f"{self.config['jira']['url']}/rest/api/2/issue/{release_key}"
            resp = requests.get(
                url,
                headers=self.jira_headers,
                params={'fields': 'summary,description,customfield_10000'},
                verify=self.config['jira']['verify_ssl'],
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning(f"⚠️ JIRA {release_key}: {resp.status_code}")
                return {}
            data = resp.json()
            summary = data.get('fields', {}).get('summary', '') or ''
            description = data.get('fields', {}).get('description', '') or ''
            services = self._extract_services_from_jira(summary, description)
            logger.info(f"✅ Получены детали для {release_key}")
            return {
                'summary': summary,
                'description': description,
                'services': services,
                'full_info': f"{summary} {services}".strip() if services else summary,
            }
        except Exception as e:
            logger.error(f"❌ Ошибка JIRA {release_key}: {e}")
            return {}

    def _extract_services_from_jira(self, summary: str, description: str) -> str:
        text = f"{summary} {description}".strip()
        patterns = [
            r'HRP\.[\w\.-]+\s*\(\d+\)',
            r'CoreUI\s*\(\d+\)',
            r'HRP\.[\w\.-]+\(\d+\)',
            r'[\w\.-]+\s*\(\d{7}\)',
            r'[\w\.-]+\(\d{7}\)',
        ]
        seen = set()
        unique = []
        for pat in patterns:
            for m in re.findall(pat, text, re.IGNORECASE):
                if m not in seen:
                    seen.add(m)
                    unique.append(m)
        return ', '.join(unique) if unique else ""

    # --- Confluence API ---

    def test_api_connection(self) -> bool:
        url = f"{self.config['confluence']['url']}/rest/api/content"
        try:
            logger.info("🔍 Тестируем подключение к Confluence API...")
            resp = requests.get(
                url, headers=self.headers, params={'limit': 1},
                verify=self.config['confluence']['verify_ssl'], timeout=10,
            )
            logger.info(f"🔍 Тест API: статус {resp.status_code}")
            if resp.status_code == 200:
                logger.info("✅ Подключение к Confluence API успешно")
                return True
            if resp.status_code == 401:
                logger.error("❌ Ошибка авторизации — проверьте CONFLUENCE_TOKEN")
            elif resp.status_code == 403:
                logger.error("❌ Недостаточно прав доступа")
            else:
                logger.error(f"❌ API недоступно: {resp.status_code}")
            return False
        except requests.exceptions.Timeout:
            logger.error("❌ Таймаут подключения к API")
            return False
        except requests.exceptions.ConnectionError:
            logger.error("❌ Ошибка подключения к Confluence")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования API: {e}")
            return False

    def get_confluence_page_content(self, page_id: str) -> Tuple[Optional[str], Optional[int]]:
        url = f"{self.config['confluence']['url']}/rest/api/content/{page_id}"
        params = {'expand': 'body.storage,version'}
        try:
            logger.info(f"📡 Запрашиваем страницу {page_id}...")
            resp = requests.get(
                url, headers=self.headers, params=params,
                verify=self.config['confluence']['verify_ssl'], timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"❌ HTTP {resp.status_code} для страницы {page_id}")
                return None, None
            try:
                data = resp.json()
            except ValueError as e:
                logger.error(f"❌ Ответ не JSON: {e}")
                return None, None
            if 'body' not in data:
                logger.error("❌ В ответе нет body")
                return None, None
            body = data['body']
            content = None
            if body.get('storage') and 'value' in body.get('storage', {}):
                content = body['storage']['value']
            elif body.get('view') and 'value' in body.get('view', {}):
                logger.info(f"📋 Используется body.view. Поля body: {list(body.keys())}")
                content = body['view']['value']
            if content is None:
                logger.error(f"❌ Нет body.storage.value. Поля body: {list(body.keys())}")
                return None, None
            if not content or not content.strip():
                logger.error("❌ Пустое содержимое страницы")
                return None, None
            version = data.get('version', {}).get('number')
            if version is None:
                logger.error("❌ Нет версии страницы")
                return None, None
            logger.info(f"✅ Получена страница {page_id}, версия {version}, размер {len(content)}")
            return content, version
        except requests.exceptions.Timeout:
            logger.error("❌ Таймаут при получении страницы")
            return None, None
        except requests.exceptions.ConnectionError:
            logger.error("❌ Ошибка подключения")
            return None, None
        except Exception as e:
            logger.error(f"❌ Ошибка получения страницы: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None

    def update_confluence_page(self, page_id: str, new_content: str, current_version: int) -> bool:
        url = f"{self.config['confluence']['url']}/rest/api/content/{page_id}"
        try:
            logger.info(f"📝 Обновляем страницу {page_id}, версия {current_version} → {current_version + 1}")
            resp = requests.get(url, headers=self.headers, verify=self.config['confluence']['verify_ssl'])
            if resp.status_code != 200:
                logger.error(f"❌ Не удалось получить страницу: {resp.status_code}")
                return False
            title = resp.json().get('title', '')
            put_resp = requests.put(
                url,
                headers=self.headers,
                json={
                    "version": {"number": current_version + 1},
                    "title": title,
                    "type": "page",
                    "body": {"storage": {"value": new_content, "representation": "storage"}},
                },
                verify=self.config['confluence']['verify_ssl'],
            )
            if put_resp.status_code == 200:
                page_url = f"{self.config['confluence']['url']}/pages/viewpage.action?pageId={page_id}"
                logger.info(f"✅ Страница обновлена: {page_url}")
                print(f"CONFLUENCE_PAGE_URL={page_url}")
                return True
            logger.error(f"❌ Ошибка обновления: {put_resp.status_code}")
            logger.error(put_resp.text[:1000])
            return False
        except Exception as e:
            logger.error(f"❌ Исключение при обновлении: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    # --- Периоды (недели суббота–пятница) ---

    def get_week_periods_from_may(self) -> List[Dict]:
        today = datetime.now()
        start_date = datetime(2025, 5, 1)
        days_to_friday = (4 - start_date.weekday()) % 7
        if days_to_friday == 0 and start_date.weekday() != 4:
            days_to_friday = 7
        first_friday = start_date + timedelta(days=days_to_friday)
        days_since_friday = (today.weekday() - 4) % 7
        if days_since_friday == 0 and today.hour >= 18:
            days_since_friday = 7
        elif days_since_friday == 0:
            days_since_friday = 0
        else:
            days_since_friday = days_since_friday if days_since_friday > 0 else 7
        last_friday = today - timedelta(days=days_since_friday)
        logger.info(f"📅 Периоды: первая пятница {first_friday.strftime('%d.%m.%Y')}, последняя {last_friday.strftime('%d.%m.%Y')}")
        periods = []
        cur = first_friday
        while cur <= last_friday:
            start_period = cur - timedelta(days=6)
            periods.append({
                'start': start_period,
                'end': cur,
                'label': f"{start_period.strftime('%d.%m.%Y')}-{cur.strftime('%d.%m.%Y')}",
            })
            cur += timedelta(weeks=1)
        periods.reverse()
        logger.info(f"📊 Создано периодов: {len(periods)}")
        return periods

    def parse_date_from_text(self, text: str) -> Optional[datetime]:
        for fmt, pat in [('%Y-%m-%d', r'(\d{4}-\d{2}-\d{2})'), ('%d.%m.%Y', r'(\d{2}\.\d{2}\.\d{4})')]:
            m = re.search(pat, text)
            if m:
                try:
                    return datetime.strptime(m.group(1), fmt)
                except ValueError:
                    pass
        return None

    def get_week_for_date(self, release_date: datetime, periods: List[Dict]) -> str:
        for p in periods:
            if p['start'] <= release_date <= p['end']:
                return p['label']
        return "Другое время"

    # --- Парсинг таблицы (по содержимому строки, без привязки к номерам колонок) ---

    def _find_release_id_and_link_in_cell(self, cell) -> Tuple[str, str]:
        """Ищет HRPRELEASE-XXX и ссылку в одной ячейке."""
        rid, link = "", ""
        if not cell:
            return rid, link
        for a in cell.find_all('a', href=True):
            href = a.get('href') or ""
            m = re.search(r'HRPRELEASE-(\d+)', href, re.IGNORECASE)
            if m:
                rid = m.group(1)
                link = href
                return rid, link
        text = cell.get_text(strip=True) or ""
        m = re.search(r'HRPRELEASE-(\d+)', text, re.IGNORECASE)
        if m:
            rid = m.group(1)
        return rid, link

    def _find_release_id_in_row(self, cells) -> Tuple[str, str, str]:
        """Ищет release_id и ссылку в любой ячейке строки. Возвращает (release_id, link, full_text_ячейки_с_релизом)."""
        full_text = ""
        for cell in cells:
            rid, link = self._find_release_id_and_link_in_cell(cell)
            if rid:
                full_text = (cell.get_text(strip=True) or "")
                return rid, link, full_text
        return "", "", full_text

    def _find_date_in_row(self, cells) -> Tuple[Optional[str], Optional[datetime]]:
        """Ищет дату в любой ячейке строки. Возвращает (raw_text, datetime)."""
        for cell in cells:
            text = cell.get_text(strip=True) or ""
            dt = self.parse_date_from_text(text)
            if dt:
                return text, dt
        return None, None

    def _find_status_in_row(self, cells) -> Optional[str]:
        """Ищет статус в любой ячейке: сначала точное совпадение, потом по подстроке."""
        skip_headers = frozenset(['статус', 'status', 'дата', 'тип', 'ответственный', 'релиз'])
        for cell in cells:
            text = normalize_status(cell.get_text(strip=True) or "")
            if text in RELEASE_STATUSES:
                return text
        for cell in cells:
            text = (cell.get_text(strip=True) or "").strip()
            if text.lower() in skip_headers or len(text) < 2:
                continue
            for sub in RELEASE_STATUS_SUBSTRINGS:
                if sub in text:
                    if 'Готов' in text:
                        return 'Готов'
                    if 'Установка на ПРОМ' in text:
                        return 'Установка на ПРОМ'
                    if 'Установлен на ПРОМ' in text or ('ПРОМ' in text and 'Установ' in text):
                        return 'Установлен на ПРОМ'
                    if 'ПРОМ' in text:
                        return 'Установлен на ПРОМ'
                    norm = normalize_status(text)
                    return norm if norm in RELEASE_STATUSES else text
        return None

    def _find_type_and_responsible_in_row(self, cells) -> Tuple[str, str]:
        """Определяет тип релиза и ответственного по подстрокам и позиции."""
        release_type = ""
        for cell in cells:
            text = (cell.get_text(strip=True) or "").strip()
            if 'Плановый' in text and 'релиз' in text.lower():
                release_type = 'Плановый релиз'
                break
            if 'Hotfix' in text or 'hotfix' in text.lower():
                release_type = 'Hotfix'
                break
        if not release_type:
            release_type = "Плановый релиз"
        responsible = ""
        if len(cells) >= 5:
            responsible = (cells[-1].get_text(strip=True) or "").strip()[:200]
        return release_type, responsible

    def parse_release_table(self, html_content: str) -> List[Dict]:
        releases = []
        rows_with_cells = 0
        skipped_no_id = 0
        skipped_no_status = 0
        logger.info("=== ПАРСИНГ ТАБЛИЦЫ РЕЛИЗОВ (по содержимому строк) ===")
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            tables = soup.find_all('table')
            logger.info(f"🔍 Найдено таблиц: {len(tables)}")
            for ti, table in enumerate(tables):
                rows = table.find_all('tr')
                for ri, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue
                    rows_with_cells += 1
                    try:
                        release_id, release_link, release_full_text = self._find_release_id_in_row(cells)
                        if not release_id:
                            skipped_no_id += 1
                            continue
                        status = self._find_status_in_row(cells)
                        if not status:
                            # Fallback: любая ячейка с похожим на статус текстом (чтобы не терять релизы)
                            for cell in cells:
                                t = (cell.get_text(strip=True) or "").strip()
                                if 3 <= len(t) <= 50 and 'HRPRELEASE' not in t and not re.search(r'^\d{2}\.\d{2}\.\d{4}', t):
                                    if 'Готов' in t or 'ПРОМ' in t or 'Установ' in t:
                                        status = t
                                        break
                            if not status:
                                skipped_no_status += 1
                                continue
                            # Приводим к одному из известных, если возможно
                            if status not in RELEASE_STATUSES:
                                if 'Готов' in status:
                                    status = 'Готов'
                                elif 'Установка на ПРОМ' in status:
                                    status = 'Установка на ПРОМ'
                                elif 'ПРОМ' in status or 'Установлен' in status:
                                    status = 'Установлен на ПРОМ'
                        date_raw, release_datetime = self._find_date_in_row(cells)
                        release_type, responsible = self._find_type_and_responsible_in_row(cells)
                        date_str = date_raw or ""

                        jira_key = f"HRPRELEASE-{release_id}"
                        jira_details = self.get_jira_release_details(jira_key)
                        if jira_details and jira_details.get('services'):
                            extended_description = f"{jira_details.get('summary', '')} {jira_details['services']}".strip()
                        elif jira_details and jira_details.get('summary'):
                            extended_description = jira_details['summary']
                        else:
                            extended_description = re.sub(r'^HRPRELEASE-\d+\s*', '', release_full_text)
                            extended_description = re.sub(r'^[\s\-–—]+', '', extended_description).strip()

                        releases.append({
                            'id': release_id,
                            'type': release_type,
                            'date': date_str,
                            'datetime': release_datetime,
                            'status': status,
                            'responsible': responsible,
                            'full_description': extended_description or "Описание не найдено",
                            'jira_link': release_link,
                            'full_text': release_full_text,
                            'jira_details': jira_details,
                        })
                        logger.info(f"✅ HRPRELEASE-{release_id}: {status}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка строки таблица {ti+1} строка {ri+1}: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга HTML: {e}")
            import traceback
            logger.error(traceback.format_exc())
        logger.info(
            f"📊 Диагностика: строк с 2+ ячейками={rows_with_cells}, "
            f"без ID={skipped_no_id}, без подходящего статуса={skipped_no_status}, принято={len(releases)}"
        )
        if not releases and rows_with_cells > 0:
            logger.warning("⚠️ Релизов 0 при наличии строк. Проверьте структуру таблицы и статусы в источнике.")
        return releases

    def group_releases_by_weeks(self, releases: List[Dict], periods: List[Dict]) -> Dict[str, List]:
        grouped = defaultdict(list)
        logger.info("🔄 Распределение по периодам (суббота–пятница)...")
        for r in releases:
            if r.get('datetime'):
                label = self.get_week_for_date(r['datetime'], periods)
                grouped[label].append(r)
                logger.info(f"   📅 HRPRELEASE-{r['id']} → {label}")
            else:
                grouped["Без даты"].append(r)
                logger.info(f"   ❓ HRPRELEASE-{r['id']} → Без даты")
        return dict(grouped)

    # --- HTML отчёта ---

    def format_extended_weekly_table(
        self,
        grouped_releases: Dict,
        periods: List[Dict],
        all_releases: List[Dict],
    ) -> str:
        total = sum(len(v) for v in grouped_releases.values())
        now = datetime.now()
        legend = self.count_legend_statistics(all_releases)
        months_data = defaultdict(list)
        months_stats = defaultdict(int)
        for p in periods:
            mk = p['start'].strftime('%Y-%m')
            months_data[mk].append({'period': p, 'releases': grouped_releases.get(p['label'], [])})
            months_stats[mk] += len(grouped_releases.get(p['label'], []))

        html = f"""
<h2>Расширенный отчет внедренных релизов (с мая 2025)</h2>
<p><em>Отчет сформирован: <strong>{now.strftime('%d.%m.%Y %H:%M')}</strong></em></p>
<p><em>Периоды: с мая 2025, недели суббота–пятница</em></p>
<h3>Общая статистика</h3>
<p><strong>Всего релизов с мая: {total}</strong></p>
<h4>Статистика по типам и статусам:</h4>
{self.format_legend_statistics(legend)}
<h4>Статистика по месяцам:</h4>
<table class="confluenceTable"><tbody>
<tr><th class="confluenceTh">Месяц</th><th class="confluenceTh">Количество</th><th class="confluenceTh">%</th></tr>
"""
        for mk in sorted(months_stats.keys(), reverse=True):
            cnt = months_stats[mk]
            pct = (cnt / total * 100) if total else 0
            month_name = datetime.strptime(mk, '%Y-%m').strftime('%B %Y')
            html += f"<tr><td class=\"confluenceTd\"><strong>{month_name}</strong></td><td class=\"confluenceTd\">{cnt}</td><td class=\"confluenceTd\">{pct:.1f}%</td></tr>\n"
        html += "</tbody></table>\n<h4>Легенда:</h4><ul><li>🟢 Плановый релиз</li><li>⬆️ Hotfix</li><li>☁️ Статус \"Готов\"</li></ul><hr/>\n"

        for mk in sorted(months_data.keys(), reverse=True):
            month_name = datetime.strptime(mk, '%Y-%m').strftime('%B %Y')
            month_total = months_stats[mk]
            html += f"<h3>📅 {month_name} (релизов: {month_total})</h3>\n"
            if month_total == 0:
                html += "<p><em>Релизы не найдены</em></p><hr/>\n"
                continue
            weeks = sorted(months_data[mk], key=lambda x: x['period']['start'], reverse=True)
            for w in weeks:
                period, week_releases = w['period'], w['releases']
                if not week_releases:
                    continue
                html += f"<h4>Неделя {period['label']} (релизов: {len(week_releases)})</h4>\n"
                html += "<table class=\"confluenceTable\"><tbody>\n"
                html += "<tr><th class=\"confluenceTh\">Тип</th><th class=\"confluenceTh\">ID</th><th class=\"confluenceTh\">Дата</th><th class=\"confluenceTh\">Статус</th><th class=\"confluenceTh\">Описание</th><th class=\"confluenceTh\">Ответственный</th></tr>\n"
                for r in sorted(week_releases, key=lambda x: x['datetime'] or datetime.min, reverse=True):
                    icon = self.get_status_icon(r['type'], r['status'])
                    link = r['jira_link'] or f"{self.config['jira']['url']}/browse/HRPRELEASE-{r['id']}"
                    rid_html = f'<a href="{link}">HRPRELEASE-{r["id"]}</a>'
                    desc = r.get('full_description') or "Описание не найдено"
                    if r.get('jira_details') and r['jira_details'].get('services'):
                        s = r['jira_details'].get('summary', '')
                        sv = r['jira_details'].get('services', '')
                        desc = f"{rid_html} - {s} {sv}".strip() if sv else (f"{rid_html} - {s}" if s else f"{rid_html} - {desc}")
                    else:
                        desc = f"{rid_html} - {desc}"
                    html += f"<tr><td class=\"confluenceTd\" style=\"text-align:center;font-size:16px;\">{icon}</td><td class=\"confluenceTd\"><strong>{rid_html}</strong></td><td class=\"confluenceTd\">{r['date']}</td><td class=\"confluenceTd\">{r['status']}</td><td class=\"confluenceTd\">{desc}</td><td class=\"confluenceTd\">{r['responsible']}</td></tr>\n"
                html += "</tbody></table>\n"
            html += "<hr/>\n"

        if grouped_releases.get("Без даты"):
            no_date = grouped_releases["Без даты"]
            html += f"<h3>🔍 Релизы без даты ({len(no_date)})</h3>\n<table class=\"confluenceTable\"><tbody>\n"
            html += "<tr><th class=\"confluenceTh\">Тип</th><th class=\"confluenceTh\">ID</th><th class=\"confluenceTh\">Статус</th><th class=\"confluenceTh\">Описание</th><th class=\"confluenceTh\">Ответственный</th></tr>\n"
            for r in no_date:
                icon = self.get_status_icon(r['type'], r['status'])
                link = r['jira_link'] or f"{self.config['jira']['url']}/browse/HRPRELEASE-{r['id']}"
                rid_html = f'<a href="{link}">HRPRELEASE-{r["id"]}</a>'
                desc = r.get('full_description') or "Описание не найдено"
                desc = f"{rid_html} - {desc}"
                html += f"<tr><td class=\"confluenceTd\" style=\"text-align:center;\">{icon}</td><td class=\"confluenceTd\"><strong>{rid_html}</strong></td><td class=\"confluenceTd\">{r['status']}</td><td class=\"confluenceTd\">{desc}</td><td class=\"confluenceTd\">{r['responsible']}</td></tr>\n"
            html += "</tbody></table>\n<hr/>\n"

        src_url = f"{self.config['confluence']['url']}/pages/viewpage.action?pageId={self.config['confluence']['source_page_id']}"
        html += f"""
<p><em>Статусы: "Установлен на ПРОМ", "Готов", "Установка на ПРОМ"</em></p>
<p><em>Источник: <a href="{src_url}">страница релизов</a></em></p>
<p><em>Актуально: {now.strftime('%d.%m.%Y %H:%M:%S')}</em></p>
<p><em>Период: с мая 2025 по {now.strftime('%B %Y')}</em></p>
"""
        return html

    def save_debug_html(self, content: str, filename: str = "debug_content.html") -> None:
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"💾 HTML сохранён: {filename}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")

    # --- Главный сценарий ---

    def generate_extended_weekly_report(self) -> bool:
        logger.info("🚀 Генерация расширенного отчёта с мая 2025")
        if not self.test_api_connection():
            logger.error("❌ Нет подключения к Confluence — выход")
            return False

        periods = self.get_week_periods_from_may()
        source_content, _ = self.get_confluence_page_content(self.config['confluence']['source_page_id'])
        if not source_content:
            logger.error("❌ Не удалось получить исходную страницу")
            return False

        self.save_debug_html(source_content)
        releases = self.parse_release_table(source_content)
        if not releases:
            logger.warning("⚠️ Релизов после фильтрации: 0. Публикуем отчёт с нулевой статистикой.")

        legend = self.count_legend_statistics(releases)
        logger.info(f"📊 Легенда: Плановый={legend.get('Плановый релиз', 0)}, Hotfix={legend.get('Hotfix', 0)}, Готов={legend.get('Готов', 0)}")
        grouped = self.group_releases_by_weeks(releases, periods)

        if os.getenv('PARSE_ONLY', '').lower() in ('1', 'true', 'yes'):
            logger.info("📋 PARSE_ONLY=1: отчёт не публикуется")
            logger.info(f"📊 Найдено релизов: {len(releases)}")
            for i, r in enumerate(releases[:5]):
                logger.info(f"   {i+1}. HRPRELEASE-{r['id']} | {r['type']} | {r['date']} | {r['status']}")
            if len(releases) > 5:
                logger.info(f"   ... и ещё {len(releases) - 5}")
            return True

        report_html = self.format_extended_weekly_table(grouped, periods, releases)
        _, current_version = self.get_confluence_page_content(self.config['confluence']['target_page_id'])
        if current_version is None:
            logger.error("❌ Не удалось получить целевую страницу")
            return False
        if not self.update_confluence_page(self.config['confluence']['target_page_id'], report_html, current_version):
            logger.error("❌ Ошибка публикации отчёта")
            return False
        logger.info("🎉 Отчёт успешно сгенерирован и опубликован")
        return True


def main() -> bool:
    if not os.getenv('CONFLUENCE_TOKEN'):
        logger.error("❌ Задайте CONFLUENCE_TOKEN в .env")
        logger.info("Опционально: JIRA_TOKEN, DEBUG_MODE=true, PARSE_ONLY=true")
        return False
    try:
        return ConfluenceReleaseStats().generate_extended_weekly_report()
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    exit(0 if main() else 1)
