import requests
import re
from typing import List, Dict, Set
import logging
from datetime import datetime
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class ConfluenceDeployPlanGenerator:
    """Генератор деплой-планов из шаблона Confluence"""

    def __init__(self, confluence_url: str, confluence_token: str, template_page_id: str):
        self.confluence_url = confluence_url.rstrip('/')
        self.confluence_token = confluence_token
        self.template_page_id = template_page_id
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'Authorization': f'Bearer {confluence_token}',
            'Content-Type': 'application/json'
        })

        if 'REQUESTS_CA_BUNDLE' in os.environ:
            del os.environ['REQUESTS_CA_BUNDLE']
        if 'CURL_CA_BUNDLE' in os.environ:
            del os.environ['CURL_CA_BUNDLE']

    def get_template_content(self) -> str:
        """Получает HTML шаблона из Confluence"""
        try:
            url = f"{self.confluence_url}/rest/api/content/{self.template_page_id}"
            params = {'expand': 'body.storage'}

            logger.info(f"📄 Загрузка шаблона ID: {self.template_page_id}")
            response = self.session.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                template_html = data['body']['storage']['value']
                logger.info(f"✅ Шаблон загружен ({len(template_html)} символов)")
                return template_html
            else:
                logger.error(f"❌ Ошибка загрузки: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return None

    def get_page_id_by_title(self, space_key: str, page_title: str) -> str:
        """Получает ID страницы по названию"""
        try:
            url = f"{self.confluence_url}/rest/api/content"
            params = {'spaceKey': space_key, 'title': page_title, 'type': 'page'}

            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                results = response.json().get('results', [])
                if results:
                    return results[0]['id']
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return None

    def generate_deploy_table_rows(self, services: List[str], team_name: str = "Команда") -> str:
        """Генерирует строки таблицы деплоя"""
        deploy_date_iso = datetime.now().strftime('%Y-%m-%d')

        rows = []
        for idx, service in enumerate(services, 1):
            rows.append(f'''    <tr>
      <td>{idx}</td>
      <td>{team_name}</td>
      <td>{service}</td>
      <td>Update+migration+deploy</td>
      <td><time datetime="{deploy_date_iso}"/></td>
      <td></td>
    </tr>''')

        return '\n'.join(rows)

    def generate_rollback_table_rows(self, services: List[str], team_name: str = "Команда") -> str:
        """Генерирует строки таблицы отката"""
        deploy_date_iso = datetime.now().strftime('%Y-%m-%d')

        rows = []
        for idx, service in enumerate(services, 1):
            rows.append(f'''    <tr>
      <td>{idx}</td>
      <td>{team_name}</td>
      <td>{service}</td>
      <td>откат на предыдущую стабильную версию</td>
      <td><time datetime="{deploy_date_iso}"/></td>
      <td></td>
    </tr>''')

        return '\n'.join(rows)

    def create_deploy_plan_page(
            self,
            space_key: str,
            parent_page_title: str,
            release_key: str,
            release_summary: str,
            release_url: str,
            services: List[str],
            team_name: str = "Команда"
    ) -> Dict:
        """Создает деплой-план из шаблона с заполнением таблиц"""
        try:
            logger.info(f"📝 Создание деплой-плана для {release_key}")

            # Загружаем шаблон
            template = self.get_template_content()
            if not template:
                return {'success': False, 'message': 'Не удалось загрузить шаблон'}

            # Получаем ID родителя
            parent_page_id = self.get_page_id_by_title(space_key, parent_page_title)
            if not parent_page_id:
                return {'success': False, 'message': f'Родитель "{parent_page_title}" не найден'}

            # Название с timestamp
            page_title = f"Deploy план {release_summary} "

            # Генерируем данные
            deploy_rows = self.generate_deploy_table_rows(services, team_name)
            rollback_rows = self.generate_rollback_table_rows(services, team_name)

            # ЗАМЕНЫ в шаблоне
            content = template

            # 1. НЕ ТРОГАЕМ макрос Jira - только заменяем ключ
            content = re.sub(
                r'(<ac:parameter ac:name="key">)[^<]+(</ac:parameter>)',
                f'\\1{release_key}\\2',
                content
            )

            # 2. Ищем таблицу Деплой - это ПОСЛЕДНЯЯ таблица ПЕРЕД <h2>План отката</h2>
            deploy_found = False
            rollback_h2_pos = content.find('<h2>План отката</h2>')

            if rollback_h2_pos > 0:
                content_before_rollback = content[:rollback_h2_pos]
                tbody_matches_before = list(re.finditer(r'<tbody[^>]*>.*?</tbody>', content_before_rollback, re.DOTALL))

                logger.info(f"DEBUG: Найдено {len(tbody_matches_before)} tbody до 'План отката'")

                if len(tbody_matches_before) >= 1:
                    last_tbody_match = tbody_matches_before[-1]
                    inner_match = re.match(r'<tbody[^>]*>(.*)</tbody>', last_tbody_match.group(0), re.DOTALL)

                    if inner_match:
                        start_pos = last_tbody_match.start() + inner_match.start(1)
                        end_pos = last_tbody_match.start() + inner_match.end(1)
                        content = content[:start_pos] + '\n' + deploy_rows + '\n' + content[end_pos:]
                        logger.info(f"✅ Таблица Деплой заполнена ({len(services)} сервисов)")
                        deploy_found = True

            if not deploy_found:
                logger.warning(f"⚠️ Таблица Деплой НЕ найдена!")

            # 3. Заполняем таблицу План отката
            rollback_match = re.search(
                r'(<h2>План отката</h2>.*?<tbody[^>]*>)(.*?)(</tbody>)',
                content,
                re.DOTALL
            )

            if rollback_match:
                content = content[:rollback_match.start(2)] + '\n' + rollback_rows + '\n' + content[rollback_match.end(2):]
                logger.info(f"✅ Таблица План отката заполнена ({len(services)} сервисов)")
            else:
                logger.warning(f"⚠️ Таблица План отката не найдена")

            # Создаем страницу
            create_data = {
                "type": "page",
                "title": page_title,
                "space": {"key": space_key},
                "ancestors": [{"id": parent_page_id}],
                "body": {
                    "storage": {
                        "value": content,
                        "representation": "storage"
                    }
                },
                "metadata": {
                    "labels": [{"prefix": "global", "name": "hrp_deploy"}]
                }
            }

            url = f"{self.confluence_url}/rest/api/content"
            response = self.session.post(url, json=create_data, timeout=30)

            if response.status_code == 200:
                page_data = response.json()
                page_id = page_data['id']
                page_url = f"{self.confluence_url}{page_data['_links']['webui']}"

                try:
                    label_url = f"{self.confluence_url}/rest/api/content/{page_id}/label"
                    self.session.post(label_url, json=[{"prefix": "global", "name": "hrp_deploy"}], timeout=10)
                except Exception:
                    pass

                logger.info(f"✅ Страница создана: {page_url}")
                return {
                    'success': True,
                    'message': 'Деплой-план создан',
                    'page_id': page_id,
                    'page_url': page_url,
                    'page_title': page_title
                }
            else:
                logger.error(f"❌ Ошибка: {response.status_code}")
                return {'success': False, 'message': f'Ошибка: {response.status_code}', 'details': response.text}
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'success': False, 'message': str(e)}


class MasterServicesAnalyzer:
    """Анализатор сервисов в master"""

    def __init__(self, jira_service, confluence_generator=None):
        self.jira_service = jira_service
        self.stash_base = "https://stash.sigma.sbrf.ru"
        self.excluded_prefixes = ('HRPRELEASE', 'HRPKIA', 'PPSL', 'PERSONALDA', 'HRPKI')
        self.confluence_generator = confluence_generator

    def get_release_summary(self, release_key: str) -> str:
        """Получает Summary релиза"""
        try:
            issue_data = self.jira_service.get_issue_details(release_key)
            if issue_data:
                summary = issue_data.get('fields', {}).get('summary', release_key)
                logger.info(f"📋 Summary: {summary}")
                return summary
            return release_key
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return release_key

    def get_linked_subtasks(self, issue_key: str) -> List[str]:
        """Получает связанные задачи"""
        try:
            issue_data = self.jira_service.get_issue_details(issue_key)
            if not issue_data:
                return []

            linked_keys = []
            fields = issue_data.get('fields', {})

            for subtask in fields.get('subtasks', []):
                key = subtask['key']
                if not key.startswith(self.excluded_prefixes):
                    linked_keys.append(key)

            for link in fields.get('issuelinks', []):
                if 'outwardIssue' in link:
                    key = link['outwardIssue']['key']
                    if not key.startswith(self.excluded_prefixes):
                        linked_keys.append(key)
                if 'inwardIssue' in link:
                    key = link['inwardIssue']['key']
                    if not key.startswith(self.excluded_prefixes):
                        linked_keys.append(key)

            if linked_keys:
                logger.info(f"  🔗 {issue_key}: {len(linked_keys)} связанных")
            return linked_keys
        except Exception as e:
            logger.error(f"  ❌ Ошибка: {e}")
            return []

    def extract_pr_links(self, issue_key: str, processed: Set[str], depth: int = 0, max_depth: int = 1) -> List[Dict]:
        """Извлекает PR"""
        try:
            if issue_key in processed or depth > max_depth:
                return []

            processed.add(issue_key)
            indent = "  " * depth
            logger.info(f"{indent}📎 {issue_key} (глубина: {depth})")

            pr_links = []
            issue_data = self.jira_service.get_issue_details(issue_key)
            if not issue_data:
                return []

            try:
                dev_url = f"{self.jira_service.config.url}/rest/dev-status/1.0/issue/detail"
                params = {
                    'issueId': issue_data['id'],
                    'applicationType': 'stash',
                    'dataType': 'pullrequest'
                }

                response = requests.get(
                    dev_url,
                    params=params,
                    headers={'Authorization': f'Bearer {self.jira_service.config.token}'},
                    verify=False,
                    timeout=10
                )

                if response.status_code == 200:
                    dev_data = response.json()
                    for detail in dev_data.get('detail', []):
                        for pr in detail.get('pullRequests', []):
                            pr_url = pr.get('url', '')
                            pr_status = pr.get('status', 'UNKNOWN')

                            if pr_status == 'DECLINED':
                                continue

                            if pr_url and 'stash.sigma.sbrf.ru' in pr_url:
                                match = re.search(r'projects/([^/]+)/repos/([^/]+)/pull-requests/(\d+)', pr_url)
                                if match:
                                    project, repo, pr_id = match.groups()
                                    pr_links.append({
                                        'project': project,
                                        'repo': repo,
                                        'pr_id': pr_id,
                                        'url': pr_url,
                                        'status': pr_status,
                                        'source': issue_key
                                    })
                                    logger.info(f"{indent}  ✅ {repo}/#{pr_id} [{pr_status}]")
            except Exception as e:
                logger.warning(f"{indent}  ⚠️ Dev API: {e}")

            if depth < max_depth:
                linked = self.get_linked_subtasks(issue_key)
                if linked:
                    for linked_key in linked:
                        linked_prs = self.extract_pr_links(linked_key, processed, depth + 1, max_depth)
                        pr_links.extend(linked_prs)

            unique = []
            seen = set()
            for link in pr_links:
                key = (link['repo'], link['pr_id'])
                if key not in seen:
                    seen.add(key)
                    unique.append(link)

            return unique
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return []

    def check_pr_merged_to_master(self, pr_data: Dict) -> bool:
        """Проверка PR"""
        try:
            if 'status' in pr_data:
                return pr_data['status'] == 'MERGED'

            project = pr_data['project']
            repo = pr_data['repo']
            pr_id = pr_data['pr_id']
            api_url = f"{self.stash_base}/rest/api/1.0/projects/{project}/repos/{repo}/pull-requests/{pr_id}"

            response = requests.get(api_url, verify=False, timeout=5)

            if response.status_code == 200:
                pr_full = response.json()
                state = pr_full.get('state', '')
                to_ref = pr_full.get('toRef', {}).get('displayId', '')
                return state == 'MERGED' and 'master' in to_ref.lower()

            return False
        except Exception as e:
            logger.error(f"Ошибка проверки: {e}")
            return False

    def analyze_release(self, release_key: str) -> Dict:
        """Анализ релиза"""
        try:
            logger.info(f"🔍 Анализ {release_key}")
            release_url = f"{self.jira_service.config.url}/browse/{release_key}"
            release_summary = self.get_release_summary(release_key)
            linked = self.jira_service.get_linked_issues(release_key)

            if not linked:
                return {
                    'success': False,
                    'message': 'Нет задач',
                    'services': [],
                    'total_tasks': 0,
                    'total_prs': 0,
                    'pr_details': [],
                    'release_key': release_key,
                    'release_summary': release_summary,
                    'release_url': release_url
                }

            logger.info(f"📊 Задач: {len(linked)}")

            services = set()
            pr_details = []

            for i, issue_key in enumerate(linked, 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"⏳ {i}/{len(linked)}: {issue_key}")
                logger.info(f"{'='*60}")

                processed = set()
                prs = self.extract_pr_links(issue_key, processed, 0, 1)

                for pr in prs:
                    repo = pr['repo']
                    source = pr.get('source', issue_key)

                    if self.check_pr_merged_to_master(pr):
                        services.add(repo)
                        pr_details.append({
                            'issue': issue_key,
                            'source': source,
                            'service': repo,
                            'pr_url': pr['url'],
                            'status': 'merged_to_master'
                        })

            logger.info(f"\n{'='*60}")
            logger.info(f"📋 Сервисов: {sorted(services)}")
            logger.info(f"{'='*60}")

            return {
                'success': True,
                'message': f'Найдено {len(services)} сервисов',
                'services': sorted(list(services)),
                'total_tasks': len(linked),
                'total_prs': len(pr_details),
                'pr_details': pr_details,
                'release_key': release_key,
                'release_summary': release_summary,
                'release_url': release_url
            }
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return {
                'success': False,
                'message': f'Ошибка: {str(e)}',
                'services': [],
                'total_tasks': 0,
                'total_prs': 0,
                'pr_details': [],
                'release_key': release_key,
                'release_summary': release_key,
                'release_url': ''
            }

    def generate_deploy_plan(
            self,
            analysis_result: Dict,
            space_key: str,
            parent_page_title: str,
            team_name: str = "Команда"
    ) -> Dict:
        """Генерация плана"""
        if not self.confluence_generator:
            return {'success': False, 'message': 'Нет генератора'}

        if not analysis_result.get('success'):
            return {'success': False, 'message': 'Анализ не выполнен'}

        services = analysis_result.get('services', [])
        if not services:
            return {'success': False, 'message': 'Нет сервисов'}

        logger.info(f"🚀 Создание для {len(services)} сервисов")

        return self.confluence_generator.create_deploy_plan_page(
            space_key=space_key,
            parent_page_title=parent_page_title,
            release_key=analysis_result['release_key'],
            release_summary=analysis_result['release_summary'],
            release_url=analysis_result['release_url'],
            services=services,
            team_name=team_name
        )
