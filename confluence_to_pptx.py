#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

try:
    import requests
    from bs4 import BeautifulSoup
    from dotenv import load_dotenv
    from pptx import Presentation
    from pptx.util import Pt
    from urllib3.exceptions import InsecureRequestWarning
except ModuleNotFoundError as exc:
    missing = str(exc).split("'")[1] if "'" in str(exc) else str(exc)
    print(
        "Не хватает python-библиотек. Установите:\n"
        "python3 -m pip install requests python-dotenv urllib3 beautifulsoup4 python-pptx\n"
        f"Не найден модуль: {missing}",
        file=sys.stderr,
    )
    raise SystemExit(2)


SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "confluence_to_pptx.log"
DEBUG_HTML_PATH = SCRIPT_DIR / "debug_content.html"
DEBUG_JSON_PATH = SCRIPT_DIR / "releases_dump.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_PATH, encoding="utf-8")],
    force=True,
)
logger = logging.getLogger("confluence_to_pptx")

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

IOS_ID = "2257858"
ANDROID_ID = "2935717"
NEURO_IDS = ("9644020", "9643401", "9644023", "9644025")


@dataclass
class Release:
    key: str
    release_type: str
    date_raw: str
    date_value: Optional[datetime]
    status: str
    description: str
    responsible: str
    jira_link: str

    @property
    def blob(self) -> str:
        return f"{self.key} {self.release_type} {self.description} {self.responsible}".lower()


def load_env() -> None:
    explicit = os.getenv("ENV_FILE", "").strip()
    candidates = [Path.cwd() / ".env", SCRIPT_DIR / ".env"]
    if explicit:
        candidates.insert(0, Path(explicit))
    for candidate in candidates:
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            logger.info("Загружен .env: %s", candidate)
            return
    logger.info(".env не найден (используем переменные окружения процесса)")


def parse_csv_env(name: str, default_values: Sequence[str]) -> List[str]:
    value = os.getenv(name, "")
    if not value.strip():
        return list(default_values)
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_date(text: str) -> Optional[datetime]:
    m = re.search(r"\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4}", text or "")
    if not m:
        return None
    raw = m.group(0)
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def week_range(now: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    current = now or datetime.now()
    days_since_friday = (current.weekday() - 4) % 7
    end = current - timedelta(days=days_since_friday)
    start = end - timedelta(days=6)
    return start.replace(hour=0, minute=0, second=0, microsecond=0), end.replace(
        hour=23, minute=59, second=59, microsecond=0
    )


def get_confluence_html(base_url: str, token: str, page_id: str, verify_ssl: bool) -> str:
    logger.info("Запрашиваем Confluence pageId=%s", page_id)
    url = f"{base_url.rstrip('/')}/rest/api/content/{page_id}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={"expand": "body.storage"},
        verify=verify_ssl,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["body"]["storage"]["value"]


def parse_releases(html: str, allowed_statuses: Sequence[str]) -> List[Release]:
    allowed = set(allowed_statuses)
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    logger.info("Найдено таблиц: %d", len(tables))
    releases: List[Release] = []

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [
            " ".join(cell.get_text(" ", strip=True).lower().split())
            for cell in rows[0].find_all(["th", "td"])
        ]
        try:
            idx_type = headers.index("тип") if "тип" in headers else None
            idx_id = headers.index("id релиза") if "id релиза" in headers else 1
            idx_date = headers.index("дата") if "дата" in headers else 2
            idx_status = headers.index("статус") if "статус" in headers else 3
            idx_desc = headers.index("описание релиза") if "описание релиза" in headers else 4
            idx_resp = headers.index("ответственный") if "ответственный" in headers else 5
        except ValueError:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= max(idx_id, idx_status):
                continue
            rel_cell = cells[idx_id]
            rel_text = rel_cell.get_text(" ", strip=True)
            match = re.search(r"HRPRELEASE-(\d+)", rel_text, re.IGNORECASE)
            if not match:
                continue
            key = f"HRPRELEASE-{match.group(1)}"
            status = cells[idx_status].get_text(" ", strip=True)
            if status not in allowed:
                continue

            link_tag = rel_cell.find("a", href=True)
            releases.append(
                Release(
                    key=key,
                    release_type=(cells[idx_type].get_text(" ", strip=True) if idx_type is not None and len(cells) > idx_type else ""),
                    date_raw=(cells[idx_date].get_text(" ", strip=True) if len(cells) > idx_date else ""),
                    date_value=parse_date(cells[idx_date].get_text(" ", strip=True) if len(cells) > idx_date else ""),
                    status=status,
                    description=(cells[idx_desc].get_text(" ", strip=True) if len(cells) > idx_desc else ""),
                    responsible=(cells[idx_resp].get_text(" ", strip=True) if len(cells) > idx_resp else ""),
                    jira_link=(link_tag["href"] if link_tag else ""),
                )
            )
    logger.info("Распарсили релизов: %d", len(releases))
    return releases


def filter_weekly(
    releases: Sequence[Release],
    keywords: Sequence[str],
    start: datetime,
    end: datetime,
) -> List[Release]:
    prepared_keywords = [k.lower() for k in keywords if k.strip()]
    out: List[Release] = []
    for rel in releases:
        if not rel.date_value or not (start <= rel.date_value <= end):
            continue
        if prepared_keywords:
            blob = f"{rel.release_type} {rel.description} {rel.key}".lower()
            if not any(k in blob for k in prepared_keywords):
                continue
        out.append(rel)
    out.sort(key=lambda r: r.date_value or datetime.min, reverse=True)
    logger.info("Релизов за период для слайда: %d", len(out))
    return out


def detect_mobile_releases(releases: Sequence[Release]) -> List[Release]:
    return [rel for rel in releases if IOS_ID in rel.blob or ANDROID_ID in rel.blob]


def detect_main_releases(releases: Sequence[Release]) -> List[Release]:
    return [rel for rel in releases if any(task_id in rel.blob for task_id in NEURO_IDS)]


def pluralize_releases(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "релиз"
    if count % 10 in (2, 3, 4) and count % 100 not in (12, 13, 14):
        return "релиза"
    return "релизов"


def extract_versions(text: str) -> List[str]:
    versions = re.findall(r"v\s?\d+\.\d+(?:\.\d+)?", text, flags=re.IGNORECASE)
    unique: List[str] = []
    seen = set()
    for item in versions:
        normalized = item.replace(" ", "").lower()
        if normalized not in seen:
            unique.append(item.strip())
            seen.add(normalized)
    return unique


def summarize_mobile(releases: Sequence[Release]) -> str:
    if not releases:
        return "Релизов не было"

    ios_releases = [rel for rel in releases if IOS_ID in rel.blob or "ios" in rel.blob]
    android_releases = [rel for rel in releases if ANDROID_ID in rel.blob or "android" in rel.blob]

    parts: List[str] = []
    if android_releases:
        parts.append("Android")
    if ios_releases:
        parts.append("iOS")
    if not parts:
        parts.append("RN/Android/iOS")

    versions: List[str] = []
    for rel in releases:
        versions.extend(extract_versions(rel.description))
    version_part = f" ({', '.join(versions)})" if versions else ""

    return f"{len(releases)} {pluralize_releases(len(releases))} для МП ({', '.join(parts)}){version_part}"


def summarize_main(releases: Sequence[Release]) -> str:
    if not releases:
        return "Релизов не было"
    return f"{len(releases)} {pluralize_releases(len(releases))} Новой главной + Агентов"


def classify_activity(rel: Release) -> str:
    text = rel.description.lower()
    if IOS_ID in rel.blob or ANDROID_ID in rel.blob or " ios " in f" {text} " or " android " in f" {text} ":
        return "МП"
    if any(task_id in rel.blob for task_id in NEURO_IDS):
        return "Новая главная + Агенты"
    if "launchpad" in text or "списка приложений" in text:
        return "Ланчпад"
    if "coreui-configurator" in text or "configurator" in text:
        return "Configurator"
    if "coreui(" in text or "hrp.coreui" in text:
        return "CoreUI"
    if "coretech(" in text or "coretech" in text:
        return "CoreTech"
    if "perftracker" in text:
        return "PerfTracker"
    if "landingbuilder" in text or "landing builder" in text:
        return "Landing builder"

    service_match = re.search(r"([A-Za-zА-Яа-я0-9\.\-]+)\(\d{6,8}\)", rel.description)
    if service_match:
        name = service_match.group(1)
        if name.startswith("HRP.CoreUI-"):
            name = name.replace("HRP.CoreUI-", "")
        return name
    return rel.key


def summarize_activity(releases: Sequence[Release]) -> List[str]:
    lines = [f"Релизная активность: {len(releases)} релизных задач"]
    if not releases:
        lines.append("Релизов не было")
        return lines

    counters = Counter(classify_activity(rel) for rel in releases)
    ordered_names = [
        "Ланчпад",
        "CoreUI",
        "CoreTech",
        "Configurator",
        "PerfTracker",
        "Landing builder",
        "МП",
        "Новая главная + Агенты",
    ]

    used = set()
    for name in ordered_names:
        count = counters.get(name, 0)
        if not count:
            continue
        used.add(name)
        if count == 1:
            lines.append(f"Релиз для {name}")
        else:
            lines.append(f"{count} {pluralize_releases(count)} для {name}")

    for name, count in sorted(counters.items(), key=lambda item: (-item[1], item[0])):
        if name in used:
            continue
        if count == 1:
            lines.append(f"Релиз для {name}")
        else:
            lines.append(f"{count} {pluralize_releases(count)} для {name}")

    return lines


def build_final_text(mobile: Sequence[Release], main: Sequence[Release], activity: Sequence[Release]) -> str:
    lines = [
        "Мобильные приложения (RN, Android, iOS):",
        summarize_mobile(mobile),
        "",
        "Новая главная + Агенты:",
        summarize_main(main),
        "",
    ]
    lines.extend(summarize_activity(activity))
    return "\n".join(lines)


def save_debug(html: str, releases: Sequence[Release]) -> None:
    DEBUG_HTML_PATH.write_text(html, encoding="utf-8")
    data = []
    for rel in releases:
        data.append(
            {
                "key": rel.key,
                "type": rel.release_type,
                "date_raw": rel.date_raw,
                "date_value": rel.date_value.isoformat() if rel.date_value else None,
                "status": rel.status,
                "description": rel.description,
                "responsible": rel.responsible,
                "jira_link": rel.jira_link,
            }
        )
    DEBUG_JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Сохранили debug файлы: %s, %s", DEBUG_HTML_PATH, DEBUG_JSON_PATH)


def resolve_template_path(raw_path: str) -> Path:
    """
    Resolve template path robustly across different user homes and
    localized Downloads directory names.
    """
    base = Path(raw_path).expanduser()
    candidates = [base]

    # Common ru/en directory-name swaps.
    raw = str(base)
    if "/Загрузки/" in raw:
        candidates.append(Path(raw.replace("/Загрузки/", "/Downloads/")))
    if "/Downloads/" in raw:
        candidates.append(Path(raw.replace("/Downloads/", "/Загрузки/")))

    # Same filename in current user's home download folders.
    filename = base.name
    home = Path.home()
    candidates.append(home / "Downloads" / filename)
    candidates.append(home / "Загрузки" / filename)

    # Same filename in current working directory.
    candidates.append(Path.cwd() / filename)

    unique_candidates: List[Path] = []
    seen = set()
    for item in candidates:
        normalized = str(item)
        if normalized not in seen:
            unique_candidates.append(item)
            seen.add(normalized)

    for item in unique_candidates:
        if item.exists():
            if item != base:
                logger.info("Шаблон найден по альтернативному пути: %s", item)
            return item

    logger.error("Шаблон не найден. Проверены пути:")
    for item in unique_candidates:
        logger.error(" - %s", item)
    raise FileNotFoundError(f"Шаблон не найден: {base}")


def update_presentation(
    template_path: Path,
    output_path: Path,
    slide_index: int,
    marker_text: str,
    section_title: str,
    final_text: str,
) -> None:
    template_path = resolve_template_path(str(template_path))

    prs = Presentation(str(template_path))
    if slide_index < 0 or slide_index >= len(prs.slides):
        raise RuntimeError(f"Неверный номер слайда: {slide_index + 1}")

    slide = prs.slides[slide_index]
    target = None
    for shape in slide.shapes:
        if not getattr(shape, "has_text_frame", False):
            continue
        text = shape.text or ""
        if marker_text in text or section_title.split(":")[0] in text:
            target = shape
            break
    if target is None:
        raise RuntimeError("Не найден текстовый блок для релизов на первом слайде")

    lines = final_text.splitlines() or ["Релизов не найдено"]

    tf = target.text_frame
    tf.clear()
    for idx, line in enumerate(lines):
        p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
        p.text = line
        p.level = 0
        p.font.size = Pt(14 if idx in (0, 3) else 12)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(output_path))
    logger.info("Презентация сохранена: %s", output_path)


def run_self_test() -> int:
    logger.info("Запуск self-test без сети")
    sample_html = """
    <table><tr><th>Тип</th><th>ID релиза</th><th>Дата</th><th>Статус</th><th>Описание релиза</th><th>Ответственный</th></tr>
    <tr><td>Плановый релиз</td><td>HRPRELEASE-111111</td><td>2026-03-05</td><td>Установлен на ПРОМ</td><td>Android update</td><td>Иванов</td></tr>
    </table>
    """
    parsed = parse_releases(sample_html, ["Установлен на ПРОМ"])
    if len(parsed) != 1:
        logger.error("Self-test failed: parse_releases")
        return 1
    logger.info("Self-test ok")
    return 0


def main() -> int:
    load_env()

    parser = argparse.ArgumentParser(description="Confluence releases -> PPTX first slide")
    parser.add_argument("--self-test", action="store_true", help="Run parser self-test without network")
    args = parser.parse_args()
    if args.self_test:
        return run_self_test()

    token = os.getenv("CONFLUENCE_TOKEN") or os.getenv("ATLASSIAN_TOKEN") or os.getenv("TOKEN")
    if not token:
        logger.error("Не найден токен Confluence (CONFLUENCE_TOKEN / ATLASSIAN_TOKEN / TOKEN)")
        return 1

    conf_url = os.getenv("CONFLUENCE_URL", "https://confluence.sberbank.ru")
    source_page_id = os.getenv("SOURCE_PAGE_ID", "18588013525")
    verify_ssl = os.getenv("CONFLUENCE_VERIFY_SSL", "false").lower() == "true"
    statuses = parse_csv_env("RELEASE_STATUSES", ["Установлен на ПРОМ", "Готов", "Установка на ПРОМ"])
    keywords = parse_csv_env("MOBILE_RELEASE_KEYWORDS", [])

    template_path = Path(os.getenv("PPTX_TEMPLATE_PATH", "~/Downloads/ОС ЦРФК 20.02.pptx"))
    output_path = Path(os.getenv("PPTX_OUTPUT_PATH", str(template_path.with_name(f"{template_path.stem} (авто).pptx"))))
    slide_index = int(os.getenv("PPTX_SLIDE_INDEX", "1")) - 1
    marker_text = os.getenv("PPTX_RELEASES_MARKER", "Релизов не найдено")
    section_title = os.getenv("PPTX_SECTION_TITLE", "Мобильные приложения (RN, Android, iOS):")

    try:
        html = get_confluence_html(conf_url, token, source_page_id, verify_ssl)
        releases = parse_releases(html, statuses)
        save_debug(html, releases)
        start, end = week_range()
        filtered = filter_weekly(releases, keywords, start, end)
        mobile = detect_mobile_releases(filtered)
        main_group = detect_main_releases(filtered)
        final_text = build_final_text(mobile, main_group, filtered)
        logger.info("Итоговый текст для слайда:\n%s", final_text)
        update_presentation(
            template_path=template_path,
            output_path=output_path,
            slide_index=slide_index,
            marker_text=marker_text,
            section_title=section_title,
            final_text=final_text,
        )
        logger.info("Готово")
        return 0
    except requests.HTTPError as exc:
        logger.error("HTTP ошибка: %s", exc)
    except requests.RequestException as exc:
        logger.error("Сетевая ошибка: %s", exc)
    except Exception as exc:
        logger.exception("Критическая ошибка: %s", exc)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
