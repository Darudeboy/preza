#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pptx import Presentation
from pptx.util import Pt
from urllib3.exceptions import InsecureRequestWarning

load_dotenv()
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


DATE_PATTERNS = ("%Y-%m-%d", "%d.%m.%Y")
DEFAULT_STATUSES = ("Установлен на ПРОМ", "Установка на ПРОМ", "Готов")
DEFAULT_MOBILE_KEYWORDS = ("rn", "react native", "android", "ios", "мп", "mobile")


@dataclass
class Release:
    key: str
    date_raw: str
    date_value: Optional[datetime]
    status: str
    release_type: str
    description: str
    responsible: str
    jira_link: str

    def formatted_line(self) -> str:
        date_label = self.date_value.strftime("%d.%m") if self.date_value else self.date_raw or "без даты"
        owner_label = f" ({self.responsible})" if self.responsible else ""
        return f"{self.key} [{date_label}, {self.status}] - {self.description}{owner_label}"


class ConfluenceClient:
    def __init__(self, base_url: str, token: str, verify_ssl: bool) -> None:
        self.base_url = base_url.rstrip("/")
        self.verify_ssl = verify_ssl
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get_page_html(self, page_id: str) -> str:
        url = f"{self.base_url}/rest/api/content/{page_id}"
        params = {"expand": "body.storage"}
        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            verify=self.verify_ssl,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload["body"]["storage"]["value"]


class ReleaseParser:
    def __init__(self, allowed_statuses: Sequence[str]) -> None:
        self.allowed_statuses = set(allowed_statuses)

    @staticmethod
    def parse_date(text: str) -> Optional[datetime]:
        for pattern in DATE_PATTERNS:
            match = re.search(r"\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4}", text)
            if not match:
                continue
            candidate = match.group(0)
            try:
                return datetime.strptime(candidate, pattern)
            except ValueError:
                continue
        return None

    @staticmethod
    def _extract_release_key(text: str) -> str:
        match = re.search(r"HRPRELEASE-\d+", text, flags=re.IGNORECASE)
        return match.group(0).upper() if match else ""

    @staticmethod
    def _extract_link(cell) -> str:
        if not cell:
            return ""
        anchor = cell.find("a", href=True)
        return anchor["href"] if anchor else ""

    def parse_html_table(self, html_content: str) -> List[Release]:
        soup = BeautifulSoup(html_content, "html.parser")
        tables = soup.find_all("table")
        releases: List[Release] = []

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 5:
                    continue

                raw_type = cells[0].get_text(" ", strip=True) if len(cells) > 0 else ""
                release_cell = cells[1] if len(cells) > 1 else None
                raw_release = release_cell.get_text(" ", strip=True) if release_cell else ""
                raw_date = cells[2].get_text(" ", strip=True) if len(cells) > 2 else ""
                raw_status = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
                raw_description = cells[4].get_text(" ", strip=True) if len(cells) > 4 else ""
                raw_responsible = cells[5].get_text(" ", strip=True) if len(cells) > 5 else ""

                release_key = self._extract_release_key(raw_release)
                if not release_key:
                    continue
                if raw_status not in self.allowed_statuses:
                    continue

                releases.append(
                    Release(
                        key=release_key,
                        date_raw=raw_date,
                        date_value=self.parse_date(raw_date),
                        status=raw_status,
                        release_type=raw_type,
                        description=raw_description,
                        responsible=raw_responsible,
                        jira_link=self._extract_link(release_cell),
                    )
                )

        return releases


def current_week_range(now: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    current = now or datetime.now()
    days_since_friday = (current.weekday() - 4) % 7
    week_end = current - timedelta(days=days_since_friday)
    week_start = week_end - timedelta(days=6)
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0), week_end.replace(
        hour=23, minute=59, second=59, microsecond=0
    )


def filter_releases(
    releases: Sequence[Release],
    week_start: datetime,
    week_end: datetime,
    mobile_keywords: Sequence[str],
) -> List[Release]:
    keywords = tuple(word.lower() for word in mobile_keywords if word.strip())
    filtered: List[Release] = []

    for release in releases:
        if not release.date_value:
            continue
        if not (week_start <= release.date_value <= week_end):
            continue
        searchable_text = f"{release.description} {release.release_type}".lower()
        if keywords and not any(word in searchable_text for word in keywords):
            continue
        filtered.append(release)

    return sorted(filtered, key=lambda item: item.date_value, reverse=True)


class PptxUpdater:
    def __init__(
        self,
        template_path: Path,
        output_path: Path,
        slide_index: int,
        marker_text: str,
    ) -> None:
        self.template_path = template_path
        self.output_path = output_path
        self.slide_index = slide_index
        self.marker_text = marker_text

    def _find_target_shape(self, slide):
        for shape in slide.shapes:
            if not getattr(shape, "has_text_frame", False):
                continue
            text_value = shape.text or ""
            if self.marker_text in text_value:
                return shape
        raise RuntimeError(
            f"Не удалось найти текстовый блок с маркером '{self.marker_text}'. "
            "Проверьте PPTX_RELEASES_MARKER в .env."
        )

    def _render_lines(self, releases: Sequence[Release]) -> List[str]:
        header = "Мобильные приложения (RN, Android, iOS):"
        lines = [header, "--"]
        if not releases:
            lines.append("Релизов не найдено")
            return lines
        lines.extend([f"- {item.formatted_line()}" for item in releases])
        return lines

    def update_first_slide(self, releases: Sequence[Release]) -> None:
        prs = Presentation(str(self.template_path))
        if self.slide_index < 0 or self.slide_index >= len(prs.slides):
            raise RuntimeError(f"Некорректный номер слайда: {self.slide_index + 1}")

        slide = prs.slides[self.slide_index]
        target_shape = self._find_target_shape(slide)
        text_frame = target_shape.text_frame
        lines = self._render_lines(releases)

        text_frame.clear()
        for idx, line in enumerate(lines):
            paragraph = text_frame.paragraphs[0] if idx == 0 else text_frame.add_paragraph()
            paragraph.text = line
            paragraph.level = 0
            paragraph.font.size = Pt(12 if idx >= 2 else 14)

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        prs.save(str(self.output_path))


def parse_csv_env(name: str, default_values: Sequence[str]) -> List[str]:
    value = os.getenv(name, "")
    if not value.strip():
        return list(default_values)
    return [part.strip() for part in value.split(",") if part.strip()]


def main() -> int:
    confluence_token = os.getenv("CONFLUENCE_TOKEN")
    if not confluence_token:
        logger.error("CONFLUENCE_TOKEN не задан.")
        return 1

    template_pptx = Path(os.getenv("PPTX_TEMPLATE_PATH", "/Users/asklimenko/Downloads/ОС ЦРФК 20.02.pptx"))
    output_pptx = Path(
        os.getenv(
            "PPTX_OUTPUT_PATH",
            str(template_pptx.with_name(f"{template_pptx.stem} (авто).pptx")),
        )
    )
    if not template_pptx.exists():
        logger.error("Шаблон презентации не найден: %s", template_pptx)
        return 1

    confluence_client = ConfluenceClient(
        base_url=os.getenv("CONFLUENCE_URL", "https://confluence.sberbank.ru"),
        token=confluence_token,
        verify_ssl=os.getenv("CONFLUENCE_VERIFY_SSL", "false").lower() == "true",
    )

    source_page_id = os.getenv("SOURCE_PAGE_ID", "18588013525")
    allowed_statuses = parse_csv_env("RELEASE_STATUSES", DEFAULT_STATUSES)
    mobile_keywords = parse_csv_env("MOBILE_RELEASE_KEYWORDS", DEFAULT_MOBILE_KEYWORDS)
    slide_index = int(os.getenv("PPTX_SLIDE_INDEX", "1")) - 1
    marker_text = os.getenv("PPTX_RELEASES_MARKER", "Релизов не найдено")

    logger.info("Загружаем релизы из Confluence pageId=%s", source_page_id)
    html_content = confluence_client.get_page_html(source_page_id)
    parser = ReleaseParser(allowed_statuses=allowed_statuses)
    all_releases = parser.parse_html_table(html_content)
    logger.info("Найдено релизов по статусам: %d", len(all_releases))

    week_start, week_end = current_week_range()
    logger.info("Анализируем период: %s - %s", week_start.strftime("%d.%m.%Y"), week_end.strftime("%d.%m.%Y"))
    weekly_mobile_releases = filter_releases(all_releases, week_start, week_end, mobile_keywords)
    logger.info("Мобильных релизов за неделю: %d", len(weekly_mobile_releases))

    updater = PptxUpdater(
        template_path=template_pptx,
        output_path=output_pptx,
        slide_index=slide_index,
        marker_text=marker_text,
    )
    updater.update_first_slide(weekly_mobile_releases)
    logger.info("Презентация обновлена: %s", output_pptx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
