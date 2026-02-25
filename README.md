# Blast Agent

Production-ready release automation assistant.

## Features

- Jira release operations (link / cleanup / remove links)
- LT check for release-linked issues
- RQG checks for nested stories (CO/IFT/distribution)
- Master branch analysis (PR -> services)
- Deploy plan generation in Confluence
- Business requirements (BT/FR) generation
- AI assistant with release pipeline orchestration
- Manual and AI-based release status transitions

## Project layout

- `main.py` — app entrypoint
- `ui.py` — UI module
- `service.py`, `lt.py`, `master_analyzer.py`, `bt3.py`, `arch.py`, `history.py`, `config.py` — runtime modules

## Quick start

1. Create virtual environment and install deps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure environment:

Create `.env` in project root with at least:

```env
JIRA_URL=https://jira.sberbank.ru
JIRA_TOKEN=...
CONFLUENCE_URL=https://confluence.sberbank.ru
CONFLUENCE_TOKEN=...
CONFLUENCE_SPACE_KEY=HRTECH
CONFLUENCE_PARENT_PAGE_TITLE=deploy plan 2k
CONFLUENCE_TEMPLATE_PAGE_ID=18532011154
TEAM_NAME=Команда
GIGACHAT_USERNAME=...
GIGACHAT_PASSWORD=...

# Optional RQG tuning
RQG_CO_KEYWORDS=цо,co
RQG_IFT_KEYWORDS=ифт,ift
RQG_DISTRIBUTION_KEYWORDS=дистриб,distrib,release-notes,install
RQG_CO_ALLOWED_STATUSES=Done,Closed,Resolved,Выполнено,Закрыто
RQG_IFT_ALLOWED_STATUSES=Done,Closed,Resolved,Выполнено,Закрыто
RQG_DISTRIBUTION_ALLOWED_STATUSES=Done,Closed,Resolved,Выполнено,Закрыто
RQG_TRANSITION_NAME=RQG
```

3. Run app:

```bash
python3 main.py
```

## Notes

- The project now runs entirely from `.py` sources.
- Legacy `.txt` code snapshots have been removed.
