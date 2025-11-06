import os
import re
import sqlite3
import smtplib
import hashlib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Dict, List, Iterable

import requests
import feedparser
from bs4 import BeautifulSoup
import yaml

DB_PATH = os.environ.get("JOBWATCH_DB", "jobwatch.db")
CONFIG_PATH = os.environ.get("JOBWATCH_CONFIG", "config.yaml")
USER_AGENT = os.environ.get("JOBWATCH_UA", "JobWatcherBot/2.0 (+respect-robots)")


# --------------------------- Helpers --------------------------- #

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

def normspace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


# --------------------------- Persistence --------------------------- #

def ensure_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            id TEXT PRIMARY KEY,
            title TEXT,
            url TEXT,
            source TEXT,
            first_seen_utc TEXT
        )
        """
    )
    conn.commit()
    return conn

def is_seen(conn: sqlite3.Connection, key: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen WHERE id=?", (key,))
    return cur.fetchone() is not None

def mark_seen(conn: sqlite3.Connection, key: str, title: str, url: str, source: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen(id, title, url, source, first_seen_utc) VALUES(?,?,?,?,?)",
        (key, title, url, source, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


# --------------------------- Fetchers --------------------------- #

def fetch_rss(url: str):
    d = feedparser.parse(url)
    for e in d.entries:
        title = normspace(getattr(e, "title", ""))
        link = getattr(e, "link", "")
        summary = normspace(getattr(e, "summary", ""))
        published = getattr(e, "published", "")
        yield {
            "title": title,
            "url": link,
            "summary": summary,
            "published": published,
            "source": url,
        }

def fetch_html(url: str, item_selector: str, title_selector: str, url_selector: str):
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for item in soup.select(item_selector):
        title_el = item.select_one(title_selector)
        url_el = item.select_one(url_selector)
        if not title_el or not url_el:
            continue
        title = normspace(title_el.get_text(" "))
        href = url_el.get("href", "")
        if href and href.startswith("/"):
            from urllib.parse import urljoin
            href = urljoin(url, href)
        yield {
            "title": title,
            "url": href,
            "summary": "",
            "published": "",
            "source": url,
        }

def fetch_provider(provider: Dict):
    ptype = provider.get("type")
    if ptype == "rss":
        yield from fetch_rss(provider["url"])
    elif ptype == "html":
        yield from fetch_html(
            provider["url"],
            provider["item_selector"],
            provider["title_selector"],
            provider["url_selector"],
        )
    elif ptype == "greenhouse":
        api = provider["url"]
        headers = {"User-Agent": USER_AGENT}
        data = requests.get(api, headers=headers, timeout=30).json()
        for job in data.get("jobs", []):
            yield {
                "title": normspace(job.get("title", "")),
                "url": job.get("absolute_url", ""),
                "summary": normspace(job.get("content", "")),
                "published": job.get("updated_at", ""),
                "source": api,
            }
    elif ptype == "lever":
        api = provider["url"]
        headers = {"User-Agent": USER_AGENT}
        data = requests.get(api, headers=headers, timeout=30).json()
        for job in data:
            yield {
                "title": normspace(job.get("text", "")),
                "url": job.get("hostedUrl", ""),
                "summary": normspace(job.get("descriptionPlain", "")),
                "published": job.get("createdAt", ""),
                "source": api,
            }
    else:
        raise ValueError(f"Unknown provider type: {ptype}")


# --------------------------- Filtering --------------------------- #

def _lower_list(values):
    return [v.lower() for v in values] if values else []

def matches_filters(job: Dict, cfg: Dict) -> bool:
    """
        New logic:
            - If `level_keywords` or `skill_keywords` exist, require:
                (at least one level) AND (at least one skill)
            - If they do not exist, fall back to the original OR logic using `include_keywords`.
            - Always exclude anything matching `exclude_keywords`.
            - Locations are optional (OR condition).
    """
    text = f"{job['title']}\n{job['summary']}".lower()
    inc = _lower_list(cfg.get("include_keywords"))
    exc = _lower_list(cfg.get("exclude_keywords"))
    levels = _lower_list(cfg.get("level_keywords"))
    skills = _lower_list(cfg.get("skill_keywords"))
    locations = _lower_list(cfg.get("locations"))

    # Exclude
    if any(k in text for k in exc):
        return False

    # AND: level & skill
    if levels or skills:
        if levels and not any(k in text for k in levels):
            return False
        if skills and not any(k in text for k in skills):
            return False
    else:
        # fallback: include (OR)
        if inc and not any(k in text for k in inc):
            return False

    # Lokace
    if locations:
        if not any(loc in text or loc in (job["url"] or "").lower() for loc in locations):
            return False

    return True


# --------------------------- Email --------------------------- #

def send_email(subject: str, html_body: str, cfg: Dict) -> None:
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    to_addr = cfg["notify_email"]
    from_addr = cfg.get("from_email", smtp_user or to_addr)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.starttls()
        if smtp_user and smtp_pass:
            s.login(smtp_user, smtp_pass)
        s.sendmail(from_addr, [to_addr], msg.as_string())


# --------------------------- Config --------------------------- #

CONFIG_TEMPLATE = """
# ===== Job Watcher (level AND skill) minimal template =====
notify_email: "you@example.com"
from_email: "job-watcher@example.com"

# Pokud použiješ level/skill, include_keywords můžeš nechat prázdné
include_keywords: []

exclude_keywords: ["senior", "staff", "principal", "lead", "manager", "unpaid", "volunteer"]

level_keywords: ["co-op", "co op", "coop", "intern", "internship", "entry level", "entry-level", "junior", "jr ", "medior"]

skill_keywords: ["react", "vue", "vue.js", "javascript", "typescript", "python", "frontend",
                 "cyber security", "information security", "soc", "security analyst", "security engineer",
                 "siem", "incident response"]

locations: ["canada", "vancouver", "british columbia", "remote", "hybrid", "burnaby", "toronto"]

providers:
  - type: rss
    url: "https://remoteok.com/remote-dev+security+frontend+python-jobs.rss"
"""

def load_config(path: str) -> Dict:
    env_cfg = os.environ.get("CONFIG_YAML")
    if env_cfg and not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(env_cfg)

    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(CONFIG_TEMPLATE)
        raise FileNotFoundError(f"Config file not found. A template was created at {path}. Fill it and rerun.")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# --------------------------- Digest --------------------------- #

def build_digest_html(jobs: List[Dict]) -> str:
    if not jobs:
        return "<p>No new matching jobs.</p>"
    items = []
    for j in jobs:
        items.append(
            f"<li><a href='{j['url']}'>{j['title']}</a>"
            f"<br><small>Source: {j['source']}</small></li>"
        )
    return "<h3>New matching jobs</h3><ul>" + "\n".join(items) + "</ul>"


# --------------------------- Main --------------------------- #

def main():
    cfg = load_config(CONFIG_PATH)
    conn = ensure_db()

    new_matches: List[Dict] = []

    for provider in cfg.get("providers", []):
        try:
            for job in fetch_provider(provider):
                key = sha1(job["url"] or job["title"])
                if is_seen(conn, key):
                    continue
                if matches_filters(job, cfg):
                    new_matches.append(job)
                mark_seen(conn, key, job["title"], job["url"], job["source"])
                time.sleep(0.1)
        except Exception as e:
            print(f"Provider error: {provider.get('url', provider.get('type'))}: {e}")

    if new_matches:
        html = build_digest_html(new_matches)
        send_email(subject=f"Job Watcher: {len(new_matches)} new match(es)", html_body=html, cfg=cfg)
        print(f"Emailed {len(new_matches)} new jobs to {cfg['notify_email']}")