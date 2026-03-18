import re
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime

import requests

from util import DB_PATH, ProcessStatus, init_logger

logger = init_logger(__name__)

RSS_URL = "https://dedicated.wallstreetcn.com/rss.xml"
KEYWORD = "华尔街见闻早餐"
CHANNEL_NAME = "華爾街見聞"
DOMAIN = "redirect"


def _parse_rss_datetime(pub_date: str) -> str:
    if not pub_date:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        dt = parsedate_to_datetime(pub_date)
        if dt.tzinfo:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_breakfast_item_from_rss():
    """取得最新一篇華爾街見聞早餐（標題/內文/link/article_id/pubDate）。"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    resp = requests.get(RSS_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns_content = "{http://purl.org/rss/1.0/modules/content/}"

    for item in root.findall(".//item"):
        title_elem = item.find("title")
        link_elem = item.find("link")
        pub_elem = item.find("pubDate")
        desc_elem = item.find("description")
        content_elem = item.find(f"{ns_content}encoded")

        title = (title_elem.text or "").strip() if title_elem is not None else ""
        link = (link_elem.text or "").strip() if link_elem is not None else ""

        if not title or KEYWORD not in title:
            continue

        match = re.search(r"/articles/(\d+)", link)
        article_id = match.group(1) if match else None
        if not article_id:
            continue

        pub_date = (pub_elem.text or "").strip() if pub_elem is not None else ""
        created_at = _parse_rss_datetime(pub_date)

        info = ""
        if content_elem is not None and content_elem.text:
            info = content_elem.text
        elif desc_elem is not None and desc_elem.text:
            info = desc_elem.text

        return {
            "article_id": article_id,
            "title": title,
            "info": info,
            "created_at": created_at,
            "link": link,
        }

    return None


def get_wallstreet_breakfast_audio(article_id: str):
    """用文章 API 取得 audio_uri（MP3 連結）。"""
    api_url = (
        f"https://api-one-wscn.awtmt.com/apiv1/content/articles/{article_id}"
        "?extract=0&accept_theme=theme%2Cpremium-theme"
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://wallstreetcn.com/",
    }

    resp = requests.get(api_url, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    article_data = data.get("data") or {}
    return article_data.get("audio_uri")


def write_to_db(mp3_url: str, title: str, info: str, created_at: str):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)

    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT OR IGNORE INTO podcast
            (video_id, domain, channel_name, title_name, info, created_at, updated_at, process_status, format)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                mp3_url,  # video_id = MP3 連結
                DOMAIN,
                CHANNEL_NAME,
                title,
                info or "",
                created_at,
                now,
                ProcessStatus.UPLOAD_PODSCAST_OK.value,
                'mp3',
            ),
        )
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def main():
    try:
        item = get_breakfast_item_from_rss()
        if not item:
            logger.info("RSS 找不到早餐文章")
            return 0

        mp3_url = get_wallstreet_breakfast_audio(item["article_id"])
        if not mp3_url:
            logger.error("未能取得 MP3 連結")
            return 1

        inserted = write_to_db(
            mp3_url=mp3_url,
            title=item["title"],
            info="",
            created_at=item["created_at"],
        )

        if inserted:
            logger.info(f"✓ 已寫入 DB: {item['title']}")
        else:
            logger.info(f"已存在，跳過: {item['title']}")

        return 0

    except Exception as e:
        logger.error(f"執行失敗: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
