"""
yahoo_auction.py
ヤフオク「カメラ・光学機器」の「今日・夜の激戦前(21時)までに終了 × 入札あり」を取得する。
検索結果ページHTMLに埋め込まれた __NEXT_DATA__(JSON) をパースするため、
DOMのクラス名変更に強い。ログイン不要の公開ページのみを対象とする。

※ robots非推奨のためアクセスは低頻度（1日数回）に限定する前提。
"""

import re
import json
import time
import math
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)
JST = ZoneInfo("Asia/Tokyo")

CATEGORY_ID = 23636          # カメラ・光学機器
PEAK_HOUR = 21               # これ以降は激戦帯（除外はせず、色で区別するだけ）
PAGE_SIZE = 100
MAX_PAGES = 12               # 取りこぼし防止（最大1200件）
REQUEST_INTERVAL = 1.5       # ページ間の待機秒

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9",
}

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)


def _cutoff(now: datetime) -> datetime:
    """今日の終わり（23:59:59）まで。夜の分も含めて当日分を全部拾う。"""
    return now.replace(hour=23, minute=59, second=59, microsecond=0)


def _build_url(begin: int, auchours: int) -> str:
    # fixed=0: オークションのみ / mode=3: 入札多い順 / auchours: 終了までの時間
    return (f"https://auctions.yahoo.co.jp/category/list/{CATEGORY_ID}"
            f"?fixed=0&is_postage_mode=1&n={PAGE_SIZE}&b={begin}"
            f"&select=3&mode=3&auchours={auchours}")


def _extract_items(html: str) -> list[dict]:
    m = NEXT_DATA_RE.search(html)
    if not m:
        raise ValueError("__NEXT_DATA__ が見つかりません（構造変更またはブロックの可能性）")
    data = json.loads(m.group(1))
    return (data["props"]["pageProps"]["initialState"]
            ["search"]["items"]["listing"]["items"])


def fetch_raw_items(auchours: int, max_pages: int = MAX_PAGES) -> list[dict]:
    all_items: list[dict] = []
    for page in range(max_pages):
        begin = page * PAGE_SIZE + 1
        resp = requests.get(_build_url(begin, auchours), headers=HEADERS, timeout=30)
        resp.raise_for_status()
        batch = _extract_items(resp.text)
        all_items.extend(batch)
        logger.info(f"page {page+1}: {len(batch)} 件 (累計 {len(all_items)})")
        if len(batch) < PAGE_SIZE:
            break
        time.sleep(REQUEST_INTERVAL)
    return all_items


def _parse_rating(s):
    """goodRating '99.9%' → 0.999（float）。取れなければ None。"""
    if not s:
        return None
    try:
        return round(float(str(s).strip().rstrip("%")) / 100, 4)
    except ValueError:
        return None


def parse_items(raw: list[dict], cutoff: datetime) -> list[dict]:
    """入札あり かつ cutoff までに終了 の商品を、終了が早い順で返す。"""
    out = []
    for it in raw:
        if int(it.get("bidCount", 0) or 0) < 1:
            continue
        end = datetime.fromisoformat(it["endTime"]).astimezone(JST)
        if end > cutoff:
            continue
        seller = it.get("seller") or {}
        out.append({
            "end_jst": end,
            "is_peak": end.hour >= PEAK_HOUR,   # 21時以降＝激戦帯
            "bids": int(it.get("bidCount", 0) or 0),
            "price": int(it.get("price", 0) or 0),
            "buynow": it.get("buyNowPrice"),
            "condition": it.get("itemCondition", ""),
            "rating": _parse_rating(seller.get("goodRating")),  # 良い評価率(0-1)
            "is_store": bool(seller.get("isStore")),            # ストア出品か
            "seller_uid": seller.get("userId", ""),             # 出品者固有ID(名前は非公開)
            "title": it.get("title", ""),
            "id": it["auctionId"],
            "url": f"https://page.auctions.yahoo.co.jp/jp/auction/{it['auctionId']}",
        })
    out.sort(key=lambda x: x["end_jst"])   # 終了が早い順（＝狙い目帯が上に集まる）
    return out


def get_todays_auctions() -> list[dict]:
    """今日中に終了 × 入札あり を全部取得（終了が早い順）。"""
    now = datetime.now(JST)
    cutoff = _cutoff(now)
    hours = max(1, math.ceil((cutoff - now).total_seconds() / 3600))
    logger.info(f"取得窓: 〜{cutoff.strftime('%H:%M')} (auchours={hours})")
    raw = fetch_raw_items(hours)
    return parse_items(raw, cutoff)
