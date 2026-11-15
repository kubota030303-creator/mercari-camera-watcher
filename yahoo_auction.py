"""
yahoo_auction.py
ヤフオク「カメラ・光学機器」カテゴリの終了間近オークションを取得する。
検索結果ページHTMLに埋め込まれた __NEXT_DATA__(JSON) をパースするため、
DOMのクラス名変更に強い。ログイン不要の公開ページのみを対象とする。

※ robots非推奨のためアクセスは低頻度（1日数回）に限定する前提。
"""

import re
import json
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)
JST = ZoneInfo("Asia/Tokyo")

# カメラ・光学機器カテゴリ
CATEGORY_ID = 23636

# 取得窓（時間）。1=1時間以内終了, 2, 6, 24 など。
# 昼のチェックで夜終了分も拾い、入札予約を仕込めるよう既定は24時間。
AUCTION_HOURS = 24

PAGE_SIZE = 100          # 1ページ最大100件
MAX_PAGES = 3            # 低頻度・低負荷のため最大3ページ(300件)まで
REQUEST_INTERVAL = 2.0   # ページ間の待機秒

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/125.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9",
}

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)


def _build_url(begin: int) -> str:
    # fixed=0: オークションのみ / mode=3: 入札多い順 / auchours: 終了までの時間
    return (f"https://auctions.yahoo.co.jp/category/list/{CATEGORY_ID}"
            f"?fixed=0&is_postage_mode=1&n={PAGE_SIZE}&b={begin}"
            f"&select=3&mode=3&auchours={AUCTION_HOURS}")


def _extract_items(html: str) -> list[dict]:
    m = NEXT_DATA_RE.search(html)
    if not m:
        raise ValueError("__NEXT_DATA__ が見つかりません（ページ構造変更またはブロックの可能性）")
    data = json.loads(m.group(1))
    return (data["props"]["pageProps"]["initialState"]
            ["search"]["items"]["listing"]["items"])


def fetch_raw_items(max_pages: int = MAX_PAGES) -> list[dict]:
    """ヤフオクの終了間近オークションを取得して生の item dict を返す。"""
    all_items: list[dict] = []
    for page in range(max_pages):
        begin = page * PAGE_SIZE + 1
        url = _build_url(begin)
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        batch = _extract_items(resp.text)
        all_items.extend(batch)
        logger.info(f"page {page+1}: {len(batch)} 件 (累計 {len(all_items)})")
        if len(batch) < PAGE_SIZE:
            break  # 最終ページ
        time.sleep(REQUEST_INTERVAL)
    return all_items


def parse_items(raw: list[dict]) -> list[dict]:
    """入札あり商品を終了が早い順に整形して返す。"""
    out = []
    for it in raw:
        if int(it.get("bidCount", 0) or 0) < 1:
            continue  # 入札なしは除外
        end = datetime.fromisoformat(it["endTime"]).astimezone(JST)
        price = int(it.get("price", 0) or 0)
        init = int(it.get("initPriceNoTax", 0) or 0)
        out.append({
            "end_jst": end,
            "bids": int(it.get("bidCount", 0) or 0),
            "price": price,
            "buynow": it.get("buyNowPrice"),
            "init_price": init,
            "ratio": round(price / init, 1) if init else None,
            "condition": it.get("itemCondition", ""),
            "title": it.get("title", ""),
            "id": it["auctionId"],
            "url": f"https://page.auctions.yahoo.co.jp/jp/auction/{it['auctionId']}",
        })
    out.sort(key=lambda x: x["end_jst"])
    return out


def get_ending_auctions(max_pages: int = MAX_PAGES) -> list[dict]:
    return parse_items(fetch_raw_items(max_pages))
