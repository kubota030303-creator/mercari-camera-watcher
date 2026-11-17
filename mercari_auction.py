"""
mercari_auction.py
mercapi の DPoP 署名だけ再利用し、オークション絞り込み検索を自前で投げて
auction フィールド(bidDeadline/totalBid/...)を読む。
「今日(JST)終了 × 入札あり」を抽出して返す。
カテゴリ・価格は呼び出し側から指定可（複数カテゴリはリストで一括指定）。
"""

import asyncio
import uuid
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from httpx import Request
from mercapi import Mercapi

logger = logging.getLogger(__name__)
JST = ZoneInfo("Asia/Tokyo")

SEARCH_URL = "https://api.mercari.jp/v2/entities:search"

# オークション attribute（キャプチャした searchCondition より）
AUCTION_ATTRIBUTE = {
    "id": "d664efe3-ae5a-4824-b729-e789bf93aba9",
    "values": [
        "3b6eac8c-7be5-4c9c-b537-7c05cd3c4905",  # 通常オークション
        "dd317554-b1ba-40a1-b9b5-475238c0765e",  # 3時間オークション
    ],
}

CAMERA_TOP_CATEGORY = 97  # 既定（カメラ・光学機器）


def _build_search_body(page_token: str, category_ids: list,
                       price_min: int, price_max: int) -> dict:
    return {
        "userId": "",
        "pageSize": 120,
        "pageToken": page_token,
        "searchSessionId": uuid.uuid4().hex,
        "indexRouting": "INDEX_ROUTING_UNSPECIFIED",
        "thumbnailTypes": [],
        "searchCondition": {
            "keyword": "",
            "excludeKeyword": "",
            "sort": "SORT_CREATED_TIME",
            "order": "ORDER_DESC",
            "status": ["STATUS_ON_SALE"],
            "sizeId": [],
            "categoryId": list(category_ids),   # 複数カテゴリはここにまとめて指定
            "brandId": [],
            "sellerId": [],
            "priceMin": price_min,
            "priceMax": price_max,
            "itemConditionId": [],
            "shippingPayerId": [],
            "shippingFromArea": [],
            "shippingMethod": [],
            "colorId": [],
            "hasCoupon": False,
            "attributes": [AUCTION_ATTRIBUTE],
            "itemTypes": [],
            "skuIds": [],
            "shopIds": [],
            "excludeShippingMethodIds": [],
        },
        "serviceFrom": "suruga",
        "withItemBrand": True,
        "withItemPromotions": True,
        "withItemSizes": True,
        "useDynamicAttribute": True,
        "withAuction": True,   # これが無いと auction フィールド(bidDeadline/totalBid)が返らない
    }


def _signed_request(m: Mercapi, body: dict) -> Request:
    req = Request("POST", SEARCH_URL, json=body, headers=m._headers)
    return m._sign_request(req)  # mercapi の DPoP 署名を再利用


async def fetch_auction_items_async(category_ids: list, price_min: int,
                                    price_max: int, max_pages: int = 8) -> list[dict]:
    """オークション商品を全ページ取得し、生の item dict のリストを返す。"""
    m = Mercapi()
    items: list[dict] = []
    page_token = ""
    for page in range(max_pages):
        body = _build_search_body(page_token, category_ids, price_min, price_max)
        res = await m._client.send(_signed_request(m, body))
        res.raise_for_status()
        data = res.json()
        batch = data.get("items", [])
        items.extend(batch)
        page_token = (data.get("meta") or {}).get("nextPageToken", "")
        logger.info(f"page {page+1}: {len(batch)} 件 (累計 {len(items)})")
        if not page_token or not batch:
            break
    return items


def _parse_deadline(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(JST)


def pick_today_with_bids(items: list[dict], today=None, price_max: int = 0) -> list[dict]:
    """今日(JST)終了 × 入札あり を抽出し、終了が早い順に並べる。
    price_max>0 のとき、現在価格がそれ以上の商品は除外（保険の二重フィルタ）。"""
    if today is None:
        today = datetime.now(JST).date()
    out = []
    for it in items:
        auc = it.get("auction")
        if not auc:
            continue
        total_bid = int(auc.get("totalBid", "0") or 0)
        if total_bid < 1:                      # 入札なし(終了時刻はダミー)を除外
            continue
        deadline = _parse_deadline(auc["bidDeadline"])
        if deadline.date() != today:           # 今日終了以外を除外
            continue
        highest = int(auc.get("highestBid", it.get("price", 0)) or 0)
        if price_max > 0 and highest >= price_max:   # 上限以上は除外
            continue
        initial = int(auc.get("initialPrice", 0) or 0)
        out.append({
            "id": it["id"],
            "name": it.get("name", ""),
            "deadline_jst": deadline,
            "total_bid": total_bid,
            "highest_bid": highest,
            "initial_price": initial,
            "ratio": round(highest / initial, 2) if initial else None,  # 値上がり率
            "brand": (it.get("itemBrand") or {}).get("name", "") if it.get("itemBrand") else "",
            "category_id": str(it.get("categoryId", "")),
            "condition_id": str(it.get("itemConditionId", "")),
            "url": f"https://jp.mercari.com/item/{it['id']}",
        })
    out.sort(key=lambda x: x["deadline_jst"])
    return out


def get_today_auction_list(category_ids=None, price_min: int = 5000,
                           price_max: int = 0, max_pages: int = 8,
                           today=None) -> list[dict]:
    """今日終了×入札あり を取得。category_ids 未指定ならカメラ(97)。"""
    if not category_ids:
        category_ids = [CAMERA_TOP_CATEGORY]
    items = asyncio.run(fetch_auction_items_async(category_ids, price_min, price_max, max_pages))
    return pick_today_with_bids(items, today, price_max)
