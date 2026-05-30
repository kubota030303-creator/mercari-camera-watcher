"""
mercari_auction.py
mercapi の DPoP 署名だけ再利用し、オークション絞り込み検索を自前で投げて
auction フィールド(bidDeadline/totalBid/...)を読む。
「今日(JST)終了 × 入札あり」を抽出して返す。
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
# ※ リクエスト側の正確な形は DevTools の Request payload で要確認（下記は response 由来の推定）
AUCTION_ATTRIBUTE = {
    "id": "d664efe3-ae5a-4824-b729-e789bf93aba9",
    "values": [
        "3b6eac8c-7be5-4c9c-b537-7c05cd3c4905",  # 通常オークション
        "dd317554-b1ba-40a1-b9b5-475238c0765e",  # 3時間オークション
    ],
}

CAMERA_TOP_CATEGORY = 97  # カメラ・光学機器（キャプチャの categoryId と一致）


def _build_search_body(page_token: str = "") -> dict:
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
            "categoryId": [CAMERA_TOP_CATEGORY],
            "brandId": [],
            "sellerId": [],
            "priceMin": 5000,
            "priceMax": 0,
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
        "withAuction": True,   # ★ これが無いと auction フィールド(bidDeadline/totalBid)が返らない
    }


def _signed_request(m: Mercapi, body: dict) -> Request:
    req = Request("POST", SEARCH_URL, json=body, headers=m._headers)
    return m._sign_request(req)  # mercapi の DPoP 署名を再利用


async def fetch_auction_items_async(max_pages: int = 8) -> list[dict]:
    """オークション商品を全ページ取得し、生の item dict のリストを返す。"""
    m = Mercapi()
    items: list[dict] = []
    page_token = ""
    for page in range(max_pages):
        body = _build_search_body(page_token)
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


def pick_today_with_bids(items: list[dict], today=None) -> list[dict]:
    """今日(JST)終了 × 入札あり を抽出し、終了が早い順に並べる。"""
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


def get_today_auction_list(max_pages: int = 8, today=None) -> list[dict]:
    items = asyncio.run(fetch_auction_items_async(max_pages))
    return pick_today_with_bids(items, today)
