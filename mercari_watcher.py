"""
mercari_watcher.py
メルカリ新着カメラ商品を取得し、前段除外フィルタを通してn8n Webhookに送信する。
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials

# ──────────────────────────────────────────
# ログ設定
# ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────
# 定数
# ──────────────────────────────────────────
MERCARI_SEARCH_URL = "https://api.mercari.jp/v2/entities:search"

# メルカリ カメラ関連カテゴリID
# 参考：カメラ全般 = 723 ～ 各サブカテゴリ
CAMERA_CATEGORY_IDS = {
    "723",   # カメラ・スマホ・家電 > カメラ
    "724",   # フィルムカメラ
    "725",   # デジタルカメラ
    "726",   # ミラーレス一眼
    "727",   # 一眼レフ
    "728",   # コンパクトデジカメ
    "730",   # 交換レンズ
    "731",   # 三脚・一脚
    "732",   # 撮影用品・アクセサリ
    "733",   # ビデオカメラ
    "1050",  # カメラ（その他）
}

NG_KEYWORDS = ["ジャンク", "故障", "部品取り"]

SPREADSHEET_NAME = "仕入れマスター_統合版_v2_v8"
BLOCKLIST_SHEET = "除外リスト"
SENT_SHEET = "送信済みリスト"

SENT_RETENTION_HOURS = 24  # 送信済みリストの保持時間


# ──────────────────────────────────────────
# Google Sheets 認証
# ──────────────────────────────────────────
def get_gspread_client() -> gspread.Client:
    """GOOGLE_CREDENTIALS 環境変数（JSON文字列）からgspreadクライアントを返す。"""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise EnvironmentError("GOOGLE_CREDENTIALS が設定されていません。")

    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


# ──────────────────────────────────────────
# ブラックリスト取得
# ──────────────────────────────────────────
def fetch_blocked_seller_ids(client: gspread.Client) -> set[str]:
    """
    スプレッドシートの「除外リスト」シートから seller_id を取得する。
    seller_id 列が存在しない場合は空セットを返す（安全フォールバック）。
    """
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(BLOCKLIST_SHEET)
        records = sheet.get_all_records()
        blocked = set()
        for row in records:
            sid = str(row.get("seller_id", "")).strip()
            if sid:
                blocked.add(sid)
        logger.info(f"ブラックリスト取得完了：{len(blocked)} 件")
        return blocked
    except Exception as e:
        logger.warning(f"ブラックリスト取得失敗（空セットで継続）: {e}")
        return set()


# ──────────────────────────────────────────
# 送信済みリスト管理
# ──────────────────────────────────────────
def fetch_already_sent(client: gspread.Client) -> set[str]:
    """
    「送信済みリスト」シートから直近 SENT_RETENTION_HOURS 以内の item_id を返す。
    シートが存在しない場合は自動作成する。
    """
    try:
        ss = client.open(SPREADSHEET_NAME)
        try:
            sheet = ss.worksheet(SENT_SHEET)
        except gspread.WorksheetNotFound:
            sheet = ss.add_worksheet(title=SENT_SHEET, rows=1000, cols=2)
            sheet.append_row(["item_id", "sent_at"])
            logger.info(f"シート「{SENT_SHEET}」を新規作成しました。")
            return set()

        records = sheet.get_all_records()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=SENT_RETENTION_HOURS)
        valid_ids = set()
        for row in records:
            item_id = str(row.get("item_id", "")).strip()
            sent_at_str = str(row.get("sent_at", "")).strip()
            if not item_id:
                continue
            try:
                sent_at = datetime.fromisoformat(sent_at_str)
                if sent_at.tzinfo is None:
                    sent_at = sent_at.replace(tzinfo=timezone.utc)
                if sent_at >= cutoff:
                    valid_ids.add(item_id)
            except ValueError:
                # パース不能な行はスキップ
                valid_ids.add(item_id)
        logger.info(f"送信済みリスト取得完了：{len(valid_ids)} 件（{SENT_RETENTION_HOURS}h以内）")
        return valid_ids
    except Exception as e:
        logger.warning(f"送信済みリスト取得失敗（空セットで継続）: {e}")
        return set()


def record_sent_items(client: gspread.Client, item_ids: list[str]) -> None:
    """送信済み item_id をシートに記録し、24時間超の行を削除する。"""
    if not item_ids:
        return
    try:
        ss = client.open(SPREADSHEET_NAME)
        sheet = ss.worksheet(SENT_SHEET)
        now_str = datetime.now(timezone.utc).isoformat()
        rows_to_add = [[iid, now_str] for iid in item_ids]
        sheet.append_rows(rows_to_add, value_input_option="RAW")

        # 古い行を削除
        _cleanup_sent_sheet(sheet)
        logger.info(f"送信済みリスト記録完了：{len(item_ids)} 件追加")
    except Exception as e:
        logger.warning(f"送信済みリスト記録失敗: {e}")


def _cleanup_sent_sheet(sheet: gspread.Worksheet) -> None:
    """SENT_RETENTION_HOURS を超えた行を削除する。"""
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return
        header = all_values[0]
        cutoff = datetime.now(timezone.utc) - timedelta(hours=SENT_RETENTION_HOURS)
        rows_to_delete = []
        for i, row in enumerate(all_values[1:], start=2):  # 1-indexed, skip header
            try:
                sent_at_str = row[1] if len(row) > 1 else ""
                sent_at = datetime.fromisoformat(sent_at_str)
                if sent_at.tzinfo is None:
                    sent_at = sent_at.replace(tzinfo=timezone.utc)
                if sent_at < cutoff:
                    rows_to_delete.append(i)
            except Exception:
                pass

        # 後ろから削除（行番号ずれ防止）
        for row_num in reversed(rows_to_delete):
            sheet.delete_rows(row_num)
        if rows_to_delete:
            logger.info(f"送信済みリスト：{len(rows_to_delete)} 件の古い行を削除")
    except Exception as e:
        logger.warning(f"送信済みリスト クリーンアップ失敗: {e}")


# ──────────────────────────────────────────
# メルカリAPI
# ──────────────────────────────────────────
def fetch_mercari_items(keyword: str = "カメラ", limit: int = 50) -> list[dict]:
    """
    メルカリ内部APIから新着商品を取得する。
    認証不要・Cookieなし。
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MercariWatcher/1.0)",
        "X-Platform": "web",
        "Accept": "application/json",
        "Accept-Language": "ja-JP,ja;q=0.9",
    }
    payload = {
        "searchSessionId": "",
        "indexRouting": "INDEX_ROUTING_UNSPECIFIED",
        "searchCondition": {
            "keyword": keyword,
            "excludeKeyword": "",
            "sort": "SORT_CREATED_TIME",
            "order": "ORDER_DESC",
            "status": ["STATUS_ON_SALE"],
            "categoryId": [],
            "itemTypes": [],
            "skuIds": [],
            "itemConditionId": [],
            "shippingPayerId": [],
            "shippingFromArea": [],
            "priceMin": 5000,
            "priceMax": 0,
        },
        "defaultDatasets": ["DATASET_TYPE_MERCARI"],
        "serviceFrom": "suruga",
        "withItemBrand": True,
        "withItemSize": False,
        "withItemPromotions": False,
        "withRecommendedItems": False,
        "useDynamicAttribute": False,
        "pageSize": limit,
        "pageToken": "",
    }

    try:
        response = requests.post(
            MERCARI_SEARCH_URL,
            headers=headers,
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        raw_items = data.get("items", [])
        logger.info(f"メルカリAPI取得件数：{len(raw_items)} 件")
        return raw_items
    except requests.RequestException as e:
        logger.error(f"メルカリAPI取得エラー: {e}")
        return []


def parse_item(raw: dict) -> dict:
    """APIレスポンスの1件を必要フィールドに整形する。"""
    brand_info = raw.get("itemBrand") or {}
    return {
        "item_id": raw.get("id", ""),
        "title": raw.get("name", ""),
        "price": int(raw.get("price", 0)),
        "url": f"https://jp.mercari.com/item/{raw.get('id', '')}",
        "seller_id": raw.get("sellerId", ""),
        "category_id": str(raw.get("categoryId", "")),
        "status": raw.get("status", ""),
        "item_type": raw.get("itemType", ""),
        "brand": brand_info.get("name", ""),
        "item_condition_id": str(raw.get("itemConditionId", "")),
    }


# ──────────────────────────────────────────
# 前段除外フィルタ
# ──────────────────────────────────────────
def apply_filters(
    items: list[dict],
    blocked_seller_ids: set[str],
    already_sent: set[str],
) -> tuple[list[dict], dict]:
    """
    指示書の Step 1〜6 の順番で除外処理を行う。
    Returns:
        passed: フィルタ通過アイテムのリスト
        stats: 各ステップの除外件数
    """
    stats = {
        "total": len(items),
        "status_blocked": 0,
        "seller_blocked": 0,
        "price_blocked": 0,
        "ng_keyword_blocked": 0,
        "category_blocked": 0,
        "duplicate_blocked": 0,
        "passed": 0,
    }

    passed = []
    for item in items:
        # Step 1: 販売中以外を除外
        if item["status"] != "ITEM_STATUS_ON_SALE":
            stats["status_blocked"] += 1
            continue

        # Step 2: sellerIdでブラックリスト照合
        if item["seller_id"] in blocked_seller_ids:
            stats["seller_blocked"] += 1
            continue

        # Step 3: 価格フィルタ
        if item["price"] < 5000:
            stats["price_blocked"] += 1
            continue

        # Step 4: NGキーワード
        if any(ng in item["title"] for ng in NG_KEYWORDS):
            stats["ng_keyword_blocked"] += 1
            continue

        # Step 5: カテゴリフィルタ
        if item["category_id"] not in CAMERA_CATEGORY_IDS:
            stats["category_blocked"] += 1
            continue

        # Step 6: 重複除外
        if item["item_id"] in already_sent:
            stats["duplicate_blocked"] += 1
            continue

        passed.append(item)

    stats["passed"] = len(passed)
    return passed, stats


# ──────────────────────────────────────────
# n8n Webhook送信
# ──────────────────────────────────────────
def send_to_n8n(items: list[dict]) -> int:
    """
    フィルタ通過商品をn8n Webhookに送信する。
    既存フローとの整合フォーマットで送信。
    Returns: 送信成功件数
    """
    webhook_url = os.environ.get("N8N_WEBHOOK_URL")
    if not webhook_url:
        logger.error("N8N_WEBHOOK_URL が設定されていません。送信をスキップします。")
        return 0

    success_count = 0
    for item in items:
        payload = {
            "id": item["item_id"],
            "name": item["title"],
            "price": item["price"],
            "url": item["url"],
            "sellerId": item["seller_id"],
            "itemConditionId": item["item_condition_id"],
        }
        try:
            resp = requests.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            success_count += 1
            logger.debug(f"送信OK: {item['item_id']} / {item['title'][:30]}")
        except requests.RequestException as e:
            logger.warning(f"送信失敗 [{item['item_id']}]: {e}")
        time.sleep(0.3)  # 連続送信の間隔
    return success_count


# ──────────────────────────────────────────
# メイン
# ──────────────────────────────────────────
def main():
    logger.info("===== Mercari Watcher 起動 =====")

    # Google Sheets クライアント
    try:
        gc = get_gspread_client()
    except Exception as e:
        logger.error(f"Google Sheets 認証失敗: {e}")
        return

    # ブラックリスト・送信済みリストを取得
    blocked_seller_ids = fetch_blocked_seller_ids(gc)
    already_sent = fetch_already_sent(gc)

    # メルカリから新着取得
    raw_items = fetch_mercari_items(keyword="カメラ", limit=50)
    if not raw_items:
        logger.warning("取得件数0件。終了します。")
        return

    # パース
    items = [parse_item(r) for r in raw_items]

    # 前段フィルタ適用
    passed, stats = apply_filters(items, blocked_seller_ids, already_sent)

    # ──── ログ出力（成果物⑤）────
    logger.info(f"取得件数　　　：{stats['total']} 件")
    logger.info(f"販売中以外除外：{stats['status_blocked']} 件")
    logger.info(f"sellerId除外　：{stats['seller_blocked']} 件")
    logger.info(f"価格除外　　　：{stats['price_blocked']} 件")
    logger.info(f"NGキーワード除外：{stats['ng_keyword_blocked']} 件")
    logger.info(f"カテゴリ除外　：{stats['category_blocked']} 件")
    logger.info(f"重複除外　　　：{stats['duplicate_blocked']} 件")
    logger.info(f"pass件数　　　：{stats['passed']} 件")

    if not passed:
        logger.info("通知対象なし。終了します。")
        return

    # n8nへ送信
    sent_count = send_to_n8n(passed)
    logger.info(f"n8n送信　　　：{sent_count} 件")

    # 送信済みリストに記録
    sent_ids = [item["item_id"] for item in passed[:sent_count]]
    record_sent_items(gc, sent_ids)

    logger.info("===== Mercari Watcher 完了 =====")


if __name__ == "__main__":
    main()
