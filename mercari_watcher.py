"""
mercari_watcher.py
mercapiライブラリでメルカリ新着カメラ商品を取得し、
前段除外フィルタを通してn8n Webhookに送信する。
"""

import asyncio
import os
import json
import re
import time
import logging
import requests
import gspread
from datetime import datetime, timezone, timedelta
from google.oauth2.service_account import Credentials
from mercapi import Mercapi

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
# ★ KEYWORD → KWORDSリストに変更（11種）
KEYWORDS = [
    "一眼レフカメラ",
    "単焦点レンズ",
    "交換レンズ",
    "ミラーレスカメラ",
    "α7",
    "EOS R",
    "Nikon Z",
    "ズームレンズ",
    "FUJIFILM X",
    "GH5",
    "OM-1",
]

CAMERA_CATEGORY_IDS = {
    # ── カメラ本体 ──
    "4006",   # ミラーレス一眼
    "4007",   # コンパクトデジカメ
    "4008",   # トイカメラ
    "4009",   # デジタル一眼レフ
    "4012",   # ビデオカメラ
    "4023",   # アクションカメラ（GoPro等）
    "4025",   # アクションカメラ
    "4028",   # インスタントカメラ（チェキ等）
    "4032",   # フィルム一眼レフ
    "4033",   # 使い捨てカメラ
    "4031",   # パノラマカメラ・その他カメラ
    "843",    # コンパクトデジカメ
    "8724",   # カメラ（その他）
    # ── レンズ ──
    "846",    # 交換レンズ
    "1255",   # 交換レンズ
    "4078",   # レンズマウントアダプター
    "3671",   # スマホ用レンズ等
    # ── 周辺機器・アクセサリ ──
    "4101",   # ジンバル・スタビライザー
    "4103",   # ストロボ・フラッシュ
    "4113",   # 小型カメラ
    "4114",   # バッテリー・充電器
    "4115",   # 充電器
    "4117",   # カメラバッグ
    "4119",   # カメラポーチ
    "4120",   # クリーニング用品
    "4097",   # ストラップ
    "4098",   # 三脚・雲台
    "4076",   # レンズフード・フィルター
    "4083",   # レンズフィルター
    "536",    # デジタルカメラ（量販店系）
    "980",    # デジタルカメラ
}

NG_KEYWORDS = ["ジャンク", "故障", "部品取り"]

# メルカリcategoryId → n8nカテゴリ名マッピング
CATEGORY_MAP = {
    "4006":  "mirrorless",  # ミラーレス一眼
    "4007":  "compact",     # コンパクトデジカメ
    "4008":  "compact",     # トイカメラ
    "4009":  "dslr",        # デジタル一眼レフ
    "4012":  "video",       # ビデオカメラ
    "4023":  "action",      # アクションカメラ（GoPro等）
    "4025":  "action",      # アクションカメラ
    "4028":  "compact",     # インスタントカメラ
    "4031":  "other",       # パノラマカメラ
    "4032":  "dslr",        # フィルム一眼レフ
    "4033":  "compact",     # 使い捨てカメラ
    "843":   "compact",     # コンパクトデジカメ
    "846":   "lens",        # 交換レンズ
    "1255":  "lens",        # 交換レンズ
    "4078":  "lens",        # レンズマウントアダプター
    "3671":  "lens",        # スマホ用レンズ
    "8724":  "other",       # カメラ（その他）
    "536":   "compact",     # デジタルカメラ
    "980":   "compact",     # デジタルカメラ
    "4101":  "other",       # ジンバル
    "4103":  "other",       # ストロボ
    "4113":  "other",       # 小型カメラ
    "4114":  "other",       # バッテリー
    "4115":  "other",       # 充電器
    "4117":  "other",       # カメラバッグ
    "4119":  "other",       # カメラポーチ
    "4120":  "other",       # クリーニング用品
    "4097":  "other",       # ストラップ
    "4098":  "other",       # 三脚・雲台
    "4076":  "other",       # レンズフード
    "4083":  "other",       # レンズフィルター
}

SPREADSHEET_NAME = "仕入れマスター_統合版_v2_v8"
BLOCKLIST_SHEET = "除外リスト"
SENT_SHEET = "送信済みリスト"
MASTER_SHEET = "相場表_v2"          # ★ 相場表シート名
SENT_RETENTION_HOURS = 24

# 状態IDと状態名・仕入上限列のマッピング
CONDITION_MAP = {
    "1": ("ほぼ新品",   "max_buy_new"),
    "2": ("非常に良い", "max_buy_very_good"),
    "3": ("良い",       "max_buy_good"),
    "4": ("可",         "max_buy_acceptable"),
}
# それ以外はジャンク → max_buy_acceptableを使用
CONDITION_DEFAULT = ("ジャンク", "max_buy_acceptable")


# ──────────────────────────────────────────
# Google Sheets 認証
# ──────────────────────────────────────────
def get_gspread_client():
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
# 相場表 機種名リスト取得
# ──────────────────────────────────────────
def fetch_master_sheet(client):
    """相場表_v2からmodel_name_normalized・max_buy_*列を全件取得して辞書リストで返す"""
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(MASTER_SHEET)
        records = sheet.get_all_records()
        logger.info(f"相場表 全件取得完了：{len(records)} 件")
        return records
    except Exception as e:
        logger.warning(f"相場表 取得失敗（空リストで継続）: {e}")
        return []


def fetch_model_names(master_records):
    """相場表レコードからmodel_name_normalizedを抽出してリストで返す（後方互換）"""
    model_names = [
        r.get("model_name_normalized", "").strip()
        for r in master_records
        if r.get("model_name_normalized", "").strip()
        and len(r.get("model_name_normalized", "").strip()) >= 4
    ]
    logger.info(f"相場表 機種名取得完了：{len(model_names)} 件")
    return model_names


MAKER_NAMES = {
    'NIKON', 'CANON', 'SONY', 'FUJIFILM', 'PANASONIC',
    'OLYMPUS', 'PENTAX', 'SIGMA', 'TAMRON', 'TOKINA',
    'LEICA', 'HASSELBLAD', 'GOPRO', 'DJI', 'INSTA360',
    'RICOH', 'CASIO', 'MINOLTA', 'KYOCERA', 'MAMIYA',
}


def extract_model_tokens(master_records):
    """
    相場表レコードから型番トークンを抽出し、
    token → [record, ...] の辞書と token のsetを返す。
    """
    token_to_records = {}  # token(大文字) → レコードリスト
    for record in master_records:
        name = record.get("model_name_normalized", "").strip()
        if not name:
            continue
        words = name.split()
        for word in words:
            w = word.upper()
            if len(w) >= 4 and re.search(r'[A-Z0-9]', w) and w not in MAKER_NAMES:
                if w not in token_to_records:
                    token_to_records[w] = []
                token_to_records[w].append(record)
    tokens = set(token_to_records.keys())
    logger.info(f"型番トークン抽出完了：{len(tokens)} 件")
    return token_to_records, tokens


def find_matched_model(title, token_to_records, condition_id):
    """
    商品タイトルに型番トークンが含まれるか判定し、
    マッチした機種名・状態名・max_priceを返す。
    マッチしない場合は None を返す。
    """
    title_upper = title.upper()
    condition_name, price_col = CONDITION_MAP.get(str(condition_id), CONDITION_DEFAULT)

    for token, records in token_to_records.items():
        if token in title_upper:
            # トークンにマッチした最初のレコードのmax_priceを使用
            record = records[0]
            raw_price = record.get(price_col, "")
            try:
                max_price = int(str(raw_price).replace(",", "").strip()) if raw_price != "" else None
            except (ValueError, TypeError):
                max_price = None
            return {
                "matched_name": record.get("model_name_normalized", ""),
                "condition": condition_name,
                "max_price": max_price,
            }
    return None


def is_model_matched(title, model_tokens):
    """商品タイトルに型番トークンが含まれるか判定（後方互換用・単純boolean）"""
    title_upper = title.upper()
    return any(token in title_upper for token in model_tokens)


# ──────────────────────────────────────────
# ブラックリスト取得
# ──────────────────────────────────────────
def fetch_blocked_seller_ids(client):
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
def fetch_already_sent(client):
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
                valid_ids.add(item_id)
        logger.info(f"送信済みリスト取得完了：{len(valid_ids)} 件（{SENT_RETENTION_HOURS}h以内）")
        return valid_ids
    except Exception as e:
        logger.warning(f"送信済みリスト取得失敗（空セットで継続）: {e}")
        return set()


def record_sent_items(client, item_ids):
    if not item_ids:
        return
    try:
        ss = client.open(SPREADSHEET_NAME)
        sheet = ss.worksheet(SENT_SHEET)
        now_str = datetime.now(timezone.utc).isoformat()
        rows_to_add = [[iid, now_str] for iid in item_ids]
        sheet.append_rows(rows_to_add, value_input_option="RAW")
        _cleanup_sent_sheet(sheet)
        logger.info(f"送信済みリスト記録完了：{len(item_ids)} 件追加")
    except Exception as e:
        logger.warning(f"送信済みリスト記録失敗: {e}")


def _cleanup_sent_sheet(sheet):
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(hours=SENT_RETENTION_HOURS)
        rows_to_delete = []
        for i, row in enumerate(all_values[1:], start=2):
            try:
                sent_at_str = row[1] if len(row) > 1 else ""
                sent_at = datetime.fromisoformat(sent_at_str)
                if sent_at.tzinfo is None:
                    sent_at = sent_at.replace(tzinfo=timezone.utc)
                if sent_at < cutoff:
                    rows_to_delete.append(i)
            except Exception:
                pass
        for row_num in reversed(rows_to_delete):
            sheet.delete_rows(row_num)
        if rows_to_delete:
            logger.info(f"送信済みリスト：{len(rows_to_delete)} 件の古い行を削除")
    except Exception as e:
        logger.warning(f"送信済みリスト クリーンアップ失敗: {e}")


# ──────────────────────────────────────────
# メルカリ取得（mercapiライブラリ使用）
# ──────────────────────────────────────────
async def fetch_mercari_items_async(keyword):
    try:
        m = Mercapi()
        results = await m.search(keyword)
        items = []
        for item in results.items:
            items.append({
                "item_id": item.id_ or "",
                "title": item.name or "",
                "price": int(item.price) if item.price else 0,
                "url": f"https://jp.mercari.com/item/{item.id_}",
                "seller_id": str(item.seller_id) if item.seller_id else "",
                "category_id": str(item.category_id) if hasattr(item, "category_id") and item.category_id else "",
                "status": str(item.status) if item.status else "",
                "item_condition_id": str(item.item_condition_id) if hasattr(item, "item_condition_id") and item.item_condition_id else "",
            })
        # ★ キーワードごとの取得件数をログ出力
        logger.info(f"{keyword}：{len(items)} 件取得")
        return items
    except Exception as e:
        logger.error(f"メルカリ取得エラー（キーワード：{keyword}）: {e}")
        return []


def fetch_mercari_items(keyword):
    return asyncio.run(fetch_mercari_items_async(keyword))


# ──────────────────────────────────────────
# 前段除外フィルタ
# ──────────────────────────────────────────
def apply_filters(items, blocked_seller_ids, already_sent, token_to_records, model_tokens):
    stats = {
        "total": len(items),
        "status_blocked": 0,
        "seller_blocked": 0,
        "price_blocked": 0,
        "ng_keyword_blocked": 0,
        "category_blocked": 0,
        "model_blocked": 0,   # ★ 相場表マッチング除外
        "duplicate_blocked": 0,
        "passed": 0,
    }

    passed = []
    for item in items:
        # Step 1: 販売中以外を除外
        status_str = str(item["status"])
        if "ON_SALE" not in status_str and status_str != "1":
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

        # Step 5: カテゴリフィルタ（category_idが取得できない場合は通過させる）
        if item["category_id"] and item["category_id"] not in CAMERA_CATEGORY_IDS:
            stats["category_blocked"] += 1
            continue

        # Step 6: 相場表マッチング（機種名が相場表にない商品は除外）
        if model_tokens:
            match_result = find_matched_model(
                item["title"], token_to_records, item["item_condition_id"]
            )
            if match_result is None:
                stats["model_blocked"] += 1
                continue
            # マッチ結果をitemに付加
            item["matched_name"] = match_result["matched_name"]
            item["condition"]    = match_result["condition"]
            item["max_price"]    = match_result["max_price"]
        else:
            item["matched_name"] = ""
            item["condition"]    = ""
            item["max_price"]    = None

        # Step 7: 重複除外
        if item["item_id"] in already_sent:
            stats["duplicate_blocked"] += 1
            continue

        passed.append(item)

    stats["passed"] = len(passed)
    return passed, stats


# ──────────────────────────────────────────
# n8n Webhook送信
# ──────────────────────────────────────────
def send_to_n8n(items):
    webhook_url = os.environ.get("N8N_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.error("N8N_WEBHOOK_URL が設定されていません。送信をスキップします。")
        return 0

    # 全件をまとめて1回のPOSTで送信（n8n実行回数を1回に節約）
    payload = {
        "items": [
            {
                "id":             item["item_id"],
                "name":           item["title"],
                "price":          item["price"],
                "url":            item["url"],
                "sellerId":       item["seller_id"],
                "itemConditionId": item["item_condition_id"],
                "category":       CATEGORY_MAP.get(item["category_id"], "other"),
                "matched_name":   item.get("matched_name", ""),
                "max_price":      item.get("max_price", None),
                "condition":      item.get("condition", ""),
            }
            for item in items
        ]
    }
    # ★ 先頭1件をサンプルログ出力
    if payload["items"]:
        logger.info(f"payloadサンプル（1件目）: {json.dumps(payload['items'][0], ensure_ascii=False)}")
    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        resp.raise_for_status()
        logger.info(f"n8n一括送信成功：{len(items)} 件")
        return len(items)
    except requests.RequestException as e:
        logger.error(f"n8n送信失敗: {e}")
        return 0


# ──────────────────────────────────────────
# メイン
# ──────────────────────────────────────────
def main():
    logger.info("===== Mercari Watcher 起動 =====")

    try:
        gc = get_gspread_client()
    except Exception as e:
        logger.error(f"Google Sheets 認証失敗: {e}")
        return

    blocked_seller_ids = fetch_blocked_seller_ids(gc)
    already_sent = fetch_already_sent(gc)
    master_records = fetch_master_sheet(gc)         # ★ 相場表 全件取得
    model_names = fetch_model_names(master_records)  # 機種名リスト（ログ確認用）
    token_to_records, model_tokens = extract_model_tokens(master_records)  # ★ 型番トークン抽出

    # ★ キーワードごとにループして全件取得・item_idで重複除去
    all_items_dict = {}  # item_id → item（重複除去用）
    for keyword in KEYWORDS:
        items = fetch_mercari_items(keyword)
        for item in items:
            item_id = item["item_id"]
            if item_id and item_id not in all_items_dict:
                all_items_dict[item_id] = item
        # ★ キーワード間に1秒のsleep（API負荷軽減）
        time.sleep(1)

    all_items = list(all_items_dict.values())
    logger.info(f"全キーワード合計（重複除去後）：{len(all_items)} 件")

    if not all_items:
        logger.warning("取得件数0件。終了します。")
        return

    passed, stats = apply_filters(all_items, blocked_seller_ids, already_sent, token_to_records, model_tokens)

    logger.info(f"取得件数　　　　：{stats['total']} 件")
    logger.info(f"販売中以外除外　：{stats['status_blocked']} 件")
    logger.info(f"sellerId除外　　：{stats['seller_blocked']} 件")
    logger.info(f"価格除外　　　　：{stats['price_blocked']} 件")
    logger.info(f"NGキーワード除外：{stats['ng_keyword_blocked']} 件")
    logger.info(f"カテゴリ除外　　：{stats['category_blocked']} 件")
    logger.info(f"相場表外除外　　：{stats['model_blocked']} 件")
    logger.info(f"重複除外　　　　：{stats['duplicate_blocked']} 件")
    logger.info(f"pass件数　　　　：{stats['passed']} 件")

    if not passed:
        logger.info("通知対象なし。終了します。")
        return

    sent_count = send_to_n8n(passed)
    logger.info(f"n8n送信　　　　：{sent_count} 件")

    sent_ids = [item["item_id"] for item in passed[:sent_count]]
    record_sent_items(gc, sent_ids)

    logger.info("===== Mercari Watcher 完了 =====")


if __name__ == "__main__":
    main()
