"""
yahoo_watcher.py
ヤフオク「今日・21時までに終了 × 入札あり」のカメラ商品を取得し、
スプレッドシートのタブ「ヤフオク終了間近」に見やすく整形して上書き出力する。

見やすさの工夫：
- 商品名はクリックで商品ページに飛べるリンク（URL列を廃止）
- 入札数が少ない＝狙い目を緑、多い＝激戦を赤で色分け
- 即決ありを強調、ヘッダー固定、¥表示
GitHub Actions で1日数回スケジュール実行する想定。
"""

import logging
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

from mercari_watcher import get_gspread_client, SPREADSHEET_NAME
from yahoo_auction import get_todays_auctions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")
YAHOO_SHEET = "ヤフオク終了間近"

# 列: 終了/残り/入札/現在/即決/状態/評価/種別/出品者/商品(リンク)
HEADER = ["終了", "残り", "入札", "現在価格", "即決", "状態",
          "評価", "種別", "出品者", "商品名（クリックで開く）"]

LOW_BID = 3        # これ以下＝狙い目(緑)
HIGH_BID = 20      # これ以上＝激戦(赤)
RATING_GOOD = 0.99  # これ以上＝優良(緑)
RATING_BAD = 0.95   # これ未満＝要注意(赤)

COND_MAP = {
    "NEW": "新品", "USED10": "未使用に近い", "USED20": "目立った傷なし",
    "USED40": "やや傷汚れ", "USED60": "傷汚れあり", "USED80": "状態悪い",
}


def _remaining(end: datetime, now: datetime) -> str:
    mins = int((end - now).total_seconds() // 60)
    if mins < 0:
        return "終了"
    h, m = divmod(mins, 60)
    return f"{h}:{m:02d}" if h else f"{m}分"


def _esc(s: str) -> str:
    return s.replace('"', '""')


def _apply_formatting(ss, sheet, n_rows: int, n_offpeak: int):
    """ヘッダー固定・色分け・¥表示などの見た目を適用（失敗してもデータは残す）。
    n_offpeak: 21時前終了（狙い目帯）の件数。並びが終了昇順なので上から連続する。"""
    sid = sheet.id
    last = n_rows + 1  # ヘッダー込みの最終行

    # 既存の条件付き書式を消してから貼り直す（毎回上書きのため蓄積防止）
    meta = ss.fetch_sheet_metadata()
    existing = 0
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("sheetId") == sid:
            existing = len(s.get("conditionalFormats", []))
            break

    reqs = []
    for _ in range(existing):
        reqs.append({"deleteConditionalFormatRule": {"sheetId": sid, "index": 0}})

    # ヘッダー固定＋太字背景
    reqs.append({"updateSheetProperties": {
        "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
        "fields": "gridProperties.frozenRowCount"}})
    reqs.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
            "horizontalAlignment": "CENTER"}},
        "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"}})
    # ヘッダー文字を白に
    reqs.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {"textFormat": {
            "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}}},
        "fields": "userEnteredFormat.textFormat"}})

    # 列幅（0:終了 1:残 2:入札 3:現在 4:即決 5:状態 6:評価 7:種別 8:出品者 9:商品）
    widths = [60, 50, 42, 82, 82, 90, 56, 48, 56, 440]
    for i, w in enumerate(widths):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w}, "fields": "pixelSize"}})

    body = {"startRowIndex": 1, "endRowIndex": last}

    # 入札: 少ない=緑(狙い目)
    reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {
        "ranges": [{"sheetId": sid, "startColumnIndex": 2, "endColumnIndex": 3, **body}],
        "booleanRule": {
            "condition": {"type": "NUMBER_LESS_THAN_EQ",
                          "values": [{"userEnteredValue": str(LOW_BID)}]},
            "format": {"backgroundColor": {"red": 0.72, "green": 0.88, "blue": 0.72}}}}}})
    # 入札: 多い=赤(激戦)
    reqs.append({"addConditionalFormatRule": {"index": 1, "rule": {
        "ranges": [{"sheetId": sid, "startColumnIndex": 2, "endColumnIndex": 3, **body}],
        "booleanRule": {
            "condition": {"type": "NUMBER_GREATER_THAN_EQ",
                          "values": [{"userEnteredValue": str(HIGH_BID)}]},
            "format": {"backgroundColor": {"red": 0.96, "green": 0.78, "blue": 0.78}}}}}})
    # 即決: 値あり=青で強調
    reqs.append({"addConditionalFormatRule": {"index": 2, "rule": {
        "ranges": [{"sheetId": sid, "startColumnIndex": 4, "endColumnIndex": 5, **body}],
        "booleanRule": {
            "condition": {"type": "NOT_BLANK"},
            "format": {"backgroundColor": {"red": 0.80, "green": 0.89, "blue": 0.97}}}}}})
    # 評価(G,col6): 優良=緑 / 要注意=赤
    reqs.append({"addConditionalFormatRule": {"index": 3, "rule": {
        "ranges": [{"sheetId": sid, "startColumnIndex": 6, "endColumnIndex": 7, **body}],
        "booleanRule": {
            "condition": {"type": "NUMBER_GREATER_THAN_EQ",
                          "values": [{"userEnteredValue": str(RATING_GOOD)}]},
            "format": {"backgroundColor": {"red": 0.72, "green": 0.88, "blue": 0.72}}}}}})
    reqs.append({"addConditionalFormatRule": {"index": 4, "rule": {
        "ranges": [{"sheetId": sid, "startColumnIndex": 6, "endColumnIndex": 7, **body}],
        "booleanRule": {
            "condition": {"type": "NUMBER_LESS_THAN",
                          "values": [{"userEnteredValue": str(RATING_BAD)}]},
            "format": {"backgroundColor": {"red": 0.96, "green": 0.78, "blue": 0.78}}}}}})
    # 出品者タグ(I,col8): 同一出品者(複数出品)=黄で強調
    reqs.append({"addConditionalFormatRule": {"index": 5, "rule": {
        "ranges": [{"sheetId": sid, "startColumnIndex": 8, "endColumnIndex": 9, **body}],
        "booleanRule": {
            "condition": {"type": "NOT_BLANK"},
            "format": {"backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.70}}}}}})

    # 価格列（現在=D, 即決=E）を¥表示
    reqs.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": last,
                  "startColumnIndex": 3, "endColumnIndex": 5},
        "cell": {"userEnteredFormat": {"numberFormat": {
            "type": "NUMBER", "pattern": "¥#,##0"}}},
        "fields": "userEnteredFormat.numberFormat"}})
    # 評価列(G)を％表示
    reqs.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": last,
                  "startColumnIndex": 6, "endColumnIndex": 7},
        "cell": {"userEnteredFormat": {"numberFormat": {
            "type": "NUMBER", "pattern": "0.0%"}}},
        "fields": "userEnteredFormat.numberFormat"}})

    # 終了列(A)を時間帯で帯色分け：上=21時前(狙い目/薄緑)、下=21時以降(激戦/薄赤)
    # 終了昇順ソートなので狙い目帯は先頭から連続する。
    split = 1 + n_offpeak  # 狙い目帯の最終行(0-based endRowIndex)
    if n_offpeak > 0:
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": split,
                      "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"backgroundColor": {
                "red": 0.85, "green": 0.94, "blue": 0.85}}},
            "fields": "userEnteredFormat.backgroundColor"}})
    if split < last:
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": split, "endRowIndex": last,
                      "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"backgroundColor": {
                "red": 0.98, "green": 0.87, "blue": 0.87}}},
            "fields": "userEnteredFormat.backgroundColor"}})

    ss.batch_update({"requests": reqs})


def write_to_sheet(client, rows: list[dict]):
    ss = client.open(SPREADSHEET_NAME)
    try:
        sheet = ss.worksheet(YAHOO_SHEET)
    except gspread.WorksheetNotFound:
        sheet = ss.add_worksheet(title=YAHOO_SHEET, rows=1200, cols=len(HEADER))
        logger.info(f"シート「{YAHOO_SHEET}」を新規作成")

    # 同一出品者の判定：一覧内で2件以上出している出品者にグループ番号を付ける
    uid_counts = Counter(r["seller_uid"] for r in rows if r["seller_uid"])
    group_label, seq = {}, 0
    for r in rows:
        uid = r["seller_uid"]
        if uid and uid_counts[uid] >= 2 and uid not in group_label:
            seq += 1
            group_label[uid] = f"S{seq}"

    now = datetime.now(JST)
    values = [HEADER]
    for r in rows:
        link = f'=HYPERLINK("{r["url"]}","{_esc(r["title"])}")'
        uid = r["seller_uid"]
        seller_tag = (f"{group_label[uid]}×{uid_counts[uid]}"
                      if uid in group_label else "")
        values.append([
            r["end_jst"].strftime("%H:%M"),
            _remaining(r["end_jst"], now),
            r["bids"],
            r["price"],
            r["buynow"] if r["buynow"] else "",
            COND_MAP.get(r["condition"], r["condition"]),
            r["rating"] if r["rating"] is not None else "",
            "ストア" if r["is_store"] else "個人",
            seller_tag,
            link,
        ])

    sheet.clear()
    sheet.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    n_offpeak = sum(1 for r in rows if not r["is_peak"])
    logger.info(f"スプレッドシート出力完了：{len(rows)} 件（うち21時前終了 {n_offpeak} 件）")

    try:
        _apply_formatting(ss, sheet, len(rows), n_offpeak)
        logger.info("書式適用 完了")
    except Exception as e:
        logger.warning(f"書式適用はスキップ（データは出力済み）: {e}")


def main():
    logger.info("===== Yahoo Watcher 起動 =====")
    try:
        gc = get_gspread_client()
    except Exception as e:
        logger.error(f"Google Sheets 認証失敗: {e}")
        return

    rows = get_todays_auctions()
    logger.info(f"今日終了 × 入札あり：{len(rows)} 件")
    write_to_sheet(gc, rows)
    logger.info("===== Yahoo Watcher 完了 =====")


if __name__ == "__main__":
    main()
