"""
yahoo_watcher.py
ヤフオクのカテゴリ（複数可）の「今日終了 × 入札あり」を取得し、
スプレッドシートのタブに見やすく整形して上書き出力する。
環境変数：
- YAHOO_CATEGORY_ID : カンマ区切りで複数可（例 "23260,23264,23268,23272"）
- YAHOO_SHEET_NAME  : 出力タブ名
- YAHOO_MIN_PRICE / YAHOO_MAX_PRICE : 現在価格の下限/上限（上限は以上を除外）
- YAHOO_EXCLUDE     : タイトルにこの語を含む商品を除外（カンマ区切り）
"""

import os
import logging
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

from mercari_watcher import get_gspread_client
from yahoo_auction import get_todays_auctions, CATEGORY_ID as DEFAULT_CATEGORY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "1MfOt13h8BAS0dcBAcROon8-nFpu3vQ25FTFZflZCYGc"

_cat_raw = os.environ.get("YAHOO_CATEGORY_ID", str(DEFAULT_CATEGORY))
CATEGORY_IDS = [int(c.strip()) for c in _cat_raw.split(",") if c.strip()]
MIN_PRICE = int(os.environ.get("YAHOO_MIN_PRICE", "0"))
MAX_PRICE = int(os.environ.get("YAHOO_MAX_PRICE", "0"))
EXCLUDE_WORDS = tuple(w.strip() for w in os.environ.get("YAHOO_EXCLUDE", "").split(",") if w.strip())

JST = ZoneInfo("Asia/Tokyo")
YAHOO_SHEET = os.environ.get("YAHOO_SHEET_NAME", "ヤフオク終了間近")
CALC_SHEET = "利益計算"

HEADER = ["終了", "残り", "入札", "現在価格", "即決", "商品名（クリックで開く）",
          "状態", "評価", "種別", "出品者",
          "想定売値", "現時点利益", "利益率", "入札上限", "id"]
COL_URIAGE = "想定売値"
COL_ID = "id"

LOW_BID = 3
HIGH_BID = 20
RATING_GOOD = 0.99
RATING_BAD = 0.95

COND_MAP = {
    "NEW": "新品", "USED10": "未使用に近い", "USED20": "目立った傷なし",
    "USED40": "やや傷汚れ", "USED60": "傷汚れあり", "USED80": "状態悪い",
}

R_RATE = f"'{CALC_SHEET}'!$B$4"
R_FIX = f"'{CALC_SHEET}'!$B$5"
R_SHIP = f"'{CALC_SHEET}'!$B$6"
R_TARGET = f"'{CALC_SHEET}'!$B$7"


def _remaining(end: datetime, now: datetime) -> str:
    mins = int((end - now).total_seconds() // 60)
    if mins < 0:
        return "終了"
    h, m = divmod(mins, 60)
    return f"{h}:{m:02d}" if h else f"{m}分"


def _esc(s: str) -> str:
    return s.replace('"', '""')


def _load_saved_uriage(sheet) -> dict:
    try:
        existing = sheet.get_values(value_render_option="UNFORMATTED_VALUE")
    except Exception as e:
        logger.warning(f"既存想定売値の読み込みスキップ: {e}")
        return {}
    if not existing or len(existing) < 2:
        return {}
    hdr = existing[0]
    try:
        j = hdr.index(COL_URIAGE)
        k = hdr.index(COL_ID)
    except ValueError:
        return {}
    saved = {}
    for row in existing[1:]:
        if len(row) <= max(j, k):
            continue
        iid = str(row[k]).strip()
        val = row[j]
        if iid and val not in ("", None):
            saved[iid] = val
    logger.info(f"想定売値の引き継ぎ: {len(saved)} 件")
    return saved


def _apply_formatting(ss, sheet, n_rows: int, n_offpeak: int):
    sid = sheet.id
    last = n_rows + 1

    meta = ss.fetch_sheet_metadata()
    existing = 0
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("sheetId") == sid:
            existing = len(s.get("conditionalFormats", []))
            break

    reqs = []
    for _ in range(existing):
        reqs.append({"deleteConditionalFormatRule": {"sheetId": sid, "index": 0}})

    reqs.append({"updateSheetProperties": {
        "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
        "fields": "gridProperties.frozenRowCount"}})
    reqs.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
            "horizontalAlignment": "CENTER"}},
        "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"}})

    widths = [60, 50, 42, 82, 82, 420, 90, 56, 48, 56, 84, 84, 52, 84]
    for i, w in enumerate(widths):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": w}, "fields": "pixelSize"}})
    reqs.append({"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 14, "endIndex": 15},
        "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}})

    body = {"startRowIndex": 1, "endRowIndex": last}

    def cf(idx, col0, col1, cond_type, val, rgb):
        rule = {"ranges": [{"sheetId": sid, "startColumnIndex": col0,
                            "endColumnIndex": col1, **body}],
                "booleanRule": {"condition": {"type": cond_type},
                                "format": {"backgroundColor": {
                                    "red": rgb[0], "green": rgb[1], "blue": rgb[2]}}}}
        if val is not None:
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": val}]
        reqs.append({"addConditionalFormatRule": {"index": idx, "rule": rule}})

    GREEN = (0.72, 0.88, 0.72)
    RED = (0.96, 0.78, 0.78)
    cf(0, 2, 3, "NUMBER_LESS_THAN_EQ", str(LOW_BID), GREEN)
    cf(1, 2, 3, "NUMBER_GREATER_THAN_EQ", str(HIGH_BID), RED)
    cf(2, 4, 5, "NOT_BLANK", None, (0.80, 0.89, 0.97))
    cf(3, 7, 8, "NUMBER_GREATER_THAN_EQ", str(RATING_GOOD), GREEN)
    cf(4, 7, 8, "NUMBER_LESS", str(RATING_BAD), RED)
    cf(5, 9, 10, "NOT_BLANK", None, (1.0, 0.95, 0.70))
    cf(6, 11, 12, "NUMBER_GREATER", "0", GREEN)
    cf(7, 11, 12, "NUMBER_LESS", "0", RED)

    for c0, c1 in ((3, 5), (10, 12), (13, 14)):
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": last,
                      "startColumnIndex": c0, "endColumnIndex": c1},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "¥#,##0"}}},
            "fields": "userEnteredFormat.numberFormat"}})
    for c0, c1 in ((7, 8), (12, 13)):
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": last,
                      "startColumnIndex": c0, "endColumnIndex": c1},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.0%"}}},
            "fields": "userEnteredFormat.numberFormat"}})
    reqs.append({"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": last,
                  "startColumnIndex": 10, "endColumnIndex": 11},
        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 0.97, "blue": 0.8}}},
        "fields": "userEnteredFormat.backgroundColor"}})

    split = 1 + n_offpeak
    if n_offpeak > 0:
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": split,
                      "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85, "green": 0.94, "blue": 0.85}}},
            "fields": "userEnteredFormat.backgroundColor"}})
    if split < last:
        reqs.append({"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": split, "endRowIndex": last,
                      "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.98, "green": 0.87, "blue": 0.87}}},
            "fields": "userEnteredFormat.backgroundColor"}})

    ss.batch_update({"requests": reqs})


def write_to_sheet(client, rows: list[dict]):
    ss = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = ss.worksheet(YAHOO_SHEET)
    except gspread.WorksheetNotFound:
        sheet = ss.add_worksheet(title=YAHOO_SHEET, rows=1200, cols=len(HEADER))
        logger.info(f"シート「{YAHOO_SHEET}」を新規作成")

    saved = _load_saved_uriage(sheet)

    need_rows = len(rows) + 5
    new_rows = max(sheet.row_count, need_rows)
    new_cols = max(sheet.col_count, len(HEADER))
    if new_rows != sheet.row_count or new_cols != sheet.col_count:
        sheet.resize(rows=new_rows, cols=new_cols)
        logger.info(f"グリッド拡張: {new_rows}行 x {new_cols}列")

    uid_counts = Counter(r["seller_uid"] for r in rows if r["seller_uid"])
    group_label, seq = {}, 0
    for r in rows:
        uid = r["seller_uid"]
        if uid and uid_counts[uid] >= 2 and uid not in group_label:
            seq += 1
            group_label[uid] = f"S{seq}"

    now = datetime.now(JST)
    values = [HEADER]
    for i, r in enumerate(rows):
        row = i + 2
        link = f'=HYPERLINK("{r["url"]}","{_esc(r["title"])}")'
        uid = r["seller_uid"]
        seller_tag = f"{group_label[uid]}×{uid_counts[uid]}" if uid in group_label else ""
        uriage = saved.get(r["id"], "")
        gaineki = (f'=IF($K{row}="","",$K{row}-($K{row}*{R_RATE}+{R_FIX})'
                   f'-($D{row}+{R_SHIP}))')
        rieki_ritsu = f'=IF($K{row}="","",$L{row}/$K{row})'
        jougen = (f'=IF($K{row}="","",$K{row}-($K{row}*{R_RATE}+{R_FIX})'
                  f'-($K{row}*{R_TARGET})-{R_SHIP})')
        values.append([
            r["end_jst"].strftime("%H:%M"),
            _remaining(r["end_jst"], now),
            r["bids"],
            r["price"],
            r["buynow"] if r["buynow"] else "",
            link,
            COND_MAP.get(r["condition"], r["condition"]),
            r["rating"] if r["rating"] is not None else "",
            "ストア" if r["is_store"] else "個人",
            seller_tag,
            uriage,
            gaineki,
            rieki_ritsu,
            jougen,
            r["id"],
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

    logger.info(f"対象: categories={CATEGORY_IDS} / タブ「{YAHOO_SHEET}」/ "
                f"下限¥{MIN_PRICE:,} 上限¥{MAX_PRICE:,} / 除外{list(EXCLUDE_WORDS)}")
    rows = get_todays_auctions(CATEGORY_IDS, MIN_PRICE, MAX_PRICE, EXCLUDE_WORDS)
    logger.info(f"今日終了 × 入札あり：{len(rows)} 件")
    write_to_sheet(gc, rows)
    logger.info("===== Yahoo Watcher 完了 =====")


if __name__ == "__main__":
    main()
