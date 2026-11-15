"""
yahoo_watcher.py
ヤフオク「カメラ・光学機器」の終了間近×入札ありオークションを取得し、
スプレッドシートのタブ「ヤフオク終了間近」に毎回上書き出力する。
GitHub Actions で1日数回スケジュール実行する想定。

既存の mercari_watcher.py と同じリポジトリに置き、
get_gspread_client / SPREADSHEET_NAME を再利用する。
"""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

from mercari_watcher import get_gspread_client, SPREADSHEET_NAME
from yahoo_auction import get_ending_auctions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")
YAHOO_SHEET = "ヤフオク終了間近"

HEADER = ["終了時刻", "残り", "入札", "現在価格", "即決価格",
          "値上がり", "状態", "商品名", "URL"]

# ヤフオクの商品状態コード → 日本語
COND_MAP = {
    "NEW": "新品", "USED10": "未使用に近い", "USED20": "目立った傷なし",
    "USED40": "やや傷汚れ", "USED60": "傷汚れあり", "USED80": "全体的に状態悪い",
}


def _remaining(end: datetime, now: datetime) -> str:
    mins = int((end - now).total_seconds() // 60)
    if mins < 0:
        return "終了"
    h, m = divmod(mins, 60)
    return f"{h}時間{m}分" if h else f"{m}分"


def write_to_sheet(client, rows: list[dict]):
    ss = client.open(SPREADSHEET_NAME)
    try:
        sheet = ss.worksheet(YAHOO_SHEET)
    except gspread.WorksheetNotFound:
        sheet = ss.add_worksheet(title=YAHOO_SHEET, rows=1000, cols=len(HEADER))
        logger.info(f"シート「{YAHOO_SHEET}」を新規作成")

    now = datetime.now(JST)
    values = [HEADER]
    for r in rows:
        values.append([
            r["end_jst"].strftime("%m/%d %H:%M"),
            _remaining(r["end_jst"], now),
            r["bids"],
            r["price"],
            r["buynow"] if r["buynow"] else "",
            f'{r["ratio"]}x' if r["ratio"] is not None else "",
            COND_MAP.get(r["condition"], r["condition"]),
            r["title"],
            r["url"],
        ])

    sheet.clear()
    sheet.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    logger.info(f"スプレッドシート出力完了：{len(rows)} 件")


def main():
    logger.info("===== Yahoo Watcher 起動 =====")
    try:
        gc = get_gspread_client()
    except Exception as e:
        logger.error(f"Google Sheets 認証失敗: {e}")
        return

    rows = get_ending_auctions()
    logger.info(f"終了間近 × 入札あり：{len(rows)} 件")

    write_to_sheet(gc, rows)
    logger.info("===== Yahoo Watcher 完了 =====")


if __name__ == "__main__":
    main()
