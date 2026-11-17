"""
auction_watcher.py
「今日(JST)終了 × 入札あり」のメルカリ オークション商品を取得し、
スプレッドシートの指定タブに毎回上書き出力する。
GitHub Actions でスケジュール実行する想定。

環境変数で切替（未指定ならカメラ＝従来どおり）：
- MERCARI_CATEGORY_ID : カンマ区切りで複数可（例 "24,39"）。既定 97(カメラ)
- MERCARI_SHEET_NAME  : 出力タブ名。既定「本日終了オークション」
- MERCARI_PRICE_MIN / MERCARI_PRICE_MAX : 現在価格の下限/上限（上限は以上を除外）
"""

import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

from mercari_watcher import get_gspread_client
from mercari_auction import get_today_auction_list, CAMERA_TOP_CATEGORY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")

# ファイル名はリネームされ得るので、不変のファイルIDで開く（名前変更に強い）
SPREADSHEET_ID = "1MfOt13h8BAS0dcBAcROon8-nFpu3vQ25FTFZflZCYGc"

_cat_raw = os.environ.get("MERCARI_CATEGORY_ID", str(CAMERA_TOP_CATEGORY))
CATEGORY_IDS = [int(c.strip()) for c in _cat_raw.split(",") if c.strip()]
PRICE_MIN = int(os.environ.get("MERCARI_PRICE_MIN", "5000"))
PRICE_MAX = int(os.environ.get("MERCARI_PRICE_MAX", "0"))
AUCTION_SHEET = os.environ.get("MERCARI_SHEET_NAME", "本日終了オークション")

HEADER = ["終了時刻", "残り", "入札", "現在価格", "開始価格",
          "値上がり", "ブランド", "カテゴリID", "状態", "商品名", "URL"]


def _remaining(deadline: datetime, now: datetime) -> str:
    delta = deadline - now
    mins = int(delta.total_seconds() // 60)
    if mins < 0:
        return "終了"
    h, m = divmod(mins, 60)
    return f"{h}時間{m}分" if h else f"{m}分"


def write_to_sheet(client, rows: list[dict]):
    ss = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = ss.worksheet(AUCTION_SHEET)
    except gspread.WorksheetNotFound:
        sheet = ss.add_worksheet(title=AUCTION_SHEET, rows=1000, cols=len(HEADER))
        logger.info(f"シート「{AUCTION_SHEET}」を新規作成")

    now = datetime.now(JST)
    values = [HEADER]
    for r in rows:
        values.append([
            r["deadline_jst"].strftime("%m/%d %H:%M"),
            _remaining(r["deadline_jst"], now),
            r["total_bid"],
            r["highest_bid"],
            r["initial_price"],
            f'{r["ratio"]}x' if r["ratio"] is not None else "",
            r["brand"],
            r["category_id"],
            r["condition_id"],
            r["name"],
            r["url"],
        ])

    sheet.clear()
    sheet.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    logger.info(f"スプレッドシート出力完了：{len(rows)} 件")


def main():
    logger.info("===== Auction Watcher 起動 =====")
    logger.info(f"対象: categories={CATEGORY_IDS} / タブ「{AUCTION_SHEET}」/ "
                f"下限¥{PRICE_MIN:,} 上限¥{PRICE_MAX:,}")
    try:
        gc = get_gspread_client()
    except Exception as e:
        logger.error(f"Google Sheets 認証失敗: {e}")
        return

    rows = get_today_auction_list(category_ids=CATEGORY_IDS,
                                  price_min=PRICE_MIN, price_max=PRICE_MAX,
                                  max_pages=8)
    logger.info(f"今日終了 × 入札あり：{len(rows)} 件")

    if not rows:
        logger.info("対象なし。空のヘッダのみ出力します。")
    write_to_sheet(gc, rows)
    logger.info("===== Auction Watcher 完了 =====")


if __name__ == "__main__":
    main()
