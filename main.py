"""
Entry point — работает и локально, и в Cloud Run (functions-framework).

─── Режимы синхронизации ────────────────────────────────────────────────────
  incremental (по умолчанию)
      Только транзакционные данные за последние LOOKBACK_DAYS дней.
      Запускается Cloud Scheduler каждые 10 минут.
      Лоадеры: transactions, storage, finance

  full
      Всё: справочники + транзакционные данные.
      Запускать вручную или раз в сутки для актуализации меню/сотрудников/клиентов.
      Лоадеры: menu, directory, transactions, storage, finance

─── Локальный запуск ────────────────────────────────────────────────────────
  python main.py                              # incremental, даты из LOOKBACK_DAYS
  python main.py 2026-04-01 2026-04-15        # incremental, явный период
  python main.py full                         # full, даты из LOOKBACK_DAYS
  python main.py full 2026-04-01 2026-04-15   # full, явный период

─── Cloud Run / Cloud Scheduler ─────────────────────────────────────────────
  POST {}                                     # incremental, lookback
  POST {"sync_type": "full"}                  # full sync
  POST {"date_from": "2026-04-01", "date_to": "2026-04-15"}
  POST {"sync_type": "full", "date_from": "...", "date_to": "..."}
"""

import json
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import functions_framework

from config import LOOKBACK_DAYS, TIMEZONE
from loaders import directory, finance, ingredients, menu
from loaders import transactions as transactions_loader
from loaders import storage

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_dates(date_from: str | None, date_to: str | None) -> tuple[str, str]:
    if date_from and date_to:
        return date_from, date_to
    today     = datetime.now(tz=ZoneInfo(TIMEZONE)).date()
    date_from = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()
    date_to   = today.isoformat()
    logger.info("No dates provided — using lookback: %s → %s", date_from, date_to)
    return date_from, date_to


def _run_all(sync_type: str, date_from: str, date_to: str) -> dict:
    """
    Запускает нужные лоадеры в зависимости от sync_type.
    Возвращает сводный результат.
    """
    results = {"sync_type": sync_type, "date_from": date_from, "date_to": date_to}

    if sync_type == "full":
        logger.info("▶ menu loader")
        results["menu"]        = menu.run()

        logger.info("▶ ingredients & tech cards loader")
        results["ingredients"] = ingredients.run()

        logger.info("▶ directory loader")
        results["directory"]   = directory.run()

    logger.info("▶ transactions loader")
    results["transactions"] = transactions_loader.run(date_from, date_to)

    logger.info("▶ storage loader")
    results["storage"]      = storage.run(date_from, date_to)

    logger.info("▶ finance loader")
    results["finance"]      = finance.run(date_from, date_to)

    results["status"] = "ok"
    return results


# ── Cloud Run handler ─────────────────────────────────────────────────────────

@functions_framework.http
def posterden_satuulardy_aluu(request):
    """HTTP Cloud Function — точка входа для Cloud Run / Cloud Scheduler."""
    body      = request.get_json(silent=True) or {}
    sync_type = body.get("sync_type", "incremental")
    date_from, date_to = _resolve_dates(body.get("date_from"), body.get("date_to"))

    try:
        result = _run_all(sync_type, date_from, date_to)
        return result, 200
    except Exception as exc:
        logger.exception("ETL failed")
        return {"status": "error", "message": str(exc)}, 500


# ── Local runner ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    # Парсинг аргументов
    # python main.py [full] [date_from] [date_to]
    sync_type = "incremental"
    date_from = None
    date_to   = None

    if args and args[0] == "full":
        sync_type = "full"
        args = args[1:]

    if len(args) >= 2:
        date_from, date_to = args[0], args[1]

    date_from, date_to = _resolve_dates(date_from, date_to)

    try:
        result = _run_all(sync_type, date_from, date_to)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as exc:
        logger.exception("ETL failed: %s", exc)
        sys.exit(1)
