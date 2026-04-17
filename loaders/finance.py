"""
Finance loader — финансовые операции.

Эндпоинты:
  finance.getCashShifts    → poster_cash_shifts_raw          (партиции по shift_date)
  finance.getTransactions  → poster_finance_transactions_raw (партиции по finance_date)

ВАЖНО: finance.getTransactions — финансовые операции (приход/расход кассы),
       НЕ путать с transactions.getTransactions (кассовые чеки продаж).

Все поля — 1:1 как возвращает Poster API.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from clients.bigquery import BigQueryClient
from clients.poster import PosterClient

logger = logging.getLogger(__name__)

BQ_CASH_SHIFTS = "poster_cash_shifts_raw"
BQ_FINANCE_TXN = "poster_finance_transactions_raw"


# ── Transform: кассовые смены ─────────────────────────────────────────────────

def _transform_cash_shift(
    s: dict,
    date_from: str,
    date_to: str,
    batch_id: str,
    load_ts: str,
) -> dict:
    def _poster_date(val) -> str | None:
        """Берём первые 10 символов и отбрасываем MySQL null '0000-00-00'."""
        d = (val or "")[:10]
        return d if d and d != "0000-00-00" else None

    shift_date = _poster_date(s.get("date_end")) or _poster_date(s.get("date_start"))

    return {
        "load_ts":            load_ts,
        "batch_id":           batch_id,
        "source_date_from":   date_from,
        "source_date_to":     date_to,
        "shift_date":         shift_date,           # DATE 'YYYY-MM-DD'
        "cash_shift_id":      str(s.get("cash_shift_id", "")),
        "spot_id":            str(s.get("spot_id", "")),
        "spot_name":          s.get("spot_name"),
        "spot_adress":        s.get("spot_adress"),
        "user_id_start":      str(s.get("user_id_start", "")),
        "user_id_end":        str(s.get("user_id_end", "")),
        "date_start":         s.get("date_start"),  # 'YYYY-MM-DD'
        "date_end":           s.get("date_end"),    # 'YYYY-MM-DD'
        "timestart":          s.get("timestart"),   # timestamp открытия
        "timeend":            s.get("timeend"),     # timestamp закрытия
        "amount_start":       float(s.get("amount_start")       or 0),  # остаток на начало смены
        "amount_end":         float(s.get("amount_end")         or 0),  # остаток на конец смены
        "amount_debit":       float(s.get("amount_debit")       or 0),  # внесения в кассу
        "amount_sell_cash":   float(s.get("amount_sell_cash")   or 0),  # выручка наличными
        "amount_sell_card":   float(s.get("amount_sell_card")   or 0),  # выручка картой
        "amount_credit":      float(s.get("amount_credit")      or 0),  # изъятия из кассы
        "amount_collection":  float(s.get("amount_collection")  or 0),  # инкассация
        "table_num":          int(s.get("table_num") or 0),
        "comment":            s.get("comment"),
        "raw_json":           json.dumps(s, ensure_ascii=False),
    }


# ── Transform: финансовые операции ───────────────────────────────────────────

def _transform_finance_txn(
    t: dict,
    date_from: str,
    date_to: str,
    batch_id: str,
    load_ts: str,
) -> dict:
    date_raw = t.get("date") or ""
    return {
        "load_ts":            load_ts,
        "batch_id":           batch_id,
        "source_date_from":   date_from,
        "source_date_to":     date_to,
        "finance_date":       date_raw[:10] or None,
        "transaction_id":     str(t.get("transaction_id", "")),
        "account_id":         str(t.get("account_id", "")),
        "account_name":       t.get("account_name"),
        "currency_symbol":    t.get("currency_symbol"),
        "user_id":            str(t.get("user_id", "")),
        "category_id":        str(t.get("category_id", "")),
        "category_name":      t.get("category_name"),
        "type":               str(t.get("type", "")),   # 1=приход, 2=расход
        "amount":             float(t.get("amount") or 0),
        "balance":            float(t.get("balance") or 0),
        "recipient_type":     str(t.get("recipient_type", "")),
        "recipient_id":       str(t.get("recipient_id", "")),
        "binding_type":       str(t.get("binding_type", "")),
        "binding_id":         str(t.get("binding_id", "")),
        "comment":            t.get("comment"),
        "agreement_date":     t.get("agreement_date"),
        "supplier_name":      t.get("supplier_name"),
        "is_deleted":         int(t.get("delete") or 0),
        "raw_json":           json.dumps(t, ensure_ascii=False),
    }


# ── Group by date ─────────────────────────────────────────────────────────────

def _group_by_date(rows: list[dict], date_field: str) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for row in rows:
        key = row.get(date_field) or "unknown"
        grouped.setdefault(key, []).append(row)
    return grouped


# ── Public entry point ────────────────────────────────────────────────────────

def run(date_from: str, date_to: str) -> dict:
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    logger.info("=== START finance loader | batch_id=%s ===", batch_id)
    logger.info("Period: %s → %s", date_from, date_to)

    poster = PosterClient()
    bq     = BigQueryClient()

    raw_shifts  = poster.get_paginated_by_date("finance.getCashShifts", date_from, date_to)
    shift_rows  = [_transform_cash_shift(s, date_from, date_to, batch_id, load_ts) for s in raw_shifts]
    n_shifts    = bq.write_by_date(BQ_CASH_SHIFTS, _group_by_date(shift_rows, "shift_date"))

    raw_txns    = poster.get_paginated_by_date("finance.getTransactions", date_from, date_to)
    txn_rows    = [_transform_finance_txn(t, date_from, date_to, batch_id, load_ts) for t in raw_txns]
    n_txns      = bq.write_by_date(BQ_FINANCE_TXN, _group_by_date(txn_rows, "finance_date"))

    logger.info("=== DONE finance | cash_shifts=%d finance_transactions=%d ===", n_shifts, n_txns)
    return {
        "status": "ok", "batch_id": batch_id,
        "date_from": date_from, "date_to": date_to,
        "cash_shifts": n_shifts, "finance_transactions": n_txns,
    }
