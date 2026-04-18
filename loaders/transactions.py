"""
Transactions loader — кассовые чеки.

Гибрид двух эндпоинтов, нулевое дублирование полей:

  dash.getTransactions           →  poster_transactions_raw
    (богатая ШАПКА: официант, гости, статус, клиент, комментарий, налоги)

  transactions.getTransactions   →  poster_transaction_products_raw
    (богатые СТРОКИ: workshop_id, type, discount на строку, bonus/cert_sum,
     printed_num, round_sum, product_sum)

JOIN на transaction_id для аналитики.

Почему так: workshop_id блюда может со временем измениться (перенос блюда
из кухни в бар). Хранить его в DIM `poster_products_raw` и JOIN-ить — значит
искажать исторические отчёты. Поэтому workshop_id сохраняем на момент
продажи, из transactions.getTransactions — это fact-level denormalization
(альтернатива SCD Type 2).

Все поля — 1:1 как возвращает Poster API.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from clients.bigquery import BigQueryClient
from clients.poster import PosterClient

logger = logging.getLogger(__name__)

BQ_TABLE_TRANSACTIONS = "poster_transactions_raw"
BQ_TABLE_PRODUCTS     = "poster_transaction_products_raw"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_int(val) -> int | None:
    try:
        return int(val) if val not in (None, "", "null") else None
    except (TypeError, ValueError):
        return None


def _to_float(val) -> float:
    try:
        return float(val) if val not in (None, "", "null") else 0.0
    except (TypeError, ValueError):
        return 0.0


# ── Transform: шапка из dash.getTransactions ─────────────────────────────────

def _transform_header(
    t: dict,
    date_from: str,
    date_to: str,
    page: int,
    batch_id: str,
    load_ts: str,
) -> dict:
    """dash.getTransactions → строка poster_transactions_raw."""
    date_close_date = t.get("date_close_date") or ""   # "YYYY-MM-DD HH:MM:SS"
    date_close_ms   = _to_int(t.get("date_close"))      # Unix timestamp (ms)

    return {
        "load_ts":             load_ts,
        "batch_id":            batch_id,
        "source_date_from":    date_from,
        "source_date_to":      date_to,
        "source_page":         page,

        "transaction_date":    date_close_date[:10] or None,
        "transaction_id":      str(t.get("transaction_id") or ""),
        "spot_id":             str(t.get("spot_id", "")),
        "table_id":            str(t.get("table_id", "")),
        "table_name":          t.get("table_name"),

        # ── бизнес-контекст ──
        "guests_count":        _to_int(t.get("guests_count")) or 0,
        "user_id":             str(t.get("user_id", "")),
        "waiter_name":         t.get("name"),
        "status":              str(t.get("status", "")),
        "processing_status":   str(t.get("processing_status", "")),
        "service_mode":        str(t.get("service_mode", "")),
        "transaction_comment": t.get("transaction_comment"),
        "reason":              t.get("reason"),

        # ── клиент ──
        "client_id":           str(t.get("client_id", "")),
        "client_firstname":    t.get("client_firstname"),
        "client_lastname":     t.get("client_lastname"),
        "client_phone":        t.get("client_phone"),
        "card_number":         str(t.get("card_number", "")),

        # ── суммы ──
        "sum":                 _to_float(t.get("sum")),
        "payed_sum":           _to_float(t.get("payed_sum")),
        "payed_cash":          _to_float(t.get("payed_cash")),
        "payed_card":          _to_float(t.get("payed_card")),
        "payed_card_type":     str(t.get("payed_card_type", "")),
        "payed_cert":          _to_float(t.get("payed_cert")),
        "payed_bonus":         _to_float(t.get("payed_bonus")),
        "payed_third_party":   _to_float(t.get("payed_third_party")),
        "payed_ewallet":       _to_float(t.get("payed_ewallet")),
        "round_sum":           _to_float(t.get("round_sum")),
        "tip_sum":             _to_float(t.get("tip_sum")),
        "tips_cash":           _to_float(t.get("tips_cash")),
        "tips_card":           _to_float(t.get("tips_card")),
        "bonus":               _to_float(t.get("bonus")),
        "discount":            _to_float(t.get("discount")),
        "tax_sum":             _to_float(t.get("tax_sum")),
        "total_profit":        _to_float(t.get("total_profit")),
        "total_profit_netto":  _to_float(t.get("total_profit_netto")),

        # ── оплата / фискализация ──
        "pay_type":            str(t.get("pay_type", "")),
        "payment_method_id":   str(t.get("payment_method_id", "")),
        "print_fiscal":        str(t.get("print_fiscal", "")),
        "auto_accept":         int(bool(t.get("auto_accept"))),
        "application_id":      str(t.get("application_id") or ""),

        # ── даты ──
        "date_close":          date_close_date or None,   # "YYYY-MM-DD HH:MM:SS"
        "date_close_ms":       date_close_ms,             # Unix ms

        # ── полный json для форензики ──
        "raw_json":            json.dumps(t, ensure_ascii=False),
    }


# ── Transform: строки из transactions.getTransactions ────────────────────────

def _transform_lines(
    t: dict,
    date_from: str,
    date_to: str,
    page: int,
    batch_id: str,
    load_ts: str,
) -> list[dict]:
    """transactions.getTransactions → строки poster_transaction_products_raw."""
    transaction_id = str(t.get("transaction_id") or "")
    products       = t.get("products") or []
    date_close_raw = t.get("date_close") or ""    # "YYYY-MM-DD HH:MM:SS"
    transaction_date = date_close_raw[:10] or None

    meta = {
        "load_ts":          load_ts,
        "batch_id":         batch_id,
        "source_date_from": date_from,
        "source_date_to":   date_to,
        "source_page":      page,
    }

    rows = []
    for i, p in enumerate(products):
        rows.append({
            **meta,
            "transaction_id":       transaction_id,
            "transaction_date":     transaction_date,
            "line_number":          i + 1,
            "product_id":           str(p.get("product_id", "")),
            "modification_id":      str(p.get("modification_id", "")),
            "type":                 str(p.get("type", "")),           # тип товара на момент продажи
            "workshop_id":          str(p.get("workshop_id", "")),    # цех на момент продажи (SCD)
            "num":                  _to_float(p.get("num")),
            "printed_num":          _to_float(p.get("printed_num")),
            "product_sum":          _to_float(p.get("product_sum")),
            "payed_sum":            _to_float(p.get("payed_sum")),
            "cert_sum":             _to_float(p.get("cert_sum")),
            "bonus_sum":            _to_float(p.get("bonus_sum")),
            "bonus_accrual":        _to_float(p.get("bonus_accrual")),
            "round_sum":            _to_float(p.get("round_sum")),
            "discount":             _to_float(p.get("discount")),
            "product_cost":         _to_float(p.get("product_cost")),
            "product_cost_netto":   _to_float(p.get("product_cost_netto")),
            "product_profit":       _to_float(p.get("product_profit")),
            "product_profit_netto": _to_float(p.get("product_profit_netto")),
            "fiscal_company_id":    str(p.get("fiscal_company_id", "")),
            "print_fiscal":         str(p.get("print_fiscal", "")),
            "tax_id":               str(p.get("tax_id", "")),
            "tax_value":            _to_float(p.get("tax_value")),
            "tax_type":             str(p.get("tax_type", "")),
            "tax_fiscal":           _to_float(p.get("tax_fiscal")),
            "tax_sum":              _to_float(p.get("tax_sum")),
            "raw_json":             json.dumps(p, ensure_ascii=False),
        })

    return rows


# ── Group by date ─────────────────────────────────────────────────────────────

def _group(rows: list[dict], date_key: str) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for r in rows:
        key = r.get(date_key) or "unknown"
        grouped.setdefault(key, []).append(r)
    return grouped


# ── Public entry point ────────────────────────────────────────────────────────

def run(date_from: str, date_to: str) -> dict:
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    logger.info("=== START transactions loader | batch_id=%s ===", batch_id)
    logger.info("Period: %s → %s", date_from, date_to)

    poster = PosterClient()

    # ── Шаг 1. Fetch из обоих эндпоинтов (в память, до записи) ──
    dash_raw = poster.get_dash_transactions(date_from, date_to)
    logger.info("Extracted %d headers from dash.getTransactions", len(dash_raw))

    tx_raw = poster.get_transactions(date_from, date_to)
    logger.info("Extracted %d transactions (with products) from transactions.getTransactions", len(tx_raw))

    # Sanity check: количества должны совпадать; если нет — логируем, но не падаем.
    dash_ids = {str(r.get("transaction_id") or "") for r in dash_raw}
    tx_ids   = {str(r.get("transaction_id") or "") for r in tx_raw}
    missing_in_tx   = dash_ids - tx_ids
    missing_in_dash = tx_ids - dash_ids
    if missing_in_tx:
        logger.warning(
            "Есть %d чеков в dash, которых нет в transactions (например: %s)",
            len(missing_in_tx), list(missing_in_tx)[:5],
        )
    if missing_in_dash:
        logger.warning(
            "Есть %d чеков в transactions, которых нет в dash (например: %s)",
            len(missing_in_dash), list(missing_in_dash)[:5],
        )

    # ── Шаг 2. Transform ──
    header_rows = [
        _transform_header(t, date_from, date_to, i + 1, batch_id, load_ts)
        for i, t in enumerate(dash_raw)
    ]

    line_rows: list[dict] = []
    for i, t in enumerate(tx_raw):
        line_rows.extend(_transform_lines(t, date_from, date_to, i + 1, batch_id, load_ts))

    headers_by_date = _group(header_rows, "transaction_date")
    lines_by_date   = _group(line_rows,   "transaction_date")

    # ── Шаг 3. Write в BigQuery ──
    bq = BigQueryClient()
    total_transactions = bq.write_by_date(BQ_TABLE_TRANSACTIONS, headers_by_date)
    total_products     = bq.write_by_date(BQ_TABLE_PRODUCTS,     lines_by_date)

    logger.info(
        "=== DONE transactions | headers=%d products=%d ===",
        total_transactions, total_products,
    )
    return {
        "status":       "ok",
        "batch_id":     batch_id,
        "date_from":    date_from,
        "date_to":      date_to,
        "transactions": total_transactions,
        "products":     total_products,
    }
