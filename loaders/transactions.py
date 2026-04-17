"""
Transactions loader — кассовые чеки.

Эндпоинт: transactions.getTransactions
Таблицы:
  poster_transactions_raw          (партиции по transaction_date)
  poster_transaction_products_raw  (партиции по transaction_date)

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


# ── Transform ─────────────────────────────────────────────────────────────────

def _transform(
    t: dict,
    date_from: str,
    date_to: str,
    page: int,
    batch_id: str,
    load_ts: str,
) -> tuple[dict, list[dict]]:
    """
    Один объект транзакции Poster → (строка чека, [строки товаров]).
    """
    transaction_id = t.get("transaction_id")
    products       = t.get("products", [])
    date_close_raw = t.get("date_close") or ""

    meta = {
        "load_ts":          load_ts,
        "batch_id":         batch_id,
        "source_date_from": date_from,
        "source_date_to":   date_to,
        "source_page":      page,
    }

    transaction_row = {
        **meta,
        "transaction_date":   date_close_raw[:10] or None,
        "transaction_id":     str(transaction_id or ""),
        "table_id":           str(t.get("table_id", "")),
        "spot_id":            str(t.get("spot_id", "")),
        "client_id":          str(t.get("client_id", "")),
        "application_id":     str(t.get("application_id", "")),
        "sum":                float(t.get("sum")               or 0),
        "payed_sum":          float(t.get("payed_sum")         or 0),
        "payed_cash":         float(t.get("payed_cash")        or 0),
        "payed_card":         float(t.get("payed_card")        or 0),
        "payed_card_type":    str(t.get("payed_card_type", "")),
        "payed_cert":         float(t.get("payed_cert")        or 0),
        "payed_bonus":        float(t.get("payed_bonus")       or 0),
        "payed_third_party":  float(t.get("payed_third_party") or 0),
        "round_sum":          float(t.get("round_sum")         or 0),
        "tip_sum":            float(t.get("tip_sum")           or 0),
        "tips_cash":          float(t.get("tips_cash")         or 0),   # чаевые наличными
        "tips_card":          float(t.get("tips_card")         or 0),   # чаевые картой
        "bonus":              float(t.get("bonus")             or 0),
        "discount":           float(t.get("discount")          or 0),
        "total_profit":       float(t.get("total_profit")      or 0),   # прибыль (копейки)
        "total_profit_netto": float(t.get("total_profit_netto") or 0),
        "pay_type":           str(t.get("pay_type", "")),
        "reason":             t.get("reason"),
        "print_fiscal":       str(t.get("print_fiscal", "")),
        "auto_accept":        int(t.get("auto_accept") or 0),
        "date_close":         date_close_raw or None,
        "raw_json":           json.dumps(t, ensure_ascii=False),
    }

    product_rows = []
    for i, p in enumerate(products):
        product_rows.append({
            **meta,
            "transaction_id":       str(transaction_id or ""),
            "transaction_date":     date_close_raw[:10] or None,
            "line_number":          i + 1,
            "product_id":           str(p.get("product_id", "")),
            "modification_id":      str(p.get("modification_id", "")),
            "type":                 str(p.get("type", "")),
            "workshop_id":          str(p.get("workshop_id", "")),
            "num":                  float(p.get("num")             or 0),
            "printed_num":          float(p.get("printed_num")     or 0),
            "product_sum":          float(p.get("product_sum")     or 0),
            "payed_sum":            float(p.get("payed_sum")       or 0),
            "cert_sum":             float(p.get("cert_sum")        or 0),
            "bonus_sum":            float(p.get("bonus_sum")       or 0),
            "bonus_accrual":        float(p.get("bonus_accrual")   or 0),
            "round_sum":            float(p.get("round_sum")       or 0),
            "discount":             float(p.get("discount")        or 0),
            "product_cost":         float(p.get("product_cost")    or 0),   # себестоимость
            "product_cost_netto":   float(p.get("product_cost_netto") or 0),
            "product_profit":       float(p.get("product_profit")  or 0),   # прибыль (копейки)
            "product_profit_netto": float(p.get("product_profit_netto") or 0),
            "fiscal_company_id":    str(p.get("fiscal_company_id", "")),
            "print_fiscal":         str(p.get("print_fiscal", "")),
            "tax_id":               str(p.get("tax_id", "")),
            "tax_value":            float(p.get("tax_value") or 0),
            "tax_type":             str(p.get("tax_type", "")),
            "tax_fiscal":           float(p.get("tax_fiscal") or 0),
            "tax_sum":              float(p.get("tax_sum")    or 0),
            "raw_json":             json.dumps(p, ensure_ascii=False),
        })

    return transaction_row, product_rows


# ── Group by date ─────────────────────────────────────────────────────────────

def _group_by_date(
    raw_rows: list[dict],
    date_from: str,
    date_to: str,
    batch_id: str,
    load_ts: str,
) -> tuple[dict[str, list], dict[str, list]]:
    transactions_by_date: dict[str, list] = {}
    products_by_date:     dict[str, list] = {}

    for page_num, t in enumerate(raw_rows, start=1):
        tr_row, pr_rows = _transform(t, date_from, date_to, page_num, batch_id, load_ts)
        date_key = (t.get("date_close") or "")[:10] or "unknown"
        transactions_by_date.setdefault(date_key, []).append(tr_row)
        products_by_date.setdefault(date_key, []).extend(pr_rows)

    return transactions_by_date, products_by_date


# ── Public entry point ────────────────────────────────────────────────────────

def run(date_from: str, date_to: str) -> dict:
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    logger.info("=== START transactions loader | batch_id=%s ===", batch_id)
    logger.info("Period: %s → %s", date_from, date_to)

    poster   = PosterClient()
    raw_rows = poster.get_transactions(date_from, date_to)
    logger.info("Extracted %d transactions from Poster", len(raw_rows))

    transactions_by_date, products_by_date = _group_by_date(
        raw_rows, date_from, date_to, batch_id, load_ts
    )

    bq = BigQueryClient()
    total_transactions = bq.write_by_date(BQ_TABLE_TRANSACTIONS, transactions_by_date)
    total_products     = bq.write_by_date(BQ_TABLE_PRODUCTS,     products_by_date)

    logger.info(
        "=== DONE transactions | transactions=%d products=%d ===",
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
