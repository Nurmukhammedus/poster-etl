"""
Storage loader — складские операции.

Эндпоинты:
  storage.getSupplies            → poster_supplies_raw   (партиции по supply_date)
  storage.getIngredientWriteOff  → poster_write_offs_raw (партиции по write_off_date)

Все поля — 1:1 как возвращает Poster API.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from clients.bigquery import BigQueryClient
from clients.poster import PosterClient

logger = logging.getLogger(__name__)

BQ_SUPPLIES  = "poster_supplies_raw"
BQ_WRITEOFFS = "poster_write_offs_raw"


# ── Transform: поставки ───────────────────────────────────────────────────────

def _transform_supply(
    s: dict,
    date_from: str,
    date_to: str,
    batch_id: str,
    load_ts: str,
) -> dict:
    date_raw = s.get("date") or ""
    return {
        "load_ts":          load_ts,
        "batch_id":         batch_id,
        "source_date_from": date_from,
        "source_date_to":   date_to,
        "supply_date":      date_raw[:10] or None,
        "supply_id":        str(s.get("supply_id", "")),
        "storage_id":       str(s.get("storage_id", "")),
        "storage_name":     s.get("storage_name"),
        "supplier_id":      str(s.get("supplier_id", "")),
        "supplier_name":    s.get("supplier_name"),
        "account_id":       str(s.get("account_id", "")),
        "supply_sum":       float(s.get("supply_sum")       or 0),  # сумма поставки (копейки)
        "supply_sum_netto": float(s.get("supply_sum_netto") or 0),
        "supply_comment":   s.get("supply_comment"),
        "is_deleted":       int(s.get("delete") or 0),
        "raw_json":         json.dumps(s, ensure_ascii=False),
    }


# ── Transform: списания ───────────────────────────────────────────────────────

def _transform_write_off(
    w: dict,
    date_from: str,
    date_to: str,
    batch_id: str,
    load_ts: str,
) -> dict:
    date_raw = w.get("date") or ""
    return {
        "load_ts":          load_ts,
        "batch_id":         batch_id,
        "source_date_from": date_from,
        "source_date_to":   date_to,
        "write_off_date":   date_raw[:10] or None,
        "write_off_id":     str(w.get("write_off_id", "")),
        "transaction_id":   str(w.get("transaction_id", "")),   # чек, к которому привязано списание
        "tr_product_id":    str(w.get("tr_product_id", "")),
        "storage_id":       str(w.get("storage_id", "")),
        "to_storage":       str(w.get("to_storage", "")),       # при перемещении между складами
        "ingredient_id":    str(w.get("ingredient_id", "")),
        "product_id":       str(w.get("product_id", "")),
        "modificator_id":   str(w.get("modificator_id", "")),
        "prepack_id":       str(w.get("prepack_id", "")),
        "name":             w.get("name"),                       # название ингредиента
        "product_name":     w.get("product_name"),               # название блюда (если привязано)
        "weight":           float(w.get("weight") or 0),         # количество списания
        "unit":             w.get("unit"),                       # единица измерения
        "cost":             float(w.get("cost")       or 0),    # себестоимость (копейки)
        "cost_netto":       float(w.get("cost_netto") or 0),
        "user_id":          str(w.get("user_id", "")),
        "type":             str(w.get("type", "")),              # тип списания
        "time":             w.get("time"),
        "reason":           w.get("reason"),
        "raw_json":         json.dumps(w, ensure_ascii=False),
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

    logger.info("=== START storage loader | batch_id=%s ===", batch_id)
    logger.info("Period: %s → %s", date_from, date_to)

    poster = PosterClient()
    bq     = BigQueryClient()

    raw_supplies     = poster.get_paginated_by_date("storage.getSupplies", date_from, date_to)
    supply_rows      = [_transform_supply(s, date_from, date_to, batch_id, load_ts) for s in raw_supplies]
    n_supplies       = bq.write_by_date(BQ_SUPPLIES, _group_by_date(supply_rows, "supply_date"))

    raw_writeoffs    = poster.get_paginated_by_date("storage.getIngredientWriteOff", date_from, date_to)
    writeoff_rows    = [_transform_write_off(w, date_from, date_to, batch_id, load_ts) for w in raw_writeoffs]
    n_writeoffs      = bq.write_by_date(BQ_WRITEOFFS, _group_by_date(writeoff_rows, "write_off_date"))

    logger.info("=== DONE storage | supplies=%d write_offs=%d ===", n_supplies, n_writeoffs)
    return {
        "status": "ok", "batch_id": batch_id,
        "date_from": date_from, "date_to": date_to,
        "supplies": n_supplies, "write_offs": n_writeoffs,
    }
