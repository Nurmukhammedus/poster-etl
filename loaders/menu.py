"""
Menu loader — справочник меню.

Эндпоинты:
  menu.getCategories → poster_categories_raw  (полная перезапись)
  menu.getProducts   → poster_products_raw    (полная перезапись)

Все поля — 1:1 как возвращает Poster API.
Вложенные объекты (price, spots) сохраняются как JSON-строка в отдельном поле.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from clients.bigquery import BigQueryClient
from clients.poster import PosterClient

logger = logging.getLogger(__name__)

BQ_CATEGORIES = "poster_categories_raw"
BQ_PRODUCTS   = "poster_products_raw"


# ── Transform: категории ──────────────────────────────────────────────────────

def _transform_category(c: dict, batch_id: str, load_ts: str) -> dict:
    return {
        "load_ts":              load_ts,
        "batch_id":             batch_id,
        "category_id":          str(c.get("category_id", "")),
        "category_name":        c.get("category_name"),
        "category_photo":       c.get("category_photo"),
        "category_photo_origin":c.get("category_photo_origin"),
        "parent_category":      c.get("parent_category"),
        "category_color":       c.get("category_color"),
        "category_tag":         c.get("category_tag"),
        "category_hidden":      int(c.get("category_hidden") or 0),
        "sort_order":           int(c.get("sort_order")       or 0),
        "fiscal":               int(c.get("fiscal")           or 0),
        "nodiscount":           int(c.get("nodiscount")       or 0),
        "tax_id":               str(c.get("tax_id", "")),
        "category_left":        int(c.get("left")             or 0),
        "category_right":       int(c.get("right")            or 0),
        "category_level":       int(c.get("level")            or 0),
        "visible":              json.dumps(c.get("visible"), ensure_ascii=False) if c.get("visible") is not None else None,
        "raw_json":             json.dumps(c, ensure_ascii=False),
    }


# ── Transform: товары/блюда ───────────────────────────────────────────────────

def _dict_or_float(val) -> tuple[str | None, float]:
    """
    Poster возвращает некоторые числовые поля (price, cost, profit) как
    dict {"spot_id": "amount"} когда у точек разные значения,
    или как обычное число/строку.
    Возвращает (json_string | None, default_float).
    """
    if isinstance(val, dict):
        json_str = json.dumps(val, ensure_ascii=False) if val else None
        first = float(list(val.values())[0] or 0) if val else 0.0
        return json_str, first
    if val is not None and val != "":
        try:
            return None, float(val)
        except (ValueError, TypeError):
            pass
    return None, 0.0


def _transform_product(p: dict, batch_id: str, load_ts: str) -> dict:
    # price, cost, profit — могут быть dict {spot_id: amount} или числом
    price_json,  default_price  = _dict_or_float(p.get("price"))
    cost_json,   default_cost   = _dict_or_float(p.get("cost"))
    profit_json, default_profit = _dict_or_float(p.get("profit"))

    cost_netto_raw = p.get("cost_netto")
    _, default_cost_netto = _dict_or_float(cost_netto_raw)

    return {
        "load_ts":                  load_ts,
        "batch_id":                 batch_id,
        "product_id":               str(p.get("product_id", "")),
        "product_name":             p.get("product_name"),
        "product_code":             p.get("product_code"),
        "barcode":                  p.get("barcode"),
        "category_id":              str(p.get("category_id", "")) if p.get("category_id") else None,
        "category_name":            p.get("category_name"),
        "menu_category_id":         str(p.get("menu_category_id", "")),
        "type":                     str(p.get("type", "")),       # 1=полуфабрикат, 2=тех.карта, 3=товар
        "unit":                     str(p.get("unit", "")),
        "weight_flag":              int(p.get("weight_flag") or 0),
        "color":                    p.get("color"),
        "photo":                    p.get("photo"),
        "photo_origin":             p.get("photo_origin"),
        "price":                    price_json,       # JSON: {spot_id: price} или None
        "price_default":            default_price,    # первая/единая цена для удобства
        "cost":                     default_cost,     # себестоимость (копейки)
        "cost_netto":               default_cost_netto,
        "profit":                   default_profit,   # прибыль (копейки)
        "fiscal":                   int(p.get("fiscal")           or 0),
        "nodiscount":               int(p.get("nodiscount")       or 0),
        "hidden":                   int(p.get("hidden")           or 0),
        "sort_order":               int(p.get("sort_order")       or 0),
        "tax_id":                   str(p.get("tax_id", "")),
        "product_tax_id":           str(p.get("product_tax_id", "")),
        "workshop":                 str(p.get("workshop", "")),
        "ingredient_id":            str(p.get("ingredient_id", "")),
        "master_id":                str(p.get("master_id", "")),
        "different_spots_prices":        int(p.get("different_spots_prices") or 0),
        # out = сумма нетто всех ингредиентов тех.карты; для товара = 0
        "out_netto":                     float(p.get("out") or 0),
        "cooking_time":                  int(p.get("cooking_time") or 0),       # сек
        "product_production_description":p.get("product_production_description"),
        "fiscal_code":                   p.get("fiscal_code"),
        "spots":                         json.dumps(p.get("spots"),             ensure_ascii=False) if p.get("spots") else None,
        "sources":                       json.dumps(p.get("sources"),           ensure_ascii=False) if p.get("sources") else None,
        "modifications":                 json.dumps(p.get("modifications"),     ensure_ascii=False) if p.get("modifications") else None,
        "group_modifications":           json.dumps(p.get("group_modifications"),ensure_ascii=False) if p.get("group_modifications") else None,
        "raw_json":                      json.dumps(p, ensure_ascii=False),
    }


# ── Public entry point ────────────────────────────────────────────────────────

def run() -> dict:
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    logger.info("=== START menu loader | batch_id=%s ===", batch_id)

    poster = PosterClient()
    bq     = BigQueryClient()

    raw_categories = poster.get_reference("menu.getCategories")
    cat_rows       = [_transform_category(c, batch_id, load_ts) for c in raw_categories]
    n_cats         = bq.write_table(BQ_CATEGORIES, cat_rows)

    raw_products = poster.get_reference("menu.getProducts")
    prod_rows    = [_transform_product(p, batch_id, load_ts) for p in raw_products]
    n_prods      = bq.write_table(BQ_PRODUCTS, prod_rows)

    logger.info("=== DONE menu | categories=%d products=%d ===", n_cats, n_prods)
    return {"status": "ok", "batch_id": batch_id, "categories": n_cats, "products": n_prods}
