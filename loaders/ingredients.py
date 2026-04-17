"""
Ingredients & Tech Cards loader.

Эндпоинты:
  menu.getIngredients → poster_ingredients_raw   (справочник ингредиентов)
  menu.getProducts    → poster_tech_cards_raw    (тех карты блюд)

Поля потерь в тех карте (structure item):
  pr_in_clear / ingredients_losses_clear  — холодная обработка (чистка)
  pr_in_cook  / ingredients_losses_cook   — варка
  pr_in_fry   / ingredients_losses_fry    — жарка
  pr_in_stew  / ingredients_losses_stew   — тушение
  pr_in_bake  / ingredients_losses_bake   — запекание

  pr_in_*             = % потерь (процент)
  ingredients_losses_* = абсолютные потери в граммах

  structure_selfprice — себестоимость (в копейках, делить на 100 в stg слое)
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from clients.bigquery import BigQueryClient
from clients.poster import PosterClient

logger = logging.getLogger(__name__)

BQ_INGREDIENTS = "poster_ingredients_raw"
BQ_TECH_CARDS  = "poster_tech_cards_raw"


# ── Transform: ингредиенты ────────────────────────────────────────────────────

def _transform_ingredient(ing: dict, batch_id: str, load_ts: str) -> dict:
    return {
        "load_ts":                  load_ts,
        "batch_id":                 batch_id,
        "ingredient_id":            str(ing.get("ingredient_id", "")),
        "ingredient_name":          ing.get("ingredient_name"),
        "ingredient_barcode":       ing.get("ingredient_barcode"),
        "category_id":              str(ing.get("category_id", "")),
        "ingredient_unit":          ing.get("ingredient_unit"),  # kg, g, l, шт и т.д.
        "ingredient_weight":        float(ing.get("ingredient_weight") or 0),
        "ingredient_left":          float(ing.get("ingredient_left")   or 0),  # текущий остаток
        "limit_value":              float(ing.get("limit_value")       or 0),  # минимальный остаток
        "ingredients_type":         int(ing.get("ingredients_type")    or 0),
        "partial_write_off":        int(ing.get("partial_write_off")   or 0),
        # Потери по типам обработки (%)
        "losses_clear":             float(ing.get("ingredients_losses_clear") or 0),
        "losses_cook":              float(ing.get("ingredients_losses_cook")  or 0),
        "losses_fry":               float(ing.get("ingredients_losses_fry")   or 0),
        "losses_stew":              float(ing.get("ingredients_losses_stew")  or 0),
        "losses_bake":              float(ing.get("ingredients_losses_bake")  or 0),
        "raw_json":                 json.dumps(ing, ensure_ascii=False),
    }


# ── Transform: тех карты ──────────────────────────────────────────────────────

def _extract_tech_card_rows(
    product: dict,
    batch_id: str,
    load_ts: str,
) -> list[dict]:
    """
    Разворачивает structure продукта в плоские строки тех карты.
    Каждая строка = один ингредиент в составе блюда.
    """
    # В menu.getProduct тех карта лежит в поле `ingredients` (не `structure`)
    structure = product.get("ingredients") or product.get("structure") or []

    # может быть dict {index: item} вместо list
    if isinstance(structure, dict):
        structure = list(structure.values())

    product_id   = str(product.get("product_id", ""))
    product_name = product.get("product_name")
    rows = []

    for line_num, item in enumerate(structure, start=1):
        rows.append({
            "load_ts":          load_ts,
            "batch_id":         batch_id,
            "product_id":       product_id,
            "product_name":     product_name,
            "line_number":      line_num,

            # Идентификаторы
            "structure_id":     str(item.get("structure_id", "")),
            "ingredient_id":    str(item.get("ingredient_id", "")),
            "ingredient_name":  item.get("ingredient_name"),
            "structure_type":   str(item.get("structure_type", "")),  # 1=ингредиент, 2=полуфабрикат

            # Единицы измерения
            "structure_unit":   item.get("structure_unit"),   # единица в тех карте (g, kg…)
            "ingredient_unit":  item.get("ingredient_unit"),  # единица хранения ингредиента

            # Веса
            "structure_brutto": float(item.get("structure_brutto") or 0),  # брутто, г
            "structure_netto":  float(item.get("structure_netto")  or 0),  # нетто, г
            "ingredient_weight": float(item.get("ingredient_weight") or 0),
            "structure_lock":   str(item.get("structure_lock", "")),  # флаг фиксации

            # Себестоимость (в копейках — делить на 100 в stg слое)
            "structure_selfprice":       float(item.get("structure_selfprice")       or 0),
            "structure_selfprice_netto": float(item.get("structure_selfprice_netto") or 0),

            # Флаги метода приготовления (0/1): используется ли данный метод
            "pr_in_clear": int(item.get("pr_in_clear") or 0),  # очистка
            "pr_in_cook":  int(item.get("pr_in_cook")  or 0),  # запекание
            "pr_in_fry":   int(item.get("pr_in_fry")   or 0),  # жарка
            "pr_in_stew":  int(item.get("pr_in_stew")  or 0),  # тушение
            "pr_in_bake":  int(item.get("pr_in_bake")  or 0),  # варка

            # Коэффициенты потерь по методу приготовления
            "losses_clear": float(item.get("ingredients_losses_clear") or 0),  # очистка
            "losses_cook":  float(item.get("ingredients_losses_cook")  or 0),  # запекание
            "losses_fry":   float(item.get("ingredients_losses_fry")   or 0),  # жарка
            "losses_stew":  float(item.get("ingredients_losses_stew")  or 0),  # тушение
            "losses_bake":  float(item.get("ingredients_losses_bake")  or 0),  # варка

            "raw_json": json.dumps(item, ensure_ascii=False),
        })

    return rows


# ── Public entry point ────────────────────────────────────────────────────────

def run() -> dict:
    """
    Полная перезапись справочников ингредиентов и тех карт.
    Вызывается при full-синхронизации.
    """
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    logger.info("=== START ingredients loader | batch_id=%s ===", batch_id)

    poster = PosterClient()
    bq     = BigQueryClient()

    # 1. Ингредиенты
    raw_ingredients = poster.get_reference("menu.getIngredients")
    ing_rows        = [_transform_ingredient(i, batch_id, load_ts) for i in raw_ingredients]
    n_ingredients   = bq.write_table(BQ_INGREDIENTS, ing_rows)
    logger.info("Ingredients: %d", n_ingredients)

    # 2. Тех карты — menu.getProducts?type=batchtickets возвращает тех.карты
    #    с полем `ingredients` прямо в списке (один запрос, без перебора по ID)
    raw_tech_cards     = poster.get_reference("menu.getProducts", {"type": "batchtickets"})
    tech_rows          = []
    products_with_card = 0

    for product in raw_tech_cards:
        rows = _extract_tech_card_rows(product, batch_id, load_ts)
        if rows:
            tech_rows.extend(rows)
            products_with_card += 1

    logger.info(
        "Tech cards: %d/%d products have ingredients, %d total lines",
        products_with_card, len(raw_tech_cards), len(tech_rows),
    )

    n_tech_cards = bq.write_table(BQ_TECH_CARDS, tech_rows)

    logger.info(
        "=== DONE ingredients | ingredients=%d tech_card_lines=%d ===",
        n_ingredients, n_tech_cards,
    )

    return {
        "status":          "ok",
        "batch_id":        batch_id,
        "ingredients":     n_ingredients,
        "tech_card_lines": n_tech_cards,
    }
