"""
Directory loader — справочники: точки продаж, сотрудники, клиенты.

Эндпоинты:
  access.getSpots     → poster_spots_raw      (полная перезапись)
  access.getEmployees → poster_employees_raw  (полная перезапись)
  clients.getClients  → poster_clients_raw    (полная перезапись)

Все поля — 1:1 как возвращает Poster API.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from clients.bigquery import BigQueryClient
from clients.poster import PosterClient

logger = logging.getLogger(__name__)

BQ_SPOTS     = "poster_spots_raw"
BQ_EMPLOYEES = "poster_employees_raw"
BQ_CLIENTS   = "poster_clients_raw"


# ── Transform: точки ──────────────────────────────────────────────────────────

def _transform_spot(s: dict, batch_id: str, load_ts: str) -> dict:
    return {
        "load_ts":     load_ts,
        "batch_id":    batch_id,
        "spot_id":     str(s.get("spot_id", "")),
        "name":        s.get("name"),
        "spot_name":   s.get("spot_name"),
        "spot_adress": s.get("spot_adress"),
        "region_id":   str(s.get("region_id", "")),
        "lat":         float(s.get("lat") or 0),
        "lng":         float(s.get("lng") or 0),
        "storages":    json.dumps(s.get("storages"), ensure_ascii=False) if s.get("storages") else None,
        "raw_json":    json.dumps(s, ensure_ascii=False),
    }


# ── Transform: сотрудники ─────────────────────────────────────────────────────

def _transform_employee(e: dict, batch_id: str, load_ts: str) -> dict:
    return {
        "load_ts":     load_ts,
        "batch_id":    batch_id,
        "user_id":     str(e.get("user_id", "")),   # ← в API это user_id, не employee_id
        "name":        e.get("name"),
        "login":       e.get("login"),
        "role_id":     str(e.get("role_id", "")),
        "role_name":   e.get("role_name"),
        "user_type":   str(e.get("user_type", "")),
        "access_mask": str(e.get("access_mask", "")),
        "phone":       e.get("phone"),
        "last_in":     e.get("last_in"),
        "raw_json":    json.dumps(e, ensure_ascii=False),
    }


# ── Transform: клиенты ───────────────────────────────────────────────────────

def _transform_client(c: dict, batch_id: str, load_ts: str) -> dict:
    return {
        "load_ts":                  load_ts,
        "batch_id":                 batch_id,
        "client_id":                str(c.get("client_id", "")),
        "firstname":                c.get("firstname"),
        "lastname":                 c.get("lastname"),
        "patronymic":               c.get("patronymic"),
        "phone":                    c.get("phone"),
        "phone_number":             c.get("phone_number"),
        "email":                    c.get("email"),
        "birthday":                 c.get("birthday"),
        "card_number":              c.get("card_number"),
        "client_sex":               str(c.get("client_sex", "")),
        "country":                  c.get("country"),
        "city":                     c.get("city"),
        "address":                  c.get("address"),
        "comment":                  c.get("comment"),
        "government_id":            c.get("government_id"),
        "discount_per":             float(c.get("discount_per")    or 0),
        "bonus":                    float(c.get("bonus")           or 0),
        "birthday_bonus":           float(c.get("birthday_bonus")  or 0),
        "ewallet":                  float(c.get("ewallet")         or 0),
        "total_payed_sum":          float(c.get("total_payed_sum") or 0),
        "loyalty_type":             str(c.get("loyalty_type", "")),
        "client_groups_id":         str(c.get("client_groups_id", "")),
        "client_groups_name":       c.get("client_groups_name"),
        "client_groups_discount":   float(c.get("client_groups_discount") or 0),
        "date_activale":            c.get("date_activale"),
        "is_deleted":               int(c.get("delete") or 0),
        "addresses":                json.dumps(c.get("addresses"), ensure_ascii=False) if c.get("addresses") else None,
        "raw_json":                 json.dumps(c, ensure_ascii=False),
    }


# ── Public entry point ────────────────────────────────────────────────────────

def run() -> dict:
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    logger.info("=== START directory loader | batch_id=%s ===", batch_id)

    poster = PosterClient()
    bq     = BigQueryClient()

    raw_spots = poster.get_reference("access.getSpots")
    spot_rows = [_transform_spot(s, batch_id, load_ts) for s in raw_spots]
    n_spots   = bq.write_table(BQ_SPOTS, spot_rows)

    raw_employees = poster.get_reference("access.getEmployees")
    emp_rows      = [_transform_employee(e, batch_id, load_ts) for e in raw_employees]
    n_employees   = bq.write_table(BQ_EMPLOYEES, emp_rows)

    raw_clients = poster.get_reference("clients.getClients")
    client_rows = [_transform_client(c, batch_id, load_ts) for c in raw_clients]
    n_clients   = bq.write_table(BQ_CLIENTS, client_rows)

    logger.info(
        "=== DONE directory | spots=%d employees=%d clients=%d ===",
        n_spots, n_employees, n_clients,
    )
    return {
        "status": "ok", "batch_id": batch_id,
        "spots": n_spots, "employees": n_employees, "clients": n_clients,
    }
