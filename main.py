from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from dotenv import load_dotenv
import requests
import json
import uuid
import math
import os

load_dotenv()

# Константы проекта
BQ_PROJECT            = "posterquery"
BQ_DATASET            = "poster_analitics"
BQ_TABLE_TRANSACTIONS = "poster_transactions_raw"
BQ_TABLE_PRODUCTS     = "poster_transaction_products_raw"
PER_PAGE              = 100  # Poster'дын бир барактагы максималдуу жазуу саны


def aluu_bq_client():
    """Вспомогательная функция — возвращает BigQuery клиент."""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    return bigquery.Client(project=BQ_PROJECT)


def bq_ke_jaz(client, table_name, rows, date_str=None):
    """
    BigQuery'ге саптарды жазат.
    date_str берилсе → ошол күндүн партициясына WRITE_TRUNCATE.
    date_str=None    → бүт таблицага WRITE_TRUNCATE (толук жүктөө).
    """
    if not rows:
        print(f"[{table_name}] Жазуу үчүн маалымат жок")
        return 0

    parent_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

    if date_str:
        date_nodash = date_str.replace("-", "")
        table_ref = f"{parent_ref}${date_nodash}"
    else:
        table_ref = parent_ref

    # Таблицанын схемасын BigQuery'ден алабыз — авто-аныктоону өчүрүү үчүн
    table_schema = client.get_table(parent_ref).schema

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=table_schema,
    )

    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()  # аяктаганча күтөбүз

    print(f"[{table_name}] {len(rows)} сап жазылды → {table_ref}")
    return len(rows)


def poster_baragyn_aluu(url, params, page):
    """
    Poster API'нен бир баракты алат.
    Ар бир чакыруу = бир HTTP суроо = бир баракчадагы чектер.
    """
    params["page"] = page

    try:
        response = requests.get(url, params=params, timeout=180)
        response.raise_for_status()
    except requests.exceptions.ConnectionError:
        return None, {"status": "Ishten chykty", "maalymdama": "Poster API baylanysh jok"}, 502
    except requests.exceptions.Timeout:
        return None, {"status": "Ishten chykty", "maalymdama": "Poster API TimeOut"}, 504
    except requests.exceptions.HTTPError as e:
        return None, {"status": "Ishten chykty", "maalymdama": "HTTP kata", "shilteme": str(e)}, 502
    except Exception as e:
        return None, {"status": "Ishten chykty", "maalymdama": "Күтүлбөгөн ката", "shilteme": str(e)}, 500

    try:
        joop = response.json()
    except Exception:
        return None, {"status": "Ishten chykty", "maalymdama": "JSON parse ката", "text": response.text[:500]}, 502

    if "error" in joop:
        return None, {"status": "Ishten chykty", "maalymdama": joop["error"]}, 502

    return joop, None, None


def transformacia(t, bashy, ayagy, page, batch_id, load_ts):
    """
    Poster'дун бир транзакциясын BigQuery схемасына айландырат.
    Чек + анын товарларын кайтарат.
    """
    transaction_id = t.get("transaction_id")
    products       = t.get("products", [])

    # Метадата — ар бир жазуунун кайдан, качан, кайсы жүктөөдөн келгени
    meta = {
        "load_ts":          load_ts,
        "batch_id":         batch_id,
        "source_date_from": bashy,
        "source_date_to":   ayagy,
        "source_page":      page,
    }

    transaction_row = {
        **meta,  # метадатаны жайып кошобуз
        "transaction_date":  t.get("date_close", "")[:10] if t.get("date_close") else None,
        "transaction_id":    transaction_id,
        "table_id":          t.get("table_id"),
        "spot_id":           t.get("spot_id"),
        "client_id":         t.get("client_id"),
        "sum":               float(t.get("sum")               or 0),
        "payed_sum":         float(t.get("payed_sum")         or 0),
        "payed_cash":        float(t.get("payed_cash")        or 0),
        "payed_card":        float(t.get("payed_card")        or 0),
        "payed_cert":        float(t.get("payed_cert")        or 0),
        "payed_bonus":       float(t.get("payed_bonus")       or 0),
        "payed_third_party": float(t.get("payed_third_party") or 0),
        "round_sum":         float(t.get("round_sum")         or 0),
        "pay_type":          t.get("pay_type"),
        "reason":            t.get("reason"),
        "tip_sum":           float(t.get("tip_sum")           or 0),
        "bonus":             float(t.get("bonus")             or 0),
        "discount":          float(t.get("discount")          or 0),
        "print_fiscal":      t.get("print_fiscal"),
        "date_close":        t.get("date_close"),
        "raw_json":          json.dumps(t, ensure_ascii=False),
    }

    product_rows = []
    for i, p in enumerate(products):
        product_rows.append({
            **meta,
            "transaction_id":   transaction_id,
            "transaction_date": t.get("date_close", "")[:10] if t.get("date_close") else None,
            "line_number":      i + 1,
            "product_id":       p.get("product_id"),
            "modification_id":  p.get("modification_id"),
            "type":             p.get("type"),
            "workshop_id":      p.get("workshop_id"),
            "num":              float(p.get("num")           or 0),
            "product_sum":      float(p.get("product_sum")   or 0),
            "payed_sum":        float(p.get("payed_sum")     or 0),
            "cert_sum":         float(p.get("cert_sum")      or 0),
            "bonus_sum":        float(p.get("bonus_sum")     or 0),
            "bonus_accrual":    float(p.get("bonus_accrual") or 0),
            "round_sum":        float(p.get("round_sum")     or 0),
            "discount":         float(p.get("discount")      or 0),
            "print_fiscal":     p.get("print_fiscal"),
            "tax_id":           p.get("tax_id"),
            "tax_value":        float(p.get("tax_value")     or 0),
            "tax_type":         p.get("tax_type"),
            "tax_fiscal":       float(p.get("tax_fiscal")    or 0),
            "tax_sum":          float(p.get("tax_sum")       or 0),
            "raw_json":         json.dumps(p, ensure_ascii=False),
        })

    return transaction_row, product_rows


def posterden_satuulardy_aluu(request):
    request_json = request.get_json(silent=True) or {}

    bashy = request_json.get("date_from")
    ayagy = request_json.get("date_to")

    if not bashy or not ayagy:
        today = datetime.now(tz=ZoneInfo("Asia/Bishkek")).date()
        bashy = (today - timedelta(days=2)).isoformat()
        ayagy = today.isoformat()

    posterBaseUrl = os.getenv("POSTER_BASE_URL")
    posterToken   = os.getenv("POSTER_TOKEN")
    url = f"{posterBaseUrl}/transactions.getTransactions"
    params = {
        "token":     posterToken,
        "date_from": bashy,
        "date_to":   ayagy,
        "per_page":  PER_PAGE,
        "page":      1
    }

    # Бул жүктөөнүн уникалдуу ID'си — бардык барактар бир batch_id'ни бөлүшөт
    batch_id = str(uuid.uuid4())
    load_ts  = datetime.now(timezone.utc).isoformat()

    # --- 1. Биринчи баракты алабыз — жалпы санды билүү үчүн ---
    joop, error_body, error_code = poster_baragyn_aluu(url, params, page=1)
    if error_body:
        return error_body, error_code

    response_data  = joop.get("response", {})
    total_count    = int(response_data.get("count", 0))
    total_pages    = math.ceil(total_count / PER_PAGE)  # ← ceil: дайыма жогору тарап

    print(f"Жалпы чек: {total_count}, барак: {total_pages}")

    # Датасы боюнча топтойбуз: { "2026-04-13": { transactions: [], products: [] } }
    by_date = {}

    # --- 2. Бардык барактарды айланабыз ---
    for page in range(1, total_pages + 1):

        # Биринчи баракты кайрадан сурабайбыз — анткени мурда алдык
        if page == 1:
            page_data = response_data.get("data", [])
        else:
            joop, error_body, error_code = poster_baragyn_aluu(url, params, page=page)
            if error_body:
                print(f"[{page}-баракта ката]: {error_body}")
                continue
            page_data = joop.get("response", {}).get("data", [])

        print(f"Баракча {page}/{total_pages}: {len(page_data)} чек")

        # --- 3. Трансформация + датасы боюнча топтоо ---
        for t in page_data:
            transaction_row, product_rows = transformacia(
                t, bashy, ayagy, page, batch_id, load_ts
            )

            date_key = (t.get("date_close") or "")[:10] or "unknown"

            if date_key not in by_date:
                by_date[date_key] = {"transactions": [], "products": []}

            by_date[date_key]["transactions"].append(transaction_row)
            by_date[date_key]["products"].extend(product_rows)

    # --- 4. BigQuery'ге датасы боюнча партицияларга жазуу ---
    try:
        client = aluu_bq_client()
        total_transactions = 0
        total_products     = 0

        for date_key, data in by_date.items():
            bq_ke_jaz(client, BQ_TABLE_TRANSACTIONS, data["transactions"], date_str=date_key)
            bq_ke_jaz(client, BQ_TABLE_PRODUCTS,     data["products"],     date_str=date_key)
            total_transactions += len(data["transactions"])
            total_products     += len(data["products"])

    except Exception as e:
        return {
            "status":     "Ishten chykty",
            "maalymdama": "BigQuery kata",
            "shilteme":   str(e)
        }, 500

    # --- 5. Жооп ---
    return {
        "status":       "ok",
        "batch_id":     batch_id,
        "bashy":        bashy,
        "ayagy":        ayagy,
        "total_count":  total_count,
        "total_pages":  total_pages,
        "transactions": total_transactions,
        "products":     total_products,
    }, 200


class LocalRequest:
    """Cloud Run request'ин жергиликтүү имитациясы."""
    def __init__(self, data=None):
        self._data = data or {}

    def get_json(self, silent=False):
        return self._data


if __name__ == "__main__":
    # Даталарды өзгөртүп жергиликтүү текшере аласың
    request = LocalRequest({
        # "date_from": "2026-04-01",
        # "date_to":   "2026-04-15",
    })

    result, status_code = posterden_satuulardy_aluu(request)
    print(f"\nСтатус: {status_code}")
    print(json.dumps(result, ensure_ascii=False, indent=2))
