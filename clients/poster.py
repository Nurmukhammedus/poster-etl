"""
Poster API client.

Инкапсулирует всё общение с Poster API:
  - retry с exponential backoff
  - постраничный обход (pagination)
  - единое место для обработки ошибок

Три режима получения данных:
  get_reference()         — справочники (категории, точки, сотрудники, товары, клиенты)
  get_paginated_by_date() — транзакционные данные с фильтром по дате
  get_transactions()       — кассовые чеки, СТРОКИ ТОВАРОВ (transactions.getTransactions)
  get_dash_transactions()  — кассовые чеки, БОГАТАЯ ШАПКА  (dash.getTransactions)
"""

import logging
import time

import requests

from config import (
    POSTER_BASE_URL,
    POSTER_MAX_RETRIES,
    POSTER_PER_PAGE,
    POSTER_TIMEOUT,
    POSTER_TOKEN,
)

logger = logging.getLogger(__name__)


class PosterAPIError(Exception):
    """Poster API вернул ошибку или недоступен."""


class PosterClient:
    def __init__(self):
        self.base_url  = POSTER_BASE_URL
        self.token     = POSTER_TOKEN
        self.per_page  = POSTER_PER_PAGE
        self.timeout   = POSTER_TIMEOUT
        self.max_retry = POSTER_MAX_RETRIES

    # ── Internal ──────────────────────────────────────────────────────────────

    def _request(self, endpoint: str, params: dict) -> dict:
        """
        Один HTTP-запрос к Poster API с retry + exponential backoff.
        Возвращает распарсенный JSON или бросает PosterAPIError.
        """
        url    = f"{self.base_url}/{endpoint}"
        params = {**params, "token": self.token}

        for attempt in range(1, self.max_retry + 1):
            try:
                logger.debug("GET %s  attempt=%d", endpoint, attempt)
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
            except requests.exceptions.ConnectionError as e:
                logger.warning("Attempt %d — connection error: %s", attempt, e)
                self._backoff(attempt)
                continue
            except requests.exceptions.Timeout as e:
                logger.warning("Attempt %d — timeout: %s", attempt, e)
                self._backoff(attempt)
                continue
            except requests.exceptions.HTTPError as e:
                raise PosterAPIError(f"HTTP error: {e}") from e

            try:
                body = response.json()
            except Exception as e:
                raise PosterAPIError(f"JSON parse error: {response.text[:300]}") from e

            if "error" in body:
                raise PosterAPIError(f"API error: {body['error']}")

            return body

        raise PosterAPIError(f"All {self.max_retry} attempts failed for {endpoint}")

    @staticmethod
    def _backoff(attempt: int) -> None:
        delay = 2 ** attempt  # 2s, 4s, 8s …
        logger.info("Retrying in %ds …", delay)
        time.sleep(delay)

    @staticmethod
    def _extract_list(resp) -> list[dict]:
        """
        Poster API возвращает data по-разному:
          {"response": [...]}            → список напрямую
          {"response": {"data": [...]}}  → словарь с data
        """
        if isinstance(resp, list):
            return resp
        if isinstance(resp, dict):
            return resp.get("data", [])
        return []

    # ── Public ────────────────────────────────────────────────────────────────

    def get_reference(self, endpoint: str, extra_params: dict | None = None) -> list[dict]:
        """
        Справочные данные: автоматически определяет, есть ли пагинация.
        Подходит для: menu.getCategories, menu.getProducts,
                      access.getSpots, access.getEmployees, clients.getClients
        """
        base_params = {"per_page": self.per_page, **(extra_params or {})}

        body = self._request(endpoint, {**base_params, "page": 1})
        resp = body.get("response", [])

        # Нет пагинации — вернули список напрямую
        if isinstance(resp, list):
            logger.info("%s: %d records (no pagination)", endpoint, len(resp))
            return resp

        # Есть пагинация — response = {"count": N, "data": [...]}
        total_count = int(resp.get("count", 0))
        total_pages = -(-total_count // self.per_page) if total_count else 1
        all_rows    = list(resp.get("data", []))

        logger.info("%s: %d records, %d pages", endpoint, total_count, total_pages)

        for page in range(2, total_pages + 1):
            body      = self._request(endpoint, {**base_params, "page": page})
            page_data = self._extract_list(body.get("response", []))
            all_rows.extend(page_data)
            logger.info("  page %d/%d — %d rows", page, total_pages, len(page_data))

        return all_rows

    def get_paginated_by_date(
        self,
        endpoint: str,
        date_from: str,
        date_to: str,
        extra_params: dict | None = None,
    ) -> list[dict]:
        """
        Транзакционные данные с фильтром по дате и пагинацией.
        Подходит для: storage.getSupplies, storage.getIngredientWriteOff,
                      finance.getCashShifts, finance.getTransactions
        """
        base_params = {
            "date_from": date_from,
            "date_to":   date_to,
            "per_page":  self.per_page,
            **(extra_params or {}),
        }

        body = self._request(endpoint, {**base_params, "page": 1})
        resp = body.get("response", {})

        # Некоторые эндпоинты возвращают список без пагинации
        if isinstance(resp, list):
            logger.info("%s: %d records (no pagination)", endpoint, len(resp))
            return resp

        total_count = int(resp.get("count", 0))
        total_pages = -(-total_count // self.per_page) if total_count else 1
        all_rows    = list(resp.get("data", []))

        logger.info(
            "%s: %d records, %d pages (%s → %s)",
            endpoint, total_count, total_pages, date_from, date_to,
        )

        for page in range(2, total_pages + 1):
            body      = self._request(endpoint, {**base_params, "page": page})
            page_data = self._extract_list(body.get("response", {}))
            all_rows.extend(page_data)
            logger.info("  page %d/%d — %d rows", page, total_pages, len(page_data))

        return all_rows

    def get_product(self, product_id: str | int) -> dict:
        """
        Детальная карточка одного продукта — содержит поле `ingredients` (тех карта).
        В отличие от get_reference("menu.getProducts"), список не содержит ingredients.
        """
        body = self._request("menu.getProduct", {"product_id": product_id})
        return body.get("response", {})

    def get_transactions(self, date_from: str, date_to: str) -> list[dict]:
        """
        Кассовые чеки — transactions.getTransactions.

        Используется ради массива products[] с полями workshop_id, type, discount,
        bonus_sum, cert_sum, printed_num, round_sum — их нет в dash.getTransactions.

        Структура ответа: {"response": {"count": N, "data": [...]}} — пагинация
        по странице (page / per_page).
        """
        endpoint = "transactions.getTransactions"
        params   = {
            "date_from": date_from,
            "date_to":   date_to,
            "per_page":  self.per_page,
        }

        body        = self._request(endpoint, {**params, "page": 1})
        resp        = body.get("response", {})
        total_count = int(resp.get("count", 0))
        total_pages = -(-total_count // self.per_page) if total_count else 1

        logger.info(
            "transactions.getTransactions: %d records, %d pages (%s → %s)",
            total_count, total_pages, date_from, date_to,
        )

        all_rows = list(resp.get("data", []))

        for page in range(2, total_pages + 1):
            body      = self._request(endpoint, {**params, "page": page})
            page_data = body.get("response", {}).get("data", [])
            all_rows.extend(page_data)
            logger.info("  page %d/%d — %d rows", page, total_pages, len(page_data))

        return all_rows

    def get_dash_transactions(self, date_from: str, date_to: str) -> list[dict]:
        """
        Кассовые чеки — dash.getTransactions.

        Используется ради богатой ШАПКИ: guests_count, user_id/name (официант),
        status, processing_status, service_mode, table_name, client_*,
        transaction_comment, tax_sum, payment_method_id, payed_ewallet.

        Пагинация:
          * after_date_close / before_date_close — Unix timestamp границы периода.
            Эти параметры работают корректно при cursor-пагинации через next_tr,
            в отличие от dateFrom/dateTo которые игнорируются на 2+ страницах.
          * next_tr — курсор (ID последнего чека страницы).
          * Останавливаемся когда API вернул пустой список ИЛИ все чеки
            страницы вышли за пределы периода (дополнительная страховка).

        ВАЖНО: include_products=true НЕ передаём — products мы берём из
        transactions.getTransactions (там полнее поля).
        """
        import calendar

        endpoint = "dash.getTransactions"

        # Конвертируем 'YYYY-MM-DD' → Unix timestamp (сек)
        # after_date_close  = начало дня date_from (00:00:00)
        # before_date_close = конец дня date_to   (23:59:59)
        from datetime import datetime, timezone
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
        ts_from = int(dt_from.timestamp())
        ts_to   = int(dt_to.timestamp())

        base_params: dict = {
            "after_date_close":  ts_from,
            "before_date_close": ts_to,
            "status":            0,   # все заказы (открытые + закрытые + удалённые)
        }

        all_rows: list[dict] = []
        next_tr: str | None  = None
        page_num             = 0
        prev_last_id: str | None = None

        while True:
            page_num += 1
            params = dict(base_params)
            if next_tr is not None:
                params["next_tr"] = next_tr

            body = self._request(endpoint, params)
            rows = body.get("response", [])

            if not isinstance(rows, list) or not rows:
                break

            # Страховка: фильтруем строки вне нашего диапазона
            # (date_close приходит в мс, переводим в сек для сравнения)
            in_range = [
                r for r in rows
                if ts_from <= int(r.get("date_close") or 0) // 1000 <= ts_to
            ]
            all_rows.extend(in_range)

            last_id      = str(rows[-1].get("transaction_id", "") or "")
            last_ts      = int(rows[-1].get("date_close") or 0) // 1000

            logger.info(
                "dash.getTransactions: page %d — %d rows, %d in range (last_id=%s)",
                page_num, len(rows), len(in_range), last_id,
            )

            # Остановка: курсор зашёл дальше диапазона или дошли до конца
            if not last_id or last_id == prev_last_id or last_ts < ts_from:
                break

            prev_last_id = last_id
            next_tr      = last_id

        logger.info(
            "dash.getTransactions: total %d records (%s → %s, %d pages)",
            len(all_rows), date_from, date_to, page_num,
        )

        return all_rows
