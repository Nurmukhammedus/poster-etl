"""
BigQuery client.

Два режима записи:
  write_table()     — полная перезапись таблицы (WRITE_TRUNCATE)
                      для справочников: категории, товары, сотрудники и т.д.
  write_partition() — перезапись одной дневной партиции (WRITE_TRUNCATE)
                      для транзакционных данных
  write_by_date()   — обёртка: пишет сразу несколько партиций
"""

import logging
import os
import re

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

from config import BQ_DATASET, BQ_PROJECT, GOOGLE_APPLICATION_CREDENTIALS

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self):
        if GOOGLE_APPLICATION_CREDENTIALS:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS
        self.client  = bigquery.Client(project=BQ_PROJECT)
        self.project = BQ_PROJECT
        self.dataset = BQ_DATASET

    # ── Internal ──────────────────────────────────────────────────────────────

    def _table_ref(self, table_name: str) -> str:
        return f"{self.project}.{self.dataset}.{table_name}"

    def _get_schema(self, table_name: str):
        """Получаем схему прямо из BigQuery — никакой авто-детекции типов."""
        return self.client.get_table(self._table_ref(table_name)).schema

    def _load(self, rows: list[dict], destination: str, schema) -> None:
        """Базовый load job — используется и write_table и write_partition."""
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
        )
        try:
            job = self.client.load_table_from_json(rows, destination, job_config=job_config)
            job.result()
        except Conflict:
            # 409: job уже был отправлен (retry после успешного запроса) — игнорируем
            logger.warning("BQ job conflict (409) for %s — already submitted, skipping", destination)

    # ── Public ────────────────────────────────────────────────────────────────

    def write_table(self, table_name: str, rows: list[dict]) -> int:
        """
        Полная перезапись таблицы (WRITE_TRUNCATE).
        Используется для справочников: категории, товары, точки, сотрудники, клиенты.
        """
        if not rows:
            logger.info("[%s] no rows — skip", table_name)
            return 0

        table_ref = self._table_ref(table_name)
        schema    = self._get_schema(table_name)

        self._load(rows, table_ref, schema)

        logger.info("[%s] %d rows → full table overwrite", table_name, len(rows))
        return len(rows)

    def write_partition(self, table_name: str, rows: list[dict], date_str: str) -> int:
        """
        Перезаписывает одну дневную партицию (WRITE_TRUNCATE).
        date_str — 'YYYY-MM-DD'
        """
        if not rows:
            logger.info("[%s] no rows for %s — skip", table_name, date_str)
            return 0

        date_nodash   = date_str.replace("-", "")
        parent_ref    = self._table_ref(table_name)
        partition_ref = f"{parent_ref}${date_nodash}"
        schema        = self._get_schema(table_name)

        self._load(rows, partition_ref, schema)

        logger.info("[%s] %d rows → %s", table_name, len(rows), partition_ref)
        return len(rows)

    def write_by_date(self, table_name: str, rows_by_date: dict[str, list[dict]]) -> int:
        """
        Записывает строки сгруппированные по дате в соответствующие партиции.
        Пропускает ключи в невалидном формате (не YYYY-MM-DD).
        Возвращает суммарное кол-во записанных строк.
        """
        total = 0
        for date_str, rows in rows_by_date.items():
            if not _DATE_RE.match(str(date_str or "")):
                logger.warning(
                    "[%s] skipping invalid date key %r (%d rows)", table_name, date_str, len(rows)
                )
                continue
            total += self.write_partition(table_name, rows, date_str)
        return total
