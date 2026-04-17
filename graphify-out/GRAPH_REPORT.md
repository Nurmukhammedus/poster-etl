# Graph Report - .  (2026-04-16)

## Corpus Check
- Corpus is ~862 words - fits in a single context window. You may not need a graph.

## Summary
- 20 nodes · 23 edges · 4 communities detected
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Poster API Ingestion|Poster API Ingestion]]
- [[_COMMUNITY_ETL Dependencies|ETL Dependencies]]
- [[_COMMUNITY_Local Test Runner|Local Test Runner]]
- [[_COMMUNITY_BigQuery Writer|BigQuery Writer]]

## God Nodes (most connected - your core abstractions)
1. `posterden_satuulardy_aluu()` - 6 edges
2. `LocalRequest` - 4 edges
3. `ETL Pipeline (Poster API → BigQuery)` - 4 edges
4. `aluu_bq_client()` - 3 edges
5. `bq_ke_jaz()` - 3 edges
6. `poster_baragyn_aluu()` - 3 edges
7. `transformacia()` - 3 edges
8. `functions-framework 3.*` - 2 edges
9. `Вспомогательная функция — возвращает BigQuery клиент.` - 1 edges
10. `BigQuery'ге саптарды жазат.     date_str берилсе → ошол күндүн партициясына WRIT` - 1 edges

## Surprising Connections (you probably didn't know these)
- `posterden_satuulardy_aluu()` --calls--> `bq_ke_jaz()`  [EXTRACTED]
  main.py → main.py  _Bridges community 3 → community 0_

## Communities

### Community 0 - "Poster API Ingestion"
Cohesion: 0.31
Nodes (7): aluu_bq_client(), poster_baragyn_aluu(), posterden_satuulardy_aluu(), Вспомогательная функция — возвращает BigQuery клиент., Poster API'нен бир баракты алат.     Ар бир чакыруу = бир HTTP суроо = бир барак, Poster'дун бир транзакциясын BigQuery схемасына айландырат.     Чек + анын товар, transformacia()

### Community 1 - "ETL Dependencies"
Cohesion: 0.33
Nodes (6): ETL Pipeline (Poster API → BigQuery), functions-framework 3.*, google-cloud-bigquery 3.35.1, Google Cloud Functions Runtime, python-dotenv, requests 2.32.5

### Community 2 - "Local Test Runner"
Cohesion: 0.67
Nodes (2): LocalRequest, Cloud Run request'ин жергиликтүү имитациясы.

### Community 3 - "BigQuery Writer"
Cohesion: 1.0
Nodes (2): bq_ke_jaz(), BigQuery'ге саптарды жазат.     date_str берилсе → ошол күндүн партициясына WRIT

## Knowledge Gaps
- **9 isolated node(s):** `Вспомогательная функция — возвращает BigQuery клиент.`, `BigQuery'ге саптарды жазат.     date_str берилсе → ошол күндүн партициясына WRIT`, `Poster API'нен бир баракты алат.     Ар бир чакыруу = бир HTTP суроо = бир барак`, `Poster'дун бир транзакциясын BigQuery схемасына айландырат.     Чек + анын товар`, `Cloud Run request'ин жергиликтүү имитациясы.` (+4 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `BigQuery Writer`** (2 nodes): `bq_ke_jaz()`, `BigQuery'ге саптарды жазат.     date_str берилсе → ошол күндүн партициясына WRIT`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `LocalRequest` connect `Local Test Runner` to `Poster API Ingestion`?**
  _High betweenness centrality (0.137) - this node is a cross-community bridge._
- **Why does `posterden_satuulardy_aluu()` connect `Poster API Ingestion` to `BigQuery Writer`?**
  _High betweenness centrality (0.120) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `ETL Pipeline (Poster API → BigQuery)` (e.g. with `requests 2.32.5` and `python-dotenv`) actually correct?**
  _`ETL Pipeline (Poster API → BigQuery)` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Вспомогательная функция — возвращает BigQuery клиент.`, `BigQuery'ге саптарды жазат.     date_str берилсе → ошол күндүн партициясына WRIT`, `Poster API'нен бир баракты алат.     Ар бир чакыруу = бир HTTP суроо = бир барак` to the rest of the system?**
  _9 weakly-connected nodes found - possible documentation gaps or missing edges._