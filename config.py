import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── Poster API ────────────────────────────────────────────────────────────────
POSTER_TOKEN       = os.getenv("POSTER_TOKEN")
POSTER_BASE_URL    = os.getenv("POSTER_BASE_URL", "https://joinposter.com/api")
POSTER_PER_PAGE    = 1000   # max allowed by Poster API
POSTER_TIMEOUT     = 180    # seconds per request
POSTER_MAX_RETRIES = 3      # retry attempts on network errors

# ── BigQuery ──────────────────────────────────────────────────────────────────
BQ_PROJECT  = os.getenv("BQ_PROJECT", "posterquery")
BQ_DATASET  = os.getenv("BQ_DATASET", "poster_analitics")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ── Timezone ──────────────────────────────────────────────────────────────────
TIMEZONE = os.getenv("TIMEZONE", "Asia/Bishkek")

# ── Lookback window ───────────────────────────────────────────────────────────
# Poster может обновлять чеки задним числом (возвраты, корректировки)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "2"))
