import os
import time
import logging
from typing import List, Any, Tuple
from twisted.enterprise import adbapi
import pymysql
import pymysql.cursors


# --------------------------------------------------------------------------------------
# ENV (loaded by run.py via dotenv)
# --------------------------------------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "property_listing")
MYSQL_CHARSET = "utf8mb4"


TABLE_NAME = "iproperty-new-listing"

# Batch behavior
BATCH_SIZE = 50
FLUSH_SECS = 300

UPSERT_LAST_WINS = bool(int(os.getenv("UPSERT_LAST_WINS", "0")))

# DB thread pool sizing
POOL_MIN = 2
POOL_MAX = 10



COLUMNS: List[str] = [
    "list_id", "name", "url", "area", "state", "price", "bed_rooms", "built_up_size",
    "posted_date", "tenure", "furnished_status", "property_type", "land_title",
    "property_title_type", "bumi_lot", "built_up_price", "occupancy", "unit_type",
    "lat", "lng", "description", "new_project", "auction", "below_market_value",
    "urgent", "agent_name", "agency_name", "website_name", "data_scraping_date", "data_scraping_date",
]



# --------------------------------------------------------------------------------------
# SQL templates
# --------------------------------------------------------------------------------------
_placeholders = ", ".join(["%s"] * len(COLUMNS))
_cols_sql = ", ".join(f"`{c}`" for c in COLUMNS)
_insert_head = f"INSERT INTO `{TABLE_NAME}` ({_cols_sql}) VALUES ({_placeholders})"

# (Deprecated but widely compatible) VALUES() syntax; fine for MySQL 5.7/8.0.
_update_cols = [c for c in COLUMNS if c != "list_id"]
_update_sql = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in _update_cols)

INSERT_SQL_IGNORE = f"INSERT IGNORE INTO `{TABLE_NAME}` ({_cols_sql}) VALUES ({_placeholders})"
INSERT_SQL_UPSERT = f"{_insert_head} ON DUPLICATE KEY UPDATE {_update_sql}"

INSERT_SQL = INSERT_SQL_UPSERT if UPSERT_LAST_WINS else INSERT_SQL_IGNORE


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _boolish_to_int(v: Any) -> Any:
    # Ensure booleans in items become 0/1 for TINYINT columns
    if isinstance(v, bool):
        return 1 if v else 0
    return v

def _row_from_item(item: dict) -> Tuple[Any, ...]:
    # Build tuple in the exact order of COLUMNS; missing keys -> None
    row = tuple(_boolish_to_int(item.get(k)) for k in COLUMNS)
    return row


# --------------------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------------------
class MySQLStorePipelineBatched:
    """
    Buffers items and flushes to MySQL in batches using Twisted's adbapi pool.
    Default policy keeps the FIRST record (INSERT IGNORE). Set UPSERT_LAST_WINS=1 to update existing rows.
    Enable in your spider.py:
        custom_settings = {
            "ITEM_PIPELINES": {"db_pipeline.MySQLStorePipelineBatched": 300},
            ...
        }
    """

    def __init__(self, dbpool):
        self.dbpool = dbpool
        self._buf: List[Tuple[Any, ...]] = []
        self._last_flush = time.time()

    @classmethod
    def from_crawler(cls, crawler):
        dbparams = dict(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset=MYSQL_CHARSET,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,  # each interaction commits independently
        )
        pool = adbapi.ConnectionPool(
            "pymysql",
            **dbparams,
            cp_min=POOL_MIN,
            cp_max=POOL_MAX,
            cp_reconnect=True,
        )
        return cls(pool)

    # Scrapy calls this for every yielded item
    def process_item(self, item, spider):
        self._buf.append(_row_from_item(item))

        # Count-based flush
        if len(self._buf) >= BATCH_SIZE:
            self._flush_async()

        # Time-based flush (opportunistic)
        elif (time.time() - self._last_flush) >= FLUSH_SECS and self._buf:
            self._flush_async()

        return item  # non-blocking; DB work runs in a thread

    # Called when spider closes
    def close_spider(self, spider):
        if self._buf:
            return self._flush_async()

    # ------------------ internals ------------------
    def _drain(self) -> List[Tuple[Any, ...]]:
        batch, self._buf = self._buf, []
        self._last_flush = time.time()
        return batch

    def _flush_async(self):
        batch = self._drain()
        n = len(batch)
        logging.info(f"[DB] Flushing batch: {n} rows")
        d = self.dbpool.runInteraction(self._insert_many, batch)
        d.addCallbacks(
            lambda _: logging.info(f"[DB] Batch OK: {n} rows"),
            lambda err: logging.error(f"[DB] Batch FAILED ({n} rows): {err}"),
        )
        return d

    def _insert_many(self, tx, batch: List[Tuple[Any, ...]]):
        tx.executemany(INSERT_SQL, batch)
