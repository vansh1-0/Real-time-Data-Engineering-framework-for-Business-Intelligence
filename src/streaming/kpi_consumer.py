"""
Kafka KPI Consumer (Windows-compatible)
Computes 5-minute tumbling window KPIs with a 2-minute watermark.
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv('configs/.env')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market-prices')

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'bi_realtime')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'bi_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'bi_password')

WINDOW_SIZE_MINUTES = 5
WATERMARK_MINUTES = 2

TABLE_COLS = [
    ("symbol",          8),
    ("window_start",   25),
    ("window_end",     25),
    ("avg_close",      12),
    ("min_close",      12),
    ("max_close",      12),
    ("vwap",           12),
    ("price_vol",      12),
    ("total_volume",   14),
    ("trade_count",    13),
]


def format_row(values):
    padded = [str(v).ljust(width)[:width] for (v, (_, width)) in zip(values, TABLE_COLS)]
    return " | ".join(padded)


def log_table_header():
    header = format_row([name for (name, _) in TABLE_COLS])
    logger.info(header)
    logger.info("-" * len(header))


def parse_event_time(ts_str):
    """Parse ISO timestamp string into timezone-aware datetime."""
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def window_start_for(ts):
    """Get tumbling window start for a timestamp."""
    minute_bucket = (ts.minute // WINDOW_SIZE_MINUTES) * WINDOW_SIZE_MINUTES
    return ts.replace(minute=minute_bucket, second=0, microsecond=0)


def window_end_for(start_ts):
    return start_ts + timedelta(minutes=WINDOW_SIZE_MINUTES)


def create_kafka_consumer():
    """Create and return a Kafka consumer instance."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='market-kpi-processor',
            consumer_timeout_ms=1000
        )
        logger.info(f"Kafka Consumer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
        return consumer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def create_db_connection():
    """Create and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        logger.info(f"Connected to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def init_kpi_table(conn):
    """Create KPI table if it does not exist (with all README columns)."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS kpi_market_prices (
        symbol          TEXT              NOT NULL,
        window_start    TIMESTAMPTZ       NOT NULL,
        window_end      TIMESTAMPTZ       NOT NULL,
        avg_close       DOUBLE PRECISION  NOT NULL,
        min_close       DOUBLE PRECISION  NOT NULL,
        max_close       DOUBLE PRECISION  NOT NULL,
        vwap            DOUBLE PRECISION  NOT NULL,
        price_volatility DOUBLE PRECISION NOT NULL,
        total_volume    BIGINT            NOT NULL,
        trade_count     INTEGER           NOT NULL,
        updated_at      TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
        PRIMARY KEY (symbol, window_start, window_end)
    );
    -- Add missing columns if table was created with an older schema
    ALTER TABLE kpi_market_prices ADD COLUMN IF NOT EXISTS min_close        DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE kpi_market_prices ADD COLUMN IF NOT EXISTS max_close        DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE kpi_market_prices ADD COLUMN IF NOT EXISTS vwap             DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE kpi_market_prices ADD COLUMN IF NOT EXISTS price_volatility DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE kpi_market_prices ADD COLUMN IF NOT EXISTS trade_count      INTEGER          NOT NULL DEFAULT 0;
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    logger.info("Ensured KPI table exists: kpi_market_prices (all 11 columns)")


def upsert_kpi(conn, symbol, window_start, window_end,
               avg_close, min_close, max_close, vwap,
               price_volatility, total_volume, trade_count):
    """Upsert KPI row for the given window (all README columns)."""
    upsert_sql = """
    INSERT INTO kpi_market_prices
        (symbol, window_start, window_end,
         avg_close, min_close, max_close, vwap,
         price_volatility, total_volume, trade_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, window_start, window_end)
    DO UPDATE SET
        avg_close        = EXCLUDED.avg_close,
        min_close        = EXCLUDED.min_close,
        max_close        = EXCLUDED.max_close,
        vwap             = EXCLUDED.vwap,
        price_volatility = EXCLUDED.price_volatility,
        total_volume     = EXCLUDED.total_volume,
        trade_count      = EXCLUDED.trade_count,
        updated_at       = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(upsert_sql, (
            symbol, window_start, window_end,
            avg_close, min_close, max_close, vwap,
            price_volatility, total_volume, trade_count
        ))


def main():
    logger.info("=" * 70)
    logger.info("Starting KPI Consumer - 5m Windows with 2m Watermark")
    logger.info("=" * 70)

    consumer = create_kafka_consumer()
    conn = create_db_connection()
    init_kpi_table(conn)

    # Aggregates: {symbol: {window_start: {sum_close, sum_vol_price, min_close, max_close, count, total_volume}}}
    aggregates = defaultdict(lambda: defaultdict(lambda: {
        "sum_close":     0.0,
        "sum_vol_price": 0.0,   # for VWAP = sum(close * volume) / sum(volume)
        "min_close":     float('inf'),
        "max_close":     float('-inf'),
        "count":         0,
        "total_volume":  0,
    }))
    max_event_time = None

    try:
        log_table_header()
        while True:
            messages = consumer.poll(timeout_ms=1000)

            for _, records in messages.items():
                for message in records:
                    data = message.value
                    ts = parse_event_time(data.get("timestamp"))
                    if ts is None:
                        continue

                    if max_event_time is None or ts > max_event_time:
                        max_event_time = ts

                    watermark_threshold = max_event_time - timedelta(minutes=WATERMARK_MINUTES)
                    if ts < watermark_threshold:
                        # Drop late data beyond watermark
                        continue

                    symbol     = data.get("symbol")
                    close_val  = float(data.get("close", 0.0))
                    volume_val = int(data.get("volume", 0))

                    win_start = window_start_for(ts)
                    agg = aggregates[symbol][win_start]
                    agg["sum_close"]     += close_val
                    agg["sum_vol_price"] += close_val * volume_val
                    agg["min_close"]      = min(agg["min_close"], close_val)
                    agg["max_close"]      = max(agg["max_close"], close_val)
                    agg["count"]         += 1
                    agg["total_volume"]  += volume_val

                    count        = agg["count"]
                    avg_close    = agg["sum_close"] / count if count else 0.0
                    min_close    = agg["min_close"] if agg["min_close"] != float('inf')  else 0.0
                    max_close    = agg["max_close"] if agg["max_close"] != float('-inf') else 0.0
                    vwap         = agg["sum_vol_price"] / agg["total_volume"] if agg["total_volume"] else avg_close
                    price_vol    = max_close - min_close
                    win_end      = window_end_for(win_start)

                    logger.info(
                        format_row([
                            symbol,
                            win_start.isoformat(),
                            win_end.isoformat(),
                            f"{avg_close:.2f}",
                            f"{min_close:.2f}",
                            f"{max_close:.2f}",
                            f"{vwap:.2f}",
                            f"{price_vol:.2f}",
                            agg["total_volume"],
                            count,
                        ])
                    )

                    upsert_kpi(conn, symbol, win_start, win_end,
                               avg_close, min_close, max_close, vwap,
                               price_vol, agg["total_volume"], count)

            # Cleanup old windows beyond watermark
            if max_event_time is not None:
                watermark_threshold = max_event_time - timedelta(minutes=WATERMARK_MINUTES)
                for symbol in list(aggregates.keys()):
                    for win_start in list(aggregates[symbol].keys()):
                        if window_end_for(win_start) < watermark_threshold:
                            del aggregates[symbol][win_start]
                    if not aggregates[symbol]:
                        del aggregates[symbol]

    except KeyboardInterrupt:
        logger.info("KPI consumer interrupted by user")
    except Exception as e:
        logger.error(f"KPI consumer error: {e}")
        raise
    finally:
        consumer.close()
        conn.close()
        logger.info("KPI consumer closed")


if __name__ == "__main__":
    main()
