# Real-Time Data Engineering Framework for Business Intelligence

## Overview
Builds an end-to-end real-time data pipeline for financial market data: ingest OHLCV ticks, stream through Kafka, process with Spark Structured Streaming, persist to PostgreSQL, and visualize live KPIs in Superset or Grafana. Designed as an academic prototype using only free, open-source components.

## Objectives
- Ingest near real-time financial data continuously (Yahoo Finance via `yfinance`)
- Stream data reliably with Kafka and process it with Spark Structured Streaming
- Persist curated facts in PostgreSQL for fast BI consumption
- Serve live dashboards for KPIs such as price trends, volume, highs/lows, and moving averages
- Keep the stack cost-free and fault-tolerant enough for prototyping

## Problem Statement
Traditional BI stacks are batch-oriented, delaying insights. Modern teams need low-latency analytics to react to market signals quickly. This framework demonstrates a real-time alternative using open-source tooling.

## Architecture (conceptual)
1) Fetch minute-level OHLCV data from Yahoo Finance using `yfinance`
2) Publish JSON events to a Kafka topic (producer)
3) Consume the stream in Spark Structured Streaming, validate/transform, and compute KPIs
4) Write curated results to PostgreSQL tables
5) Connect Superset or Grafana to PostgreSQL for live dashboards

## Data Source
- Provider: Yahoo Finance
- Access: `yfinance` Python library
- Fields: open, high, low, close, volume (OHLCV)
- Frequency: minute-level pulls to simulate real-time streaming

## Technology Stack
- Language: Python
- Streaming bus: Apache Kafka
- Stream processing: Apache Spark Structured Streaming
- Storage: PostgreSQL
- Visualization: Apache Superset or Grafana
- Data source library: `yfinance`

## Key KPIs
- Price trend over time
- Volume trend
- Daily high vs low comparison
- Moving averages
- Top gainers/losers (by chosen window)

## Suggested Project Layout
```
.
├── src/
│   ├── producer/           # Kafka producer that fetches via yfinance
│   ├── streaming/          # Spark Structured Streaming jobs
│   ├── dashboards/         # BI configs/SQL (Superset or Grafana)
│   └── utils/              # Shared helpers (schemas, logging, configs)
├── configs/
│   └── env.example         # Copy to .env with your secrets
├── notebooks/              # Exploration or KPI prototypes
├── docker-compose.yml      # Optional: Kafka, Zookeeper, Postgres, Superset
└── README.md
```

## Prerequisites
- Python 3.10+
- Java 8+ (for Spark)
- Apache Kafka (local or Docker)
- Apache Spark 3.x (with Structured Streaming)
- PostgreSQL 14+
- Superset or Grafana
- Git and PowerShell (Windows)

## Quick Start (local, PowerShell)
1) Clone and create a virtual env
	```powershell
	git clone https://github.com/vansh1-0/Real-time-Data-Engineering-framework-for-Business-Intelligence.git
	cd Real-time-Data-Engineering-framework-for-Business-Intelligence
	python -m venv .venv
	.\.venv\Scripts\Activate
	```

2) Install Python deps (adjust package list to your code)
	```powershell
	pip install --upgrade pip
	pip install yfinance kafka-python pyspark psycopg2-binary pandas python-dotenv
	```

3) Configure environment
	Create `.env` (or set in your shell) with values like:
	```dotenv
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092
	KAFKA_TOPIC=market-prices
	SYMBOLS=AAPL,MSFT,GOOG
	FETCH_INTERVAL_SECONDS=60
	POSTGRES_HOST=localhost
	POSTGRES_PORT=5432
	POSTGRES_DB=bi_realtime
	POSTGRES_USER=bi_user
	POSTGRES_PASSWORD=bi_password
	```

4) Start infrastructure
- Kafka/Zookeeper: start your local services or a Docker stack
- PostgreSQL: ensure the database and user above exist
- Superset/Grafana: run locally or via Docker and connect to PostgreSQL

5) Create Kafka topic (example)
	```powershell
	kafka-topics --create --topic market-prices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
	```

6) Run the producer (example path)
	```powershell
	python src/producer/kafka_producer.py --symbols %SYMBOLS% --interval %FETCH_INTERVAL_SECONDS% --topic %KAFKA_TOPIC% --bootstrap %KAFKA_BOOTSTRAP_SERVERS%
	```

7) Run the streaming job (example spark-submit)
	```powershell
	spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	  src/streaming/spark_streaming.py \
	  --input-topic %KAFKA_TOPIC% \
	  --bootstrap %KAFKA_BOOTSTRAP_SERVERS% \
	  --postgres-url jdbc:postgresql://%POSTGRES_HOST%:%POSTGRES_PORT%/%POSTGRES_DB% \
	  --postgres-user %POSTGRES_USER% \
	  --postgres-password %POSTGRES_PASSWORD%
	```

8) Visualize
- Point Superset/Grafana to PostgreSQL
- Build dashboards for the KPIs listed above (price trend, volume, highs/lows, moving averages, gainers/losers)

## Data Model (example)
- `raw_ticks` (optional landing): symbol, ts, open, high, low, close, volume
- `fact_prices`: symbol, ts_minute, open, high, low, close, volume, ma_5, ma_20
- `kpi_snapshots`: window_start, window_end, symbol, avg_price, total_volume, gain_pct

## Testing and Quality
- Lint: `ruff .` (or `flake8` if preferred)
- Type check: `mypy src`
- Unit tests: `pytest`

## Troubleshooting
- Push rejected / branch mismatch: ensure you are on `main` tracking `origin/main`
- Kafka not reachable: confirm `KAFKA_BOOTSTRAP_SERVERS` and that the topic exists
- Spark cannot load Kafka package: verify the `--packages` coordinate matches your Spark version
- PostgreSQL auth errors: recheck user/password and ensure the DB exists

## Roadmap Ideas
- Add schema registry and Avro/Protobuf payloads
- Add monitoring (Prometheus + Grafana) for pipeline health
- Add CI for lint/test
- Provide Docker Compose for one-command local bring-up

## Contributors
- Vansh Mehta
- Dax Virani