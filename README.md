# Seattle Housing ETL Pipeline

Fetches the **50 newest residential building permits** from the
[City of Seattle Open Data Portal](https://data.seattle.gov/), transforms
them, and writes a Snappy-compressed Parquet file to Amazon S3 with
Hive-style date partitioning — scheduled to run **every Monday at 07:00 UTC**
via Apache Airflow.

---

## Project Structure

```
seattle_housing_etl/
├── pipeline.py                    # Standalone entry point (no Airflow)
├── requirements.txt
├── dags/
│   └── seattle_housing_dag.py     # Airflow DAG — weekly Monday schedule
├── docker/
│   ├── docker-compose.yml         # Full Airflow stack (Celery + Postgres + Redis)
│   └── .env.template              # Copy to .env and fill in secrets
├── extractor/
│   └── seattle_extractor.py       # Pulls data from Seattle Open Data (Socrata)
├── transformer/
│   └── housing_transformer.py     # Cleans, casts types, derives columns
├── loader/
│   └── s3_loader.py               # Serialises to Parquet, uploads to S3
└── utils/
    └── logger.py                  # Structured logging
```

---

## Prerequisites

| Tool | Minimum version | Install |
|---|---|---|
| Python | 3.11 | [python.org](https://python.org) |
| Docker | 24+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Docker Compose | v2 (plugin) | Bundled with Docker Desktop |
| AWS CLI | 2.x | [aws.amazon.com/cli](https://aws.amazon.com/cli/) |

---

## Option A — Run with Airflow via Docker Compose (Recommended)

This spins up a full local Airflow environment: Postgres, Redis, a Scheduler,
a Celery Worker, and the Web UI — everything pre-wired.

### Step 1 — Clone & configure secrets

```bash
git clone <your-repo-url>
cd seattle_housing_etl/docker

cp .env.template .env
# Open .env and fill in your AWS credentials and S3 bucket name
```

Your `.env` should look like:
```
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=abc123...
AWS_REGION=us-west-2
S3_BUCKET=my-data-lake-bucket
S3_PREFIX=seattle-housing/raw
```

### Step 2 — Initialise the database (first time only)

```bash
docker compose up airflow-init
```

Wait for the `Airflow initialised ✅` message, then Ctrl-C.

### Step 3 — Start the full stack

```bash
docker compose up -d
```

Services starting up:

| Service | URL / purpose |
|---|---|
| **webserver** | http://localhost:8080 — Airflow UI (admin / admin) |
| **scheduler** | Parses DAGs, triggers runs |
| **worker** | Executes tasks |
| **postgres** | Metadata DB |
| **redis** | Celery broker |

Check status:
```bash
docker compose ps
docker compose logs -f scheduler   # watch the scheduler
```

### Step 4 — Verify the DAG

1. Open http://localhost:8080 and log in with `admin` / `admin`
2. Search for **`seattle_housing_weekly_etl`**
3. Toggle the DAG **on** (the blue switch)
4. The first automatic run fires **next Monday at 07:00 UTC**

### Step 5 — Trigger a manual run (optional)

```bash
# From the UI: click the ▶ "Trigger DAG" button

# Or from the CLI:
docker compose exec scheduler \
  airflow dags trigger seattle_housing_weekly_etl
```

Watch it run under **Graph** or **Grid** view in the UI.

### Step 6 — Tear down

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop + delete all volumes (full reset)
```

---

## Option B — Run Standalone (no Airflow)

Useful for quick local testing before deploying to Airflow.

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-west-2
export S3_BUCKET=your-bucket-name

# Run once
cd seattle_housing_etl
python pipeline.py
```

---

## DAG Details

| Property | Value |
|---|---|
| DAG ID | `seattle_housing_weekly_etl` |
| Schedule | `0 7 * * MON` — Every Monday 07:00 UTC |
| Catchup | Disabled (won't backfill missed runs) |
| Retries | 2 attempts, 5-minute delay |
| Timeout | 15 minutes per task |

### Task Flow

```
start → extract_seattle_housing
      → transform_housing_data
      → load_to_s3
      → log_pipeline_summary
      → end
```

### Airflow Variables

These are seeded automatically from your `.env` file via Docker Compose.
You can also manage them in the UI under **Admin → Variables**.

| Variable key | Description | Default |
|---|---|---|
| `seattle_housing_s3_bucket` | S3 bucket name | *(required)* |
| `seattle_housing_s3_prefix` | S3 key prefix | `seattle-housing/raw` |
| `seattle_housing_aws_region` | AWS region | `us-west-2` |
| `seattle_housing_record_limit` | Number of permits to fetch | `50` |

---

## S3 Output

Files land at a Hive-partitioned path, making them immediately queryable by
Athena, Glue, Spark, or DuckDB without any crawlers:

```
s3://<bucket>/seattle-housing/raw/year=YYYY/month=MM/day=DD/<run_id>.parquet
```

Example Athena query after cataloguing with a Glue Crawler:
```sql
SELECT address, value, issue_date, value_bucket
FROM seattle_housing
WHERE year = '2025' AND month = '05'
ORDER BY issue_date DESC
LIMIT 20;
```

---

## Troubleshooting

**DAG not appearing in UI**
```bash
docker compose exec scheduler airflow dags list
docker compose exec scheduler airflow dags report
```

**Task failing with import errors**
Make sure the project directories are mounted in `docker-compose.yml` and
that `PYTHONPATH` includes `/opt/airflow` (this is the default in the image).

**S3 permission denied**
Ensure your IAM user/role has `s3:PutObject` on the target bucket:
```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject"],
  "Resource": "arn:aws:s3:::your-bucket-name/*"
}
```

**Out of memory on Docker Desktop (Mac/Windows)**
Increase Docker's memory allocation to at least **4 GB** under
Docker Desktop → Settings → Resources.
