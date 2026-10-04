import hashlib
import os
import re
from datetime import date, timedelta

def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Use simple unquoted Snowflake identifiers")
    return value.upper()

def connection():
    import snowflake.connector
    options = {"account": os.environ["SNOWFLAKE_ACCOUNT"], "user": os.environ["SNOWFLAKE_USER"],
               "warehouse": identifier(os.environ["SNOWFLAKE_WAREHOUSE"]),
               "database": identifier(os.environ["SNOWFLAKE_DATABASE"]),
               "schema": identifier(os.environ["SNOWFLAKE_SCHEMA"]),
               "role": identifier(os.environ["SNOWFLAKE_ROLE"]),
               "session_parameters": {"QUERY_TAG": "credit-card-churn-poc", "STATEMENT_TIMEOUT_IN_SECONDS": 3600}}
    options["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"]
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        options["private_key_file_pwd"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    return snowflake.connector.connect(**options)

def table(cfg, kind):
    suffix = hashlib.sha256(cfg.run_id.encode()).hexdigest()[:16].upper()
    return f"POC_{kind.upper()}_{suffix}"

def synthetic_sql(cfg, kind, day):
    # All identifiers are validated and dates parsed before interpolation.
    day = date.fromisoformat(day).isoformat()
    def u(salt):
        return f"MOD(MOD(HASH(customer_id, '{day}', {salt}),1000000)+1000000,1000000)/1000000.0"
    label = "" if kind == "score" else f""",
      IFF({u(11)} < 1/(1+EXP(-(-3.5 + 0.045*inactive_days + 0.28*support_calls_90d
        -0.025*txn_count_30d -0.035*digital_logins_30d
        -1.2*(spend_30d/spend_prev_30d-1)))), 1.0, 0.0)::DOUBLE AS churn_90d"""
    return f"""CREATE OR REPLACE TRANSIENT TABLE {table(cfg, kind)} AS
    WITH ids AS (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS customer_id
                FROM TABLE(GENERATOR(ROWCOUNT => {cfg.rows}))),
    base AS (SELECT customer_id, '{day}'::DATE AS as_of_date,
      FLOOR(3+{u(1)}*117)::DOUBLE AS tenure_months,
      ROUND(1000+{u(2)}*19000,2)::DOUBLE AS credit_limit,
      ROUND(credit_limit*{u(3)},2)::DOUBLE AS balance,
      FLOOR({u(4)}*100)::DOUBLE AS txn_count_30d,
      ROUND(100+{u(5)}*4900,2)::DOUBLE AS spend_prev_30d,
      ROUND(spend_prev_30d*(0.05+{u(6)}*1.5),2)::DOUBLE AS spend_30d,
      FLOOR({u(7)}*90)::DOUBLE AS inactive_days,
      FLOOR({u(8)}*8)::DOUBLE AS support_calls_90d,
      FLOOR(1+{u(9)}*5)::DOUBLE AS products,
      FLOOR({u(10)}*40)::DOUBLE AS digital_logins_30d
      FROM ids)
    SELECT * {label} FROM base"""

def ingest(cfg, store):
    historical = os.getenv("TRAIN_AS_OF", "2026-05-01")
    current = os.getenv("SCORE_AS_OF") or date.today().isoformat()
    if date.fromisoformat(historical)+timedelta(days=90) > date.fromisoformat(current):
        raise ValueError("Training labels must mature before scoring snapshot")
    with connection() as conn:
        with conn.cursor() as cur:
            for kind, day in [("train", historical), ("score", current)]:
                cur.execute(synthetic_sql(cfg, kind, day))
                cur.execute(f"SELECT COUNT(*) FROM {table(cfg, kind)}")
                if cur.fetchone()[0] != cfg.rows:
                    raise ValueError("Snowflake source row count mismatch")
                print(f"Created synthetic {kind} snapshot with {cfg.rows} rows")

def export(cfg, store):
    from .config import BASE_FEATURES
    integration = identifier(os.environ["SNOWFLAKE_STORAGE_INTEGRATION"])
    store.clear_remote("raw")
    with connection() as conn:
        with conn.cursor() as cur:
            for kind in ("train", "score"):
                # Use quoted lowercase aliases and explicit numeric casts for a stable Parquet schema.
                cols = ['customer_id::BIGINT AS "customer_id"', 'as_of_date::DATE AS "as_of_date"']
                cols += [f'{c}::DOUBLE AS "{c}"' for c in BASE_FEATURES + (["churn_90d"] if kind == "train" else [])]
                uri = f"s3://{cfg.bucket}/{cfg.key('raw')}{kind}/"
                cur.execute(f"""COPY INTO '{uri}' FROM (SELECT {', '.join(cols)} FROM {table(cfg, kind)})
                    STORAGE_INTEGRATION = {integration}
                    FILE_FORMAT = (TYPE=PARQUET COMPRESSION=SNAPPY)
                    HEADER=TRUE MAX_FILE_SIZE=134217728 INCLUDE_QUERY_ID=TRUE""")
                print(f"Exported {kind} to {uri}")
    store.s3.put_object(Bucket=cfg.bucket, Key=cfg.key("raw")+"_READY", Body=b"")
