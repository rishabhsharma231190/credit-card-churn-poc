import argparse
import json
import time
from .config import Config
from .storage import Store

STAGES = ["ingest", "export", "prepare", "features", "train", "score"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=STAGES+["all"])
    parser.add_argument("--mode", choices=["local", "cloud"], default="local")
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    cfg = Config(args.run_id, args.mode, args.rows)
    store = Store(cfg)
    for stage in STAGES if args.stage == "all" else [args.stage]:
        started = time.monotonic()
        spark = None
        try:
            if stage in ["ingest", "export"] and cfg.mode == "cloud":
                from .snowflake_io import ingest, export
                {"ingest": ingest, "export": export}[stage](cfg, store)
            elif stage == "export":
                print("LOCAL DEMO: Snowflake/S3 transfer bypassed; raw Parquet already exists")
            else:
                from .spark import session
                spark = session(stage)
                if stage == "ingest":
                    from .generate import generate
                    generate(cfg, spark, store)
                else:
                    from importlib import import_module
                    getattr(import_module("churn."+stage), stage)(cfg, spark, store)
        finally:
            if spark:
                spark.stop()
        elapsed = round(time.monotonic()-started, 2)
        store.write_json("timings", stage+".json", {"stage": stage, "seconds": elapsed})
        store.publish("timings")
        print(json.dumps({"stage": stage, "seconds": elapsed, "status": "success"}))

if __name__ == "__main__":
    main()
