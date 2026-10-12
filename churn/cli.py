import argparse
import json
import time
from .config import Config
from .storage import Store

STAGES = ["ingest", "export", "prepare", "features", "train", "score"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=STAGES+["all", "export_existing", "resolve_model"])
    parser.add_argument("--mode", choices=["local", "cloud"], default="local")
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scoring-only", action="store_true")
    args = parser.parse_args()
    if args.scoring_only and args.stage in ("ingest", "train", "all", "export"):
        parser.error("Scoring-only mode cannot ingest, train, or export generated tables")
    if args.stage in ("export_existing", "resolve_model") and not args.scoring_only:
        parser.error("This stage requires --scoring-only")
    cfg = Config(args.run_id, args.mode, args.rows, scoring_only=args.scoring_only)
    store = Store(cfg)
    for stage in STAGES if args.stage == "all" else [args.stage]:
        started = time.monotonic()
        spark = None
        try:
            if stage == "resolve_model":
                from .resolve_model import resolve_model
                resolve_model(cfg, store)
            elif stage == "export_existing":
                if cfg.mode == "cloud":
                    from .snowflake_io import export_existing
                    export_existing(cfg, store)
                elif not (cfg.path("raw") / "score").is_dir():
                    raise ValueError("Local scoring requires an existing raw/score Parquet dataset")
            elif stage in ["ingest", "export"] and cfg.mode == "cloud":
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
