import os
import re
from dataclasses import dataclass
from pathlib import Path

BASE_FEATURES = ["tenure_months", "credit_limit", "balance", "txn_count_30d",
                 "spend_30d", "spend_prev_30d", "inactive_days", "support_calls_90d",
                 "products", "digital_logins_30d"]
FEATURES = BASE_FEATURES + ["utilization", "spend_change", "avg_ticket", "low_engagement"]
FEATURE_VERSION = "v1"

@dataclass
class Config:
    run_id: str
    mode: str
    rows: int

    def __post_init__(self):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", self.run_id):
            raise ValueError("run-id must contain only letters, digits, underscore or hyphen")
        if self.mode not in ("local", "cloud") or not 100 <= self.rows <= 20_000_000:
            raise ValueError("mode must be local/cloud and rows between 100 and 20 million")
        self.root = Path(os.getenv("WORK_ROOT", "/opt/airflow/work")) / self.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.bucket = os.getenv("S3_BUCKET", "")
        self.prefix = os.getenv("S3_PREFIX", "churn-poc").strip("/")
        if self.mode == "cloud" and not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket):
            raise ValueError("Set S3_BUCKET in .env")
        if not re.fullmatch(r"[A-Za-z0-9/_-]+", self.prefix):
            raise ValueError("Invalid S3_PREFIX")
        self.model_name = os.getenv("MODEL_NAME", "credit_card_churn")

    def path(self, stage):
        return self.root / stage

    def key(self, stage):
        return f"{self.prefix}/runs/{self.run_id}/{stage}/"
