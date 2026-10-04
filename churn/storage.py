"""Bounded concurrent file transfer; Spark never collects the data into Python.
Single-node POC: S3 files are staged on EBS. Switch to S3A/EMR for multi-node Spark.
"""
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import boto3

class Store:
    def __init__(self, cfg):
        self.cfg = cfg
        self.s3 = boto3.client("s3") if cfg.mode == "cloud" else None

    def clear_remote(self, stage):
        if not self.s3:
            return
        prefix = self.cfg.key(stage)
        if f"/runs/{self.cfg.run_id}/" not in prefix:
            raise ValueError("Refusing to clear outside this run")
        # Only this run/stage is overwritten on retry. Never clear bucket roots.
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.cfg.bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                result = self.s3.delete_objects(Bucket=self.cfg.bucket, Delete={"Objects": keys})
                if result.get("Errors"):
                    raise RuntimeError(result["Errors"])

    def publish(self, stage):
        if not self.s3:
            return
        base = self.cfg.path(stage)
        self.clear_remote(stage)
        paths = [p for p in base.rglob("*") if p.is_file() and not p.name.startswith(".")]
        def upload(p):
            self.s3.upload_file(str(p), self.cfg.bucket, self.cfg.key(stage) + p.relative_to(base).as_posix())
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(upload, paths))
        self.s3.put_object(Bucket=self.cfg.bucket, Key=self.cfg.key(stage)+"_READY", Body=b"")

    def fetch(self, stage, subpath=None):
        base = self.cfg.path(stage) / subpath if subpath else self.cfg.path(stage)
        if not self.s3:
            return base
        # A downstream stage must not consume a partially written upstream result.
        self.s3.head_object(Bucket=self.cfg.bucket, Key=self.cfg.key(stage)+"_READY")
        if base.exists():
            shutil.rmtree(base)
        base.mkdir(parents=True)
        objects = []
        prefix = self.cfg.key(stage) + (subpath.strip("/")+"/" if subpath else "")
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.cfg.bucket, Prefix=prefix):
            objects.extend(o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith("/"))
        def download(key):
            rel = Path(key[len(prefix):])
            if ".." in rel.parts or rel.is_absolute():
                raise ValueError("Unsafe object key")
            path = base / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.cfg.bucket, key, str(path))
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(download, objects))
        return base

    def write_json(self, stage, name, value):
        path = self.cfg.path(stage) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2, default=str))
        return path
