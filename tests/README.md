# Validation tests

These tests check the feature contract, arithmetic, deterministic generation, fail-fast data quality and run-ID safety. The full integration run additionally verifies fitting, MLflow registration, model reload and the scoring count.

With Java 17 and the pinned Python requirements installed in a development virtual environment:

```bash
python -m pip install pytest==8.3.5
SPARK_LOCAL_IP=127.0.0.1 SPARK_MASTER='local[2]' python -m pytest -q tests
```

Docker production jobs do not require pytest. For the end-to-end integration check, use the small run from README.

`test_deployment.py` tests deployment success, active-run protection and health-check rollback with fake Docker/AWS executables. It makes no cloud calls.
