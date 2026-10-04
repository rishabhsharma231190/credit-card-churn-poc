"""Run once from the project folder with any local Python 3 interpreter."""
import base64
import secrets
from pathlib import Path
p = Path(".env")
if p.exists():
    raise SystemExit(".env already exists; edit it rather than overwriting your configuration")
text = Path(".env.example").read_text()
text = text.replace("CHANGE_ME_LOCAL_PASSWORD", secrets.token_hex(12))
text = text.replace("CHANGE_ME_DATABASE_PASSWORD", secrets.token_hex(24))
text = text.replace("CHANGE_ME_RANDOM_LONG_STRING", secrets.token_hex(32))
text = text.replace("AIRFLOW_FERNET_KEY=", "AIRFLOW_FERNET_KEY=" + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())
p.write_text(text)
p.chmod(0o600)
Path("secrets").mkdir(exist_ok=True)
print("Created .env. Open it to view your Airflow admin password.")
