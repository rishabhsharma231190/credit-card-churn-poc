"""Package only application source; never archive the working tree wholesale."""
import argparse
import hashlib
from pathlib import Path
import re
import tarfile


def build(root, output, commit):
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("Expected a full commit SHA")
    allowed = [root / "requirements.txt", root / "requirements-native.lock"]
    allowed.extend(root / "scripts" / name for name in
                   ("run_scoring.sh", "deploy_native.py"))
    for directory, pattern in (("churn", "*.py"), ("dags", "*.py")):
        allowed.extend(sorted((root / directory).glob(pattern)))
    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, "w:gz") as archive:
        for source in allowed:
            if source.is_symlink() or not source.is_file():
                raise ValueError(f"Expected a regular source file: {source}")
            archive.add(source, arcname=str(source.relative_to(root)), recursive=False)
        import io
        data = (commit + "\n").encode()
        entry = tarfile.TarInfo("CODE_COMMIT")
        entry.size = len(data)
        entry.mode = 0o644
        archive.addfile(entry, io.BytesIO(data))
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + ".sha256").write_text(digest + "\n")
    print(f"Created {output.name}; SHA256={digest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(Path(__file__).resolve().parents[1], args.output, args.commit)
