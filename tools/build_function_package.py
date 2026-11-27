"""Package functions/* and src/foundry_batch into dist/functionapp.zip.

Deterministic (sorted entries, fixed timestamps) so the same commit yields the same artifact,
and the artifact name carries the commit id for traceability (US-8.1).

When functions/requirements.lock exists (pip-compile --generate-hashes, see `make lock`), its
content is packaged as requirements.txt. `--require-lock` fails the build without a lock (the
main branch uses it).

`--site-packages DIR` ships dependencies the pipeline installed on the build agent (from the
lock, hashes enforced by pip) under `.python_packages/lib/site-packages`, so the app is deployed
without a remote build and never downloads packages from the internet (private-only egress,
ADR 0010).
"""

from __future__ import annotations

import argparse
import hashlib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXED_TIME = (2020, 1, 1, 0, 0, 0)
EXCLUDE_PARTS = {"__pycache__", ".venv", "tests"}
EXCLUDE_NAMES = {"local.settings.json", "local.settings.example.json"}
LOCK_NAME = "requirements.lock"
REQUIRED_ENTRIES = ("function_app.py", "host.json", "requirements.txt")
SITE_PACKAGES = ".python_packages/lib/site-packages"
VENDOR_EXCLUDE_PARTS = {"__pycache__"}


class PackageError(Exception):
    """The package would not deploy: a required entry or the dependency lock is missing."""


def _excluded(path: Path, base: Path) -> bool:
    # Only the parts below the source folder count: a checkout under any folder named like an
    # excluded part (for example /agent/_work/tests/s) must not empty the package.
    return bool(EXCLUDE_PARTS & set(path.relative_to(base).parts))


def files(root: Path = ROOT) -> list[tuple[Path, str]]:
    entries: list[tuple[Path, str]] = []
    functions = root / "functions"
    for path in sorted(functions.rglob("*")):
        if not path.is_file() or _excluded(path, functions) or path.name in EXCLUDE_NAMES:
            continue
        name = path.relative_to(functions).as_posix()
        if name == LOCK_NAME:
            continue
        locked = name == "requirements.txt" and (functions / LOCK_NAME).is_file()
        entries.append((functions / LOCK_NAME if locked else path, name))
    library = root / "src"
    for path in sorted((library / "foundry_batch").rglob("*.py")):
        if not _excluded(path, library):
            entries.append((path, path.relative_to(library).as_posix()))
    names = {name for _, name in entries}
    missing = [name for name in REQUIRED_ENTRIES if name not in names]
    if missing or not any(name.startswith("foundry_batch/") for name in names):
        raise PackageError(f"package incomplete, missing: {', '.join(missing) or 'foundry_batch'}")
    return entries


def vendored(site_packages: Path) -> list[tuple[Path, str]]:
    """Dependencies installed on the build agent, placed where the Python worker loads them."""
    if not site_packages.is_dir():
        raise PackageError(f"site-packages directory not found: {site_packages}")
    entries = [
        (path, f"{SITE_PACKAGES}/{path.relative_to(site_packages).as_posix()}")
        for path in sorted(site_packages.rglob("*"))
        if path.is_file()
        and not VENDOR_EXCLUDE_PARTS & set(path.relative_to(site_packages).parts)
        and path.suffix != ".pyc"
    ]
    if not entries:
        raise PackageError(f"site-packages directory is empty: {site_packages}")
    return entries


def build(
    output: Path,
    root: Path = ROOT,
    *,
    require_lock: bool = False,
    site_packages: Path | None = None,
) -> str:
    if require_lock and not (root / "functions" / LOCK_NAME).is_file():
        raise PackageError(f"functions/{LOCK_NAME} is missing; run `make lock` and commit it")
    entries = files(root)
    if site_packages is not None:
        entries += vendored(site_packages)
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, name in entries:
            info = zipfile.ZipInfo(name, FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())
    return hashlib.sha256(output.read_bytes()).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(ROOT / "dist" / "functionapp.zip"))
    parser.add_argument(
        "--require-lock",
        action="store_true",
        help=f"fail unless functions/{LOCK_NAME} pins the runtime dependencies",
    )
    parser.add_argument(
        "--site-packages",
        help="installed dependencies to ship in .python_packages (deploy without remote build)",
    )
    args = parser.parse_args(argv)
    vendor = Path(args.site_packages) if args.site_packages else None
    try:
        digest = build(Path(args.output), require_lock=args.require_lock, site_packages=vendor)
    except PackageError as error:
        print(f"error: {error}")
        return 1
    print(f"{args.output} sha256={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
