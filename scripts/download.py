#!/usr/bin/env python3
"""Download Medicaid provider spending Parquet file from HHS."""

import hashlib
import sys
from pathlib import Path

import urllib.request

from tqdm import tqdm

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DATA_DIR, PARQUET_PATH, PARQUET_SHA256, PARQUET_URL


def sha256_file(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def download_with_progress(url: str, dest: Path, expected_sha256: str) -> Path:
    """Download a file with progress bar and SHA256 verification.

    Skips download if file exists and checksum matches.
    Supports resumable downloads via HTTP Range headers.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Check if already downloaded and valid
    if dest.exists():
        print(f"File exists: {dest}")
        print("Verifying SHA256...", end=" ", flush=True)
        actual = sha256_file(dest)
        if actual == expected_sha256:
            print("OK")
            return dest
        else:
            print(f"MISMATCH (expected {expected_sha256[:16]}..., got {actual[:16]}...)")
            print("Re-downloading...")

    # Get file size for progress bar
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req) as resp:
        total_size = int(resp.headers.get("Content-Length", 0))

    # Check for partial download (resume support)
    partial_path = dest.with_suffix(dest.suffix + ".partial")
    existing_size = partial_path.stat().st_size if partial_path.exists() else 0

    headers = {}
    if existing_size > 0 and existing_size < total_size:
        headers["Range"] = f"bytes={existing_size}-"
        print(f"Resuming download from {existing_size / (1024**3):.2f} GB...")
        mode = "ab"
    else:
        existing_size = 0
        mode = "wb"

    req = urllib.request.Request(url, headers=headers)

    print(f"Downloading: {url}")
    print(f"Destination: {dest}")
    if total_size > 0:
        print(f"Size: {total_size / (1024**3):.2f} GB")

    with urllib.request.urlopen(req) as resp:
        with open(partial_path, mode) as f:
            with tqdm(
                total=total_size,
                initial=existing_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=dest.name,
            ) as pbar:
                while chunk := resp.read(1024 * 1024):
                    f.write(chunk)
                    pbar.update(len(chunk))

    # Verify
    print("Verifying SHA256...", end=" ", flush=True)
    actual = sha256_file(partial_path)
    if actual != expected_sha256:
        print(f"FAILED")
        print(f"  Expected: {expected_sha256}")
        print(f"  Got:      {actual}")
        partial_path.unlink(missing_ok=True)
        raise ValueError(f"SHA256 mismatch for {dest.name}")
    print("OK")

    # Rename to final path
    partial_path.rename(dest)
    return dest


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("ΛXYM Research — Data Download")
    print("=" * 60)
    download_with_progress(PARQUET_URL, PARQUET_PATH, PARQUET_SHA256)
    print(f"\nDone. Parquet file ready at: {PARQUET_PATH}")


if __name__ == "__main__":
    main()
