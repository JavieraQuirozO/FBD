#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Check whether gene_association parse metadata is present in Supabase and
whether the edge function is returning it correctly.

Designed to run comfortably from Spyder:
- loads .env files automatically
- does not require terminal-exported environment variables

Supported env vars:
- SUPABASE_URL
- SUPABASE_ANON_KEY
- SUPABASE_SERVICE_ROLE_KEY   (optional, but needed to inspect rest/v1/links directly)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import requests


DATASET = "gene_association"


def load_local_env() -> None:
    candidates = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent / "supabase" / ".env",
        Path(__file__).resolve().parent / "supabase" / "tables" / ".env",
    ]

    for env_path in candidates:
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')

            if key and key not in os.environ:
                os.environ[key] = value


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def print_block(title: str, payload) -> None:
    print(f"\n=== {title} ===")
    if isinstance(payload, (dict, list)):
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(payload)


def check_links_row(base_url: str, service_role_key: str | None) -> None:
    if not service_role_key:
        print_block(
            "links row",
            "Skipped direct rest/v1/links check because SUPABASE_SERVICE_ROLE_KEY is not available.",
        )
        return

    url = f"{base_url.rstrip('/')}/rest/v1/links"
    response = requests.get(
        url,
        params={
            "dataset": f"eq.{DATASET}",
            "select": "dataset,filename,header,parser_type,parse_config",
        },
        headers={
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json()
    print_block("links row", rows)


def check_edge_search(base_url: str, anon_key: str | None) -> None:
    url = f"{base_url.rstrip('/')}/functions/v1/fbd/search"
    headers = {"apikey": anon_key} if anon_key else {}
    response = requests.get(
        url,
        params={"q": DATASET},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    print_block("edge /search", response.json())


def check_edge_dataset(base_url: str, anon_key: str | None) -> None:
    url = f"{base_url.rstrip('/')}/functions/v1/fbd/datasets/{DATASET}"
    headers = {"apikey": anon_key} if anon_key else {}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    print_block("edge /datasets/{dataset}", response.json())


def summarize() -> None:
    print("\nWhat to verify:")
    print("- links row should contain parser_type='fb'")
    print("- links row should contain parse_config.start_line")
    print("- edge /search should include parser_type and parse_config")
    print("- edge /datasets/{dataset} should include parser_type and parse_config")
    print("- if links is correct but edge is missing fields, redeploy the edge function")
    print("- if edge is correct but Python still fails, then the issue is in the client path")


def main() -> None:
    load_local_env()

    base_url = require_env("SUPABASE_URL")
    anon_key = os.getenv("SUPABASE_ANON_KEY", "").strip() or None
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip() or None

    print(f"Using SUPABASE_URL={base_url}")
    print(f"Anon key present: {bool(anon_key)}")
    print(f"Service role present: {bool(service_role_key)}")

    check_links_row(base_url, service_role_key)
    check_edge_search(base_url, anon_key)
    check_edge_dataset(base_url, anon_key)
    summarize()


if __name__ == "__main__":
    main()
