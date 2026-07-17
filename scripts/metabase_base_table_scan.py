#!/usr/bin/env python3
"""
Scan every Metabase question (card) for direct references to specific base-layer
tables, ahead of converting those dbt models to `ephemeral` (which drops their
physical table). Any card that references a flagged table blocks that model's
conversion until the card is repointed at a reporting-layer model.

Auth (env vars):
    METABASE_URL        e.g. https://metabase.company.com
    METABASE_API_KEY    Admin > Settings > API keys (preferred), OR
    METABASE_USERNAME + METABASE_PASSWORD  (session-based fallback)

Usage:
    python metabase_base_table_scan.py [--tables tiktok_base,pinterest_base] [--out report.json]
"""
import argparse
import json
import os
import sys
from typing import Any

import requests

DEFAULT_TARGET_SCHEMAS = ["tiktok_base", "pinterest_base"]

# Specific base table names, for a tighter substring match on native SQL
# (schema-only matching on MBQL still uses DEFAULT_TARGET_SCHEMAS).
DEFAULT_TARGET_TABLES = [
    "tiktok_adgroups", "tiktok_ads", "tiktok_campaigns",
    "tiktok_ads_insights", "tiktok_ads_insights_age",
    "tiktok_campaigns_insights", "tiktok_campaigns_insights_region",
    "pinterest_ad_groups", "pinterest_advertisers", "pinterest_campaigns",
    "pinterest_pins", "pinterest_ad_groups_insights", "pinterest_pins_insights",
]


class MetabaseSession:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def auth_with_api_key(self, api_key: str) -> None:
        self.session.headers["x-api-key"] = api_key

    def auth_with_password(self, username: str, password: str) -> None:
        resp = self.session.post(
            f"{self.base_url}/api/session",
            json={"username": username, "password": password},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["id"]
        self.session.headers["X-Metabase-Session"] = token

    def get(self, path: str, **kwargs) -> Any:
        resp = self.session.get(f"{self.base_url}{path}", timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()


def build_session() -> MetabaseSession:
    base_url = os.environ.get("METABASE_URL")
    if not base_url:
        sys.exit("METABASE_URL is required")

    mb = MetabaseSession(base_url)
    api_key = os.environ.get("METABASE_API_KEY")
    if api_key:
        mb.auth_with_api_key(api_key)
        return mb

    username = os.environ.get("METABASE_USERNAME")
    password = os.environ.get("METABASE_PASSWORD")
    if username and password:
        mb.auth_with_password(username, password)
        return mb

    sys.exit("Set METABASE_API_KEY, or METABASE_USERNAME + METABASE_PASSWORD")


def table_lookup(mb: MetabaseSession) -> dict[int, dict]:
    """table_id -> {schema, name, db_id}, built once to resolve MBQL source-table refs."""
    tables = mb.get("/api/table")
    return {
        t["id"]: {"schema": t.get("schema"), "name": t.get("name"), "db_id": t.get("db_id")}
        for t in tables
    }


def mbql_hits(query: dict, tables_by_id: dict, target_schemas: set[str]) -> list[str]:
    """Walk an MBQL query dict for source-table / join references landing in a target schema."""
    hits = []

    def visit(node: Any):
        if isinstance(node, dict):
            src = node.get("source-table")
            if isinstance(src, int):
                t = tables_by_id.get(src)
                if t and t["schema"] in target_schemas:
                    hits.append(f"{t['schema']}.{t['name']}")
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for v in node:
                visit(v)

    visit(query)
    return hits


def list_all_cards(mb: MetabaseSession) -> list[dict]:
    """Enumerate card stubs via /api/search (avoids /api/card, which 500s on this
    instance — one card's dataset_query has a malformed JSON field that breaks the
    bulk-hydrating endpoint)."""
    cards = []
    offset = 0
    page_size = 100
    while True:
        result = mb.get("/api/search", params={"models": "card", "limit": page_size, "offset": offset})
        batch = result.get("data", [])
        cards.extend(batch)
        total = result.get("total", len(cards))
        offset += page_size
        if offset >= total or not batch:
            break
    return cards


def scan(mb: MetabaseSession, target_schemas: list[str], target_tables: list[str]) -> tuple[list[dict], list[dict]]:
    schemas_set = set(target_schemas)
    tables_lower = [t.lower() for t in target_tables]
    tables_by_id = table_lookup(mb)

    findings = []
    errors = []
    for stub in list_all_cards(mb):
        card_id = stub["id"]
        try:
            detail = mb.get(f"/api/card/{card_id}")
        except requests.exceptions.HTTPError as e:
            errors.append({"card_id": card_id, "name": stub.get("name"), "error": str(e)})
            continue

        dq = detail.get("dataset_query", {})
        qtype = dq.get("type")

        matched: list[str] = []
        if qtype == "native":
            sql = (dq.get("native", {}).get("query") or "").lower()
            for tbl in tables_lower:
                if tbl in sql:
                    matched.append(tbl)
            for schema in schemas_set:
                if schema.lower() in sql and schema.lower() not in matched:
                    matched.append(schema.lower())
        elif qtype == "query":
            matched = mbql_hits(dq.get("query", {}), tables_by_id, schemas_set)

        if matched:
            findings.append({
                "card_id": card_id,
                "name": detail.get("name"),
                "collection_id": detail.get("collection_id"),
                "collection_name": (detail.get("collection") or {}).get("name"),
                "query_type": qtype,
                "matched": sorted(set(matched)),
            })

    return findings, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schemas", default=",".join(DEFAULT_TARGET_SCHEMAS))
    parser.add_argument("--tables", default=",".join(DEFAULT_TARGET_TABLES))
    parser.add_argument("--out", default="metabase_base_table_scan_report.json")
    args = parser.parse_args()

    mb = build_session()
    findings, errors = scan(
        mb,
        target_schemas=[s.strip() for s in args.schemas.split(",") if s.strip()],
        target_tables=[t.strip() for t in args.tables.split(",") if t.strip()],
    )

    with open(args.out, "w") as f:
        json.dump({"findings": findings, "errors": errors}, f, indent=2)

    if not findings:
        print("No live Metabase question references tiktok_base/pinterest_base tables.")
    else:
        print(f"{len(findings)} card(s) reference flagged base tables — see {args.out}:")
        for hit in findings:
            print(f"  card {hit['card_id']} \"{hit['name']}\" "
                  f"(collection: {hit['collection_name']}) -> {hit['matched']}")

    if errors:
        print(f"\n{len(errors)} card(s) could not be inspected (fetch error) — treat as UNKNOWN, not cleared:")
        for e in errors:
            print(f"  card {e['card_id']} \"{e['name']}\": {e['error']}")


if __name__ == "__main__":
    main()
