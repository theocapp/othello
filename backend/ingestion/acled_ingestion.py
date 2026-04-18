"""ACLED ingestion moved into `backend.ingestion` package.

This module was moved from the top-level `acled_ingestion.py` to the
`backend/ingestion/` package as part of the domain refactor. The code
was kept intact to preserve behavior.
"""

import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests

from corpus import (
    get_source_registry,
    upsert_historical_url_queue,
    upsert_structured_events,
)
from db.events_repo import deduplicate_cross_dataset_events

ACLED_AUTH_URL = "https://acleddata.com/oauth/token"
ACLED_EVENTS_URL = "https://acleddata.com/api/acled/read"

_token_cache = {"access_token": None, "expires_at": 0.0}


def _queue_urls_for_fetch(urls: list[str], *, discovered_via: str) -> int:
    now = time.time()
    seen: set[str] = set()
    records: list[dict] = []
    for raw in urls:
        url = str(raw or "").strip()
        if not url or not url.startswith("http") or url in seen:
            continue
        seen.add(url)
        domain = urlparse(url).netloc.lower() or None
        records.append(
            {
                "url": url,
                "canonical_url": url,
                "title": None,
                "source_name": domain,
                "source_domain": domain,
                "published_at": None,
                "language": None,
                "discovered_via": discovered_via,
                "topic_guess": "geopolitics",
                "gdelt_query": None,
                "gdelt_window_start": None,
                "gdelt_window_end": None,
                "fetch_status": "pending",
                "last_attempt_at": None,
                "attempt_count": 0,
                "payload": {
                    "queued_from": discovered_via,
                    "queued_at": now,
                    "url": url,
                    "source_domain": domain,
                },
            }
        )
    return upsert_historical_url_queue(records)


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "OthelloV2/1.0 (+acled ingestion)",
            "Accept": "application/json",
        }
    )
    return session


def _registry_source() -> dict | None:
    rows = get_source_registry(source_type="structured_event", active_only=True)
    for row in rows:
        if row["source_name"] == "ACLED":
            return row
    return None


def _get_token(session: requests.Session) -> str:
    if _token_cache["access_token"] and _token_cache["expires_at"] > time.time() + 60:
        return _token_cache["access_token"]

    username = os.getenv("ACLED_EMAIL") or os.getenv("ACLED_USERNAME")
    password = os.getenv("ACLED_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "ACLED_EMAIL/ACLED_PASSWORD are required for ACLED ingestion."
        )

    response = session.post(
        ACLED_AUTH_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "username": username,
            "password": password,
            "grant_type": "password",
            "client_id": "acled",
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))
    if not access_token:
        raise RuntimeError("ACLED auth response did not include access_token.")
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = time.time() + expires_in
    return access_token


def _normalize_event(record: dict) -> dict:
    event_date = record.get("event_date")
    source_urls = []
    source_url = record.get("source_url")
    if source_url:
        source_urls.append(source_url)
    notes = record.get("notes") or ""

    # Build a meaningful summary even when ACLED notes field is empty
    if notes.strip():
        summary = notes[:400]
    else:
        et = (record.get("event_type") or "Incident").strip()
        sub = (record.get("sub_event_type") or "").strip()
        loc = (record.get("location") or "").strip()
        admin1 = (record.get("admin1") or "").strip()
        country = (record.get("country") or "").strip()
        a1 = (record.get("actor1") or "").strip()
        a2 = (record.get("actor2") or "").strip()
        fatalities = (
            int(record["fatalities"])
            if record.get("fatalities") not in (None, "")
            else 0
        )

        # Use sub_event_type for specificity when available
        action = sub if sub and sub.lower() != et.lower() else et
        place_in = (
            f"in {loc}"
            if loc
            else (f"in {admin1}" if admin1 else (f"in {country}" if country else ""))
        )

        parts = []
        if a1 and a2:
            parts.append(f"{action} involving {a1} and {a2} {place_in}".strip())
        elif a1:
            parts.append(f"{action} involving {a1} {place_in}".strip())
        else:
            parts.append(f"{action} reported {place_in}".strip())

        if fatalities:
            parts.append(f"{fatalities} fatalities reported")
        if event_date:
            parts.append(f"on {event_date}")

        summary = ". ".join(parts) + "."

    return {
        "event_id": f"acled-{record.get('event_id_cnty') or record.get('event_id_no_cnty') or record.get('event_id')}",
        "dataset": "acled",
        "dataset_event_id": str(
            record.get("event_id_cnty")
            or record.get("event_id_no_cnty")
            or record.get("event_id")
            or ""
        ),
        "event_date": event_date,
        "country": record.get("country"),
        "region": record.get("region"),
        "admin1": record.get("admin1"),
        "admin2": record.get("admin2"),
        "location": record.get("location"),
        "latitude": (
            float(record["latitude"])
            if record.get("latitude") not in (None, "")
            else None
        ),
        "longitude": (
            float(record["longitude"])
            if record.get("longitude") not in (None, "")
            else None
        ),
        "event_type": record.get("event_type"),
        "sub_event_type": record.get("sub_event_type"),
        "actor_primary": record.get("actor1"),
        "actor_secondary": record.get("actor2"),
        "fatalities": (
            int(record["fatalities"])
            if record.get("fatalities") not in (None, "")
            else None
        ),
        "source_count": (
            int(record["source_scale"])
            if str(record.get("source_scale", "")).isdigit()
            else None
        ),
        "source_urls": source_urls,
        "summary": summary,
        "payload": record,
        "first_ingested_at": time.time(),
        "last_ingested_at": time.time(),
    }


def _fetch_event_page(
    session: requests.Session,
    token: str,
    *,
    start_date: datetime,
    end_date: datetime,
    limit: int,
    page: int,
) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "event_date": f"{start_date.date().isoformat()}|{end_date.date().isoformat()}",
        "event_date_where": "BETWEEN",
        "limit": limit,
        "page": page,
        "_format": "json",
    }
    response = session.get(ACLED_EVENTS_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_acled_events(days: int = 2, limit: int = 500) -> tuple[list[dict], dict]:
    session = _session()
    token = _get_token(session)
    window_days = max(1, days)
    end_date = datetime.now(timezone.utc) - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)

    all_events = []
    total_count = None
    page = 1
    while True:
        payload = _fetch_event_page(
            session,
            token,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            page=page,
        )
        records = payload.get("data") or payload.get("results") or []
        total_count = int(
            payload.get("total_count") or payload.get("count") or len(records)
        )
        all_events.extend(records)
        if len(records) < limit or len(all_events) >= total_count:
            break
        page += 1

    normalized = []
    seen = set()
    for record in all_events:
        event = _normalize_event(record)
        if not event["dataset_event_id"] or event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        normalized.append(event)
    return normalized, {
        "queried_start": start_date.date().isoformat(),
        "queried_end": end_date.date().isoformat(),
        "fetched": len(normalized),
        "raw_records": len(all_events),
        "total_count": total_count or len(all_events),
    }


def ingest_acled_recent(days: int = 2, limit: int = 500) -> dict:
    source = _registry_source()
    if not source:
        raise RuntimeError("ACLED source is not present in source_registry.")
    events, meta = fetch_acled_events(days=days, limit=limit)
    inserted = upsert_structured_events(events)
    queued_urls: list[str] = []
    seen_queued_urls: set[str] = set()
    for event in events:
        for url in event.get("source_urls") or []:
            normalized = str(url or "").strip()
            if not normalized or normalized in seen_queued_urls:
                continue
            seen_queued_urls.add(normalized)
            queued_urls.append(normalized)
    queued = _queue_urls_for_fetch(queued_urls, discovered_via="acled-structured") if queued_urls else 0
    dedup_result = deduplicate_cross_dataset_events(days=3)
    print(f"[acled] Cross-dataset dedup: {dedup_result}")
    return {
        "source_name": source["source_name"],
        "days": days,
        "queried_start": meta["queried_start"],
        "queried_end": meta["queried_end"],
        "fetched": len(events),
        "raw_records": meta["raw_records"],
        "inserted_or_updated": inserted,
        "queued_for_fetch": queued,
    }
