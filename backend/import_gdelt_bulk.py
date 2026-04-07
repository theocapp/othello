import argparse
import csv
import json
import re
import zipfile
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from urllib.parse import urlparse

from cache import init_db as init_cache_db
from corpus import init_db as init_corpus_db
from corpus import upsert_historical_url_queue
from news import TOPIC_KEYWORDS

TOPICS = tuple(TOPIC_KEYWORDS.keys())

URL_KEYS = [
    "url",
    "URL",
    "sourceurl",
    "SOURCEURL",
    "source_url",
    "SourceURL",
    "documentidentifier",
    "DocumentIdentifier",
    "document_identifier",
    "link",
]

TITLE_KEYS = [
    "title",
    "Title",
    "headline",
    "name",
    "V2Title",
]

SOURCE_NAME_KEYS = [
    "source",
    "Source",
    "source_name",
    "SourceCommonName",
    "sourcecommonname",
    "domain",
    "Domain",
]

DOMAIN_KEYS = [
    "source_domain",
    "domain",
    "Domain",
    "sourceurl_domain",
    "SOURCEURL_DOMAIN",
]

LANGUAGE_KEYS = [
    "language",
    "Language",
    "source_lang",
    "SourceLanguage",
]

PUBLISHED_AT_KEYS = [
    "published_at",
    "PublishedAt",
    "date",
    "DATE",
    "Date",
    "datetime",
    "DATEADDED",
    "dateadded",
    "seendate",
    "SEENDATE",
    "seen_date",
]

QUERY_KEYS = [
    "query",
    "Query",
    "gdelt_query",
    "GDELTQuery",
]

WINDOW_START_KEYS = [
    "window_start",
    "WindowStart",
    "gdelt_window_start",
    "startdatetime",
    "STARTDATETIME",
]

WINDOW_END_KEYS = [
    "window_end",
    "WindowEnd",
    "gdelt_window_end",
    "enddatetime",
    "ENDDATETIME",
]

SIGNAL_KEYS = [
    "title",
    "Title",
    "V2Themes",
    "v2themes",
    "Themes",
    "themes",
    "V2EnhancedThemes",
    "V2Locations",
    "locations",
    "V2Persons",
    "persons",
    "V2Organizations",
    "organizations",
    "source",
    "Source",
    "domain",
    "Domain",
]

GDELT_MENTIONS_COLUMNS = [
    "GlobalEventID",
    "EventTimeDate",
    "MentionTimeDate",
    "MentionType",
    "MentionSourceName",
    "MentionIdentifier",
    "SentenceID",
    "Actor1CharOffset",
    "Actor2CharOffset",
    "ActionCharOffset",
    "InRawText",
    "Confidence",
    "MentionDocLen",
    "MentionDocTone",
    "MentionDocTranslationInfo",
]

GDELT_GKG_COLUMNS = [
    "GKGRECORDID",
    "DATE",
    "SourceCollectionIdentifier",
    "SourceCommonName",
    "DocumentIdentifier",
    "Counts",
    "V2Counts",
    "Themes",
    "V2Themes",
    "Locations",
    "V2Locations",
    "Persons",
    "V2Persons",
    "Organizations",
    "V2Organizations",
    "Tone",
    "Dates",
    "GCAM",
    "SharingImage",
    "RelatedImages",
    "SocialImageEmbeds",
    "SocialVideoEmbeds",
    "Quotations",
    "AllNames",
    "Amounts",
    "TranslationInfo",
    "ExtrasXML",
]


def _extract_value(record: dict, keys: list[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
        if value not in (None, ""):
            return str(value)
    return None


def _normalize_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda raw: datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=UTC),
        lambda raw: datetime.strptime(raw, "%Y%m%d").replace(tzinfo=UTC),
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
    ):
        try:
            return parser(text).isoformat()
        except ValueError:
            continue
    return text


def _normalize_domain(url: str | None, fallback: str | None = None) -> str | None:
    candidate = (fallback or "").strip()
    if candidate:
        return candidate.lower()
    if not url:
        return None
    return urlparse(url).netloc.lower() or None


def _infer_topic_guess(record: dict) -> str | None:
    signals = " ".join(str(record.get(key) or "") for key in SIGNAL_KEYS).lower()
    best_topic = None
    best_score = 0
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in signals)
        if score > best_score:
            best_topic = topic
            best_score = score
    return best_topic if best_score else None


def _normalize_queue_record(
    record: dict, discovered_via: str, archive_member: str | None = None
) -> dict | None:
    url = _extract_value(record, URL_KEYS)
    if not url:
        return None
    normalized = {
        "url": url,
        "title": _extract_value(record, TITLE_KEYS),
        "source_name": _extract_value(record, SOURCE_NAME_KEYS),
        "source_domain": _normalize_domain(
            url,
            _extract_value(record, DOMAIN_KEYS),
        ),
        "published_at": _normalize_timestamp(_extract_value(record, PUBLISHED_AT_KEYS)),
        "language": _extract_value(record, LANGUAGE_KEYS),
        "discovered_via": discovered_via,
        "topic_guess": _infer_topic_guess(record),
        "gdelt_query": _extract_value(record, QUERY_KEYS),
        "gdelt_window_start": _normalize_timestamp(
            _extract_value(record, WINDOW_START_KEYS)
        ),
        "gdelt_window_end": _normalize_timestamp(
            _extract_value(record, WINDOW_END_KEYS)
        ),
        "fetch_status": "pending",
        "attempt_count": 0,
        "payload": {
            "raw_record": record,
            "archive_member": archive_member,
        },
    }
    return normalized


def _looks_like_gdelt_mentions(lines: list[str]) -> bool:
    if not lines:
        return False
    first = lines[0].strip()
    if not first:
        return False
    if "MentionIdentifier" in first:
        return False
    parts = first.split("\t")
    if len(parts) < 6:
        return False
    return (
        parts[0].isdigit()
        and parts[1].isdigit()
        and parts[2].isdigit()
        and parts[5].startswith("http")
    )


def _looks_like_gdelt_gkg(lines: list[str]) -> bool:
    if not lines:
        return False
    first = lines[0].strip()
    if not first:
        return False
    if "DocumentIdentifier" in first:
        return False
    parts = first.split("\t")
    if len(parts) < 5:
        return False
    return "-" in parts[0] and parts[1].isdigit() and parts[4].startswith("http")


def _load_gdelt_mentions_records(lines: list[str]) -> list[dict]:
    records = []
    for line in lines:
        text = line.strip()
        if not text:
            continue
        parts = text.split("\t")
        if len(parts) < len(GDELT_MENTIONS_COLUMNS):
            parts.extend([""] * (len(GDELT_MENTIONS_COLUMNS) - len(parts)))
        row = dict(zip(GDELT_MENTIONS_COLUMNS, parts[: len(GDELT_MENTIONS_COLUMNS)]))
        row["source_domain"] = row.get("MentionSourceName")
        row["url"] = row.get("MentionIdentifier")
        row["published_at"] = row.get("MentionTimeDate")
        records.append(row)
    return records


def _page_title_from_extras(raw: str) -> str | None:
    match = re.search(
        r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", raw or "", flags=re.IGNORECASE | re.DOTALL
    )
    if not match:
        return None
    return match.group(1).strip() or None


def _load_gdelt_gkg_records(lines: list[str]) -> list[dict]:
    records = []
    for line in lines:
        text = line.strip()
        if not text:
            continue
        parts = text.split("\t")
        if len(parts) < len(GDELT_GKG_COLUMNS):
            parts.extend([""] * (len(GDELT_GKG_COLUMNS) - len(parts)))
        row = dict(zip(GDELT_GKG_COLUMNS, parts[: len(GDELT_GKG_COLUMNS)]))
        row["source_domain"] = row.get("SourceCommonName")
        row["source_name"] = row.get("SourceCommonName")
        row["url"] = row.get("DocumentIdentifier")
        row["published_at"] = row.get("DATE")
        row["title"] = _page_title_from_extras(row.get("ExtrasXML") or "")
        records.append(row)
    return records


def _load_csv_records(handle, delimiter: str = ",") -> list[dict]:
    lines = list(handle)
    if delimiter == "\t" and _looks_like_gdelt_mentions(lines[:5]):
        return _load_gdelt_mentions_records(lines)
    if delimiter == "\t" and _looks_like_gdelt_gkg(lines[:5]):
        return _load_gdelt_gkg_records(lines)
    return [
        dict(row)
        for row in csv.DictReader(StringIO("".join(lines)), delimiter=delimiter)
    ]


def _load_json_records(handle) -> list[dict]:
    payload = json.load(handle)
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("articles", "items", "data", "records", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    raise ValueError("Unsupported JSON structure for GDELT bulk import")


def _load_jsonl_records(handle) -> list[dict]:
    records = []
    for line in handle:
        text = line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _detect_member_format(name: str) -> str:
    suffixes = Path(name).suffixes
    if suffixes[-2:] == [".jsonl", ".gz"]:
        return "jsonl"
    suffix = Path(name).suffix.lower()
    lowered = name.lower()
    if lowered.endswith(".mentions.csv") or lowered.endswith(".mentions.csv.zip"):
        return "tsv"
    if lowered.endswith(".gkg.csv") or lowered.endswith(".gkg.csv.zip"):
        return "tsv"
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix == ".json":
        return "json"
    raise ValueError(f"Unsupported bulk member format: {name}")


def _load_records_from_handle(handle, format_name: str) -> list[dict]:
    if format_name == "csv":
        return _load_csv_records(handle, delimiter=",")
    if format_name == "tsv":
        return _load_csv_records(handle, delimiter="\t")
    if format_name == "json":
        return _load_json_records(handle)
    if format_name == "jsonl":
        return _load_jsonl_records(handle)
    raise ValueError(f"Unsupported format: {format_name}")


def _detect_format(path: Path, explicit: str) -> str:
    if explicit != "auto":
        return explicit
    if path.suffix.lower() == ".zip":
        return "zip"
    return _detect_member_format(path.name)


def _load_archive_records(
    path: Path, format_name: str
) -> list[tuple[dict, str | None]]:
    records: list[tuple[dict, str | None]] = []
    if format_name == "zip":
        with zipfile.ZipFile(path) as archive:
            for member in archive.namelist():
                if member.endswith("/"):
                    continue
                try:
                    member_format = _detect_member_format(member)
                except ValueError:
                    continue
                with archive.open(member) as raw_handle:
                    text_handle = (
                        line.decode("utf-8", errors="replace") for line in raw_handle
                    )
                    if member_format in {"csv", "tsv"}:
                        loaded = _load_records_from_handle(text_handle, member_format)
                    else:
                        loaded = _load_records_from_handle(
                            StringIO("".join(text_handle)), member_format
                        )
                records.extend((record, member) for record in loaded)
        return records

    with path.open(encoding="utf-8", errors="replace", newline="") as handle:
        loaded = _load_records_from_handle(handle, format_name)
    return [(record, None) for record in loaded]


def _batch(records: list[dict], batch_size: int) -> list[list[dict]]:
    return [
        records[index : index + batch_size]
        for index in range(0, len(records), batch_size)
    ]


def import_gdelt_bulk_archive(
    path: Path, format_name: str, discovered_via: str, batch_size: int
) -> dict:
    init_cache_db()
    init_corpus_db()

    loaded_records = _load_archive_records(path, format_name)
    normalized_records: list[dict] = []
    skipped = 0
    topic_counts = {topic: 0 for topic in TOPICS}

    for record, archive_member in loaded_records:
        normalized = _normalize_queue_record(
            record, discovered_via=discovered_via, archive_member=archive_member
        )
        if not normalized:
            skipped += 1
            continue
        topic_guess = normalized.get("topic_guess")
        if topic_guess in topic_counts:
            topic_counts[topic_guess] += 1
        normalized_records.append(normalized)

    inserted_or_updated = 0
    for batch in _batch(normalized_records, max(1, batch_size)):
        inserted_or_updated += upsert_historical_url_queue(batch)

    return {
        "path": str(path),
        "format": format_name,
        "discovered_via": discovered_via,
        "records_seen": len(loaded_records),
        "queued": len(normalized_records),
        "skipped": skipped,
        "inserted_or_updated": inserted_or_updated,
        "topic_guesses": topic_counts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Queue GDELT bulk discovery records into Othello's historical URL queue."
    )
    parser.add_argument(
        "path", help="Path to a GDELT bulk CSV, TSV, JSON, JSONL, or zip archive."
    )
    parser.add_argument(
        "--format",
        default="auto",
        choices=["auto", "csv", "tsv", "json", "jsonl", "zip"],
    )
    parser.add_argument(
        "--discovered-via",
        default="gdelt-bulk",
        help="Discovery label stored on queued URLs.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Batch size for queue writes."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.path).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Archive not found: {path}")

    result = import_gdelt_bulk_archive(
        path=path,
        format_name=_detect_format(path, args.format),
        discovered_via=args.discovered_via,
        batch_size=args.batch_size,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
