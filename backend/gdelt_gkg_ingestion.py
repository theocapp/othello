"""Ingest geolocated events from GDELT Events 2.0 master update files.

GDELT publishes a new 15-minute update file at:
  https://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.export.CSV.zip

Each file contains CAMEO-coded global events with:
  - Precise latitude/longitude for the action location
  - CAMEO event codes (root 18-20 = conflict, 14 = protest, etc.)
  - Number of article mentions and sources
  - Source URL

This gives genuine event-level geolocation rather than country centroids.
"""

import csv
import hashlib
import io
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone

import os

import requests
import urllib3

from corpus import upsert_structured_events

# GDELT's data.gdeltproject.org has a hostname mismatch on its cert;
# disable verification when OTHELLO_ALLOW_INSECURE_GDELT is true (same as news.py).
_ALLOW_INSECURE_GDELT = (
    os.getenv("OTHELLO_ALLOW_INSECURE_GDELT", "true").lower() == "true"
)
if _ALLOW_INSECURE_GDELT:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GDELT_EVENTS_BASE = "https://data.gdeltproject.org/gdeltv2"
GDELT_LASTUPDATE_URL = f"{GDELT_EVENTS_BASE}/lastupdate.txt"

# GDELT Events 2.0 CSV column indices (tab-separated, no header row)
_C_EVENT_ID = 0
_C_DAY = 1
_C_EVENT_CODE = 26
_C_EVENT_ROOT_CODE = 28
_C_QUAD_CLASS = 29
_C_GOLDSTEIN = 30
_C_NUM_MENTIONS = 31
_C_NUM_ARTICLES = 33
_C_ACTION_GEO_TYPE = 51
_C_ACTION_GEO_FULLNAME = 52
_C_ACTION_GEO_COUNTRY = 53
_C_ACTION_GEO_ADM1 = 54
_C_ACTION_GEO_LAT = 56
_C_ACTION_GEO_LON = 57
_C_SOURCE_URL = 60

# QuadClass: 1=Verbal Coop, 2=Material Coop, 3=Verbal Conflict, 4=Material Conflict
_CONFLICT_QUAD = {3, 4}
# Root codes to include (13+ = coercive/conflictual)
_RELEVANT_ROOTS = {"13", "14", "15", "16", "17", "18", "19", "20"}

_CAMEO_ROOT_TO_EVENT_TYPE = {
    "13": "Strategic developments",  # Threaten
    "14": "Protests",  # Protest/demonstrate
    "15": "Explosions/Remote violence",  # Exhibit military posture
    "16": "Strategic developments",  # Reduce relations
    "17": "Battles",  # Coerce
    "18": "Battles",  # Assault
    "19": "Battles",  # Fight
    "20": "Violence against civilians",  # Use unconventional mass violence
}

# Human-readable sub-event descriptions for common CAMEO event codes
_CAMEO_CODE_TO_SUB_EVENT: dict[str, str] = {
    "130": "Threaten",
    "131": "Threaten to boycott",
    "132": "Threaten with sanctions",
    "133": "Threaten to aid rebel forces",
    "134": "Threaten with military force",
    "135": "Threaten with unconventional weapons",
    "136": "Threaten to harm civilians",
    "137": "Threaten to restrict movement",
    "138": "Threaten with coercion",
    "139": "Threaten with unspecified measures",
    "140": "Protest",
    "141": "Demonstrate for political change",
    "142": "Conduct hunger strike",
    "143": "Conduct strike or boycott",
    "144": "Obstruct passage",
    "145": "Protest violently or riot",
    "150": "Exhibit military posture",
    "151": "Increase military alert status",
    "152": "Mobilize armed forces",
    "153": "Mobilize police forces",
    "154": "Conduct naval display",
    "155": "Nuclear posturing",
    "160": "Reduce diplomatic relations",
    "161": "Reduce or sever diplomatic relations",
    "162": "Reduce or stop material aid",
    "163": "Impose embargo or sanctions",
    "164": "Halt negotiations",
    "165": "Cut diplomatic communications",
    "166": "Expel or deport individuals",
    "170": "Coerce",
    "171": "Seize or damage property",
    "172": "Impose administrative sanctions",
    "173": "Arrest or detain",
    "174": "Expel or deport",
    "175": "Use tactics of repression",
    "180": "Assault",
    "181": "Abduct or kidnap",
    "182": "Engage in physical assault",
    "183": "Conduct armed attack",
    "184": "Occupy territory",
    "185": "Engage in armed battle",
    "186": "Conduct suicide or car bombing",
    "190": "Fight",
    "191": "Engage in armed battle",
    "192": "Engage in guerrilla warfare",
    "193": "Fight with artillery and tanks",
    "194": "Employ aerial weapons",
    "195": "Use chemical, biological, or radiological weapons",
    "200": "Use unconventional mass violence",
    "201": "Engage in mass killings",
    "202": "Engage in ethnic cleansing",
    "203": "Use weapons of mass destruction",
    "204": "Use unconventional violence",
}

# Maps ISO 3166-1 alpha-2 codes to country names, plus FIPS 10-4 overrides.
# GDELT uses FIPS codes (e.g. IS=Israel, IR=Iran, TU=Turkey) which differ from ISO.
COUNTRY_CODE_TO_NAME: dict[str, str] = {
    "AF": "Afghanistan",
    "AL": "Albania",
    "DZ": "Algeria",
    "AO": "Angola",
    "AR": "Argentina",
    "AM": "Armenia",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BH": "Bahrain",
    "BD": "Bangladesh",
    "BY": "Belarus",
    "BE": "Belgium",
    "BJ": "Benin",
    "BO": "Bolivia",
    "BA": "Bosnia and Herzegovina",
    "BR": "Brazil",
    "BG": "Bulgaria",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "KH": "Cambodia",
    "CM": "Cameroon",
    "CA": "Canada",
    "CF": "Central African Republic",
    "TD": "Chad",
    "CL": "Chile",
    "CN": "China",
    "CO": "Colombia",
    "CG": "Congo",
    "CD": "DR Congo",
    "HR": "Croatia",
    "CU": "Cuba",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DK": "Denmark",
    "DJ": "Djibouti",
    "EG": "Egypt",
    "SV": "El Salvador",
    "ER": "Eritrea",
    "ET": "Ethiopia",
    "FI": "Finland",
    "FR": "France",
    "GA": "Gabon",
    "GE": "Georgia",
    "DE": "Germany",
    "GH": "Ghana",
    "GR": "Greece",
    "GT": "Guatemala",
    "GN": "Guinea",
    "HT": "Haiti",
    "HN": "Honduras",
    "HU": "Hungary",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran",
    "IQ": "Iraq",
    "IE": "Ireland",
    "IL": "Israel",
    "IT": "Italy",
    "JP": "Japan",
    "JO": "Jordan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KW": "Kuwait",
    "KG": "Kyrgyzstan",
    "LA": "Laos",
    "LB": "Lebanon",
    "LY": "Libya",
    "LT": "Lithuania",
    "MK": "North Macedonia",
    "MG": "Madagascar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "ML": "Mali",
    "MR": "Mauritania",
    "MX": "Mexico",
    "MD": "Moldova",
    "MN": "Mongolia",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NP": "Nepal",
    "NL": "Netherlands",
    "NZ": "New Zealand",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "KP": "North Korea",
    "NO": "Norway",
    "OM": "Oman",
    "PK": "Pakistan",
    "PS": "Palestine",
    "PA": "Panama",
    "PG": "Papua New Guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippines",
    "PL": "Poland",
    "PT": "Portugal",
    "QA": "Qatar",
    "RO": "Romania",
    "RU": "Russia",
    "RW": "Rwanda",
    "SA": "Saudi Arabia",
    "SN": "Senegal",
    "RS": "Serbia",
    "SL": "Sierra Leone",
    "SO": "Somalia",
    "ZA": "South Africa",
    "SS": "South Sudan",
    "ES": "Spain",
    "LK": "Sri Lanka",
    "SD": "Sudan",
    "SY": "Syria",
    "TW": "Taiwan",
    "TJ": "Tajikistan",
    "TZ": "Tanzania",
    "TH": "Thailand",
    "TL": "East Timor",
    "TG": "Togo",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TM": "Turkmenistan",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "United Arab Emirates",
    "GB": "United Kingdom",
    "US": "United States",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "YE": "Yemen",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}

# FIPS 10-4 codes used by GDELT that differ from ISO 3166-1 alpha-2
# These override/supplement the ISO codes above for GDELT compatibility
_FIPS_OVERRIDES: dict[str, str] = {
    "IS": "Israel",
    "TU": "Turkey",
    "AG": "Algeria",
    "AJ": "Azerbaijan",
    "AS": "Australia",
    "BK": "Bosnia and Herzegovina",
    "BL": "Bolivia",
    "BN": "Benin",
    "BO": "Belarus",
    "BU": "Bulgaria",
    "CB": "Cambodia",
    "CD": "DR Congo",
    "CF": "Congo",
    "CG": "DR Congo",
    "CH": "China",
    "CI": "Chile",
    "CK": "Cook Islands",
    "CS": "Costa Rica",
    "CT": "Central African Republic",
    "DA": "Denmark",
    "DR": "Dominican Republic",
    "EI": "Ireland",
    "EN": "Estonia",
    "EZ": "Czech Republic",
    "FI": "Finland",
    "FR": "France",
    "GA": "Gambia",
    "GG": "Georgia",
    "GJ": "Grenada",
    "GM": "Germany",
    "GR": "Greece",
    "GY": "Guyana",
    "HA": "Haiti",
    "HO": "Honduras",
    "HU": "Hungary",
    "IC": "Iceland",
    "IO": "British Indian Ocean Territory",
    "IZ": "Iraq",
    "JA": "Japan",
    "JN": "Jan Mayen",
    "KN": "North Korea",
    "KS": "South Korea",
    "KU": "Kuwait",
    "KZ": "Kazakhstan",
    "LE": "Lebanon",
    "LG": "Latvia",
    "LH": "Lithuania",
    "LI": "Liberia",
    "LO": "Slovakia",
    "LY": "Libya",
    "MA": "Madagascar",
    "MC": "Macau",
    "MG": "Mongolia",
    "MI": "Malawi",
    "MK": "North Macedonia",
    "MO": "Morocco",
    "MP": "Mauritius",
    "MU": "Oman",
    "MX": "Mexico",
    "MY": "Malaysia",
    "MZ": "Mozambique",
    "NI": "Nigeria",
    "NK": "Niue",
    "NL": "Netherlands",
    "NO": "Norway",
    "NP": "Nepal",
    "NS": "Suriname",
    "NU": "Nicaragua",
    "PA": "Paraguay",
    "PE": "Peru",
    "PK": "Pakistan",
    "PL": "Poland",
    "PM": "Panama",
    "PO": "Portugal",
    "PP": "Papua New Guinea",
    "QA": "Qatar",
    "RI": "Serbia",
    "RM": "Marshall Islands",
    "RO": "Romania",
    "RP": "Philippines",
    "RS": "Russia",
    "RW": "Rwanda",
    "SA": "Saudi Arabia",
    "SE": "Seychelles",
    "SF": "South Africa",
    "SG": "Senegal",
    "SI": "Slovenia",
    "SL": "Sierra Leone",
    "SN": "Singapore",
    "SO": "Somalia",
    "SP": "Spain",
    "SU": "Sudan",
    "SW": "Sweden",
    "SZ": "Switzerland",
    "TD": "Trinidad and Tobago",
    "TH": "Thailand",
    "TI": "Tajikistan",
    "TS": "Tunisia",
    "TX": "Turkmenistan",
    "UG": "Uganda",
    "UK": "United Kingdom",
    "UP": "Ukraine",
    "US": "United States",
    "UV": "Burkina Faso",
    "UZ": "Uzbekistan",
    "VE": "Venezuela",
    "VM": "Vietnam",
    "WA": "Namibia",
    "WZ": "Eswatini",
    "YM": "Yemen",
}
COUNTRY_CODE_TO_NAME.update(_FIPS_OVERRIDES)


def _get_recent_update_urls(count: int = 4) -> list[str]:
    """Fetch the GDELT last-update manifest and build a list of export CSV URLs."""
    resp = requests.get(
        GDELT_LASTUPDATE_URL, timeout=15, verify=not _ALLOW_INSECURE_GDELT
    )
    resp.raise_for_status()

    export_url: str | None = None
    # Format: "filesize md5 url" (3 fields)
    for line in resp.text.strip().splitlines():
        parts = line.strip().split()
        url_field = next((p for p in parts if ".export.CSV.zip" in p), None)
        if url_field:
            export_url = url_field
            break

    if not export_url:
        raise RuntimeError("Could not find export URL in GDELT lastupdate.txt")

    urls = [export_url]
    m = re.search(r"/(\d{14})\.export", export_url)
    if m and count > 1:
        ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        for i in range(1, count):
            prev_ts = ts - timedelta(minutes=15 * i)
            urls.append(
                export_url.replace(m.group(1), prev_ts.strftime("%Y%m%d%H%M%S"))
            )

    return urls[:count]


def _parse_events_csv(zip_bytes: bytes) -> list[dict]:
    """Unzip and parse a GDELT Events 2.0 export CSV, returning conflict events."""
    now_ts = time.time()
    events: list[dict] = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".export.CSV")), None)
        if not csv_name:
            return []
        with zf.open(csv_name) as raw:
            reader = csv.reader(
                io.TextIOWrapper(raw, encoding="utf-8", errors="replace"),
                delimiter="\t",
            )
            for row in reader:
                if len(row) <= _C_SOURCE_URL:
                    continue

                root_code = row[_C_EVENT_ROOT_CODE].strip()
                quad_str = row[_C_QUAD_CLASS].strip()
                try:
                    quad = int(quad_str) if quad_str else 0
                except ValueError:
                    continue

                if quad not in _CONFLICT_QUAD and root_code not in _RELEVANT_ROOTS:
                    continue

                lat_s = row[_C_ACTION_GEO_LAT].strip()
                lon_s = row[_C_ACTION_GEO_LON].strip()
                if not lat_s or not lon_s:
                    continue
                try:
                    lat = float(lat_s)
                    lon = float(lon_s)
                except ValueError:
                    continue
                if abs(lat) < 0.01 and abs(lon) < 0.01:
                    continue  # null island

                day_s = row[_C_DAY].strip()
                if len(day_s) < 8:
                    continue
                event_date = f"{day_s[:4]}-{day_s[4:6]}-{day_s[6:8]}"

                country_code = row[_C_ACTION_GEO_COUNTRY].strip()
                country = (
                    COUNTRY_CODE_TO_NAME.get(country_code, country_code) or "Unknown"
                )
                raw_loc_name = row[_C_ACTION_GEO_FULLNAME].strip() or country
                raw_admin1 = row[_C_ACTION_GEO_ADM1].strip() or None
                event_code = row[_C_EVENT_CODE].strip()
                event_type = _CAMEO_ROOT_TO_EVENT_TYPE.get(
                    root_code, "Strategic developments"
                )

                # Clean up GDELT location names — resolve codes, strip qualifiers
                # If GEO_FULLNAME is just a 2-letter code, resolve it to a country name
                if (
                    raw_loc_name
                    and len(raw_loc_name) <= 3
                    and raw_loc_name.upper() == raw_loc_name
                ):
                    loc_name = COUNTRY_CODE_TO_NAME.get(raw_loc_name, country)
                else:
                    # Remove "(general)" qualifier and deduplicate segments
                    loc_name = re.sub(r"\s*\(general\)", "", raw_loc_name).strip()
                    # Deduplicate comma-separated parts (e.g. "Tehran, Tehran, Iran" → "Tehran, Iran")
                    parts = [p.strip() for p in loc_name.split(",") if p.strip()]
                    seen = set()
                    deduped = []
                    for p in parts:
                        # Resolve 2-letter codes in segments
                        resolved = (
                            COUNTRY_CODE_TO_NAME.get(p, p)
                            if len(p) == 2 and p.isupper()
                            else p
                        )
                        key = resolved.lower()
                        if key not in seen:
                            deduped.append(resolved)
                            seen.add(key)
                    loc_name = ", ".join(deduped) if deduped else country

                # GDELT admin codes: "IS00", "USCA", "UKH9", or bare "IS", "UK" — not human-readable
                if raw_admin1 and re.match(r"^[A-Z]{2}[A-Z0-9]{0,4}$", raw_admin1):
                    admin1 = None
                else:
                    admin1 = raw_admin1

                # Get human-readable sub-event type from CAMEO code
                sub_event_type = _CAMEO_CODE_TO_SUB_EVENT.get(event_code, event_type)

                try:
                    goldstein = (
                        float(row[_C_GOLDSTEIN]) if row[_C_GOLDSTEIN].strip() else 0.0
                    )
                    num_mentions = (
                        int(row[_C_NUM_MENTIONS]) if row[_C_NUM_MENTIONS].strip() else 1
                    )
                    num_articles = (
                        int(row[_C_NUM_ARTICLES]) if row[_C_NUM_ARTICLES].strip() else 1
                    )
                except (ValueError, IndexError):
                    goldstein, num_mentions, num_articles = 0.0, 1, 1

                source_url = row[_C_SOURCE_URL].strip()
                event_id_raw = row[_C_EVENT_ID].strip()
                material = f"gdelt_events|{event_id_raw}|{lat:.4f}|{lon:.4f}"
                event_id = "gevt-" + hashlib.sha256(material.encode()).hexdigest()[:18]

                # Build a human-readable summary instead of raw CAMEO codes
                if (
                    loc_name
                    and loc_name.lower() != country.lower()
                    and country.lower() not in loc_name.lower()
                ):
                    place_detail = f"{loc_name}, {country}"
                else:
                    place_detail = loc_name or country
                goldstein_desc = ""
                if goldstein <= -5.0:
                    goldstein_desc = " (high-intensity conflict event)"
                elif goldstein <= -2.0:
                    goldstein_desc = " (escalatory event)"
                elif goldstein >= 5.0:
                    goldstein_desc = " (cooperative development)"
                mention_desc = ""
                if num_mentions >= 10:
                    mention_desc = f" Covered by {num_articles} sources with {num_mentions} mentions."
                elif num_articles >= 3:
                    mention_desc = f" Reported by {num_articles} sources."
                summary_line = f"{event_type} reported in {place_detail} on {event_date}.{goldstein_desc}{mention_desc}"

                events.append(
                    {
                        "event_id": event_id,
                        "dataset": "gdelt_gkg",
                        "dataset_event_id": event_id_raw,
                        "event_date": event_date,
                        "country": country,
                        "region": None,
                        "admin1": admin1,
                        "admin2": None,
                        "location": loc_name,
                        "latitude": lat,
                        "longitude": lon,
                        "event_type": event_type,
                        "sub_event_type": sub_event_type,
                        "actor_primary": None,
                        "actor_secondary": None,
                        "fatalities": None,
                        "source_count": num_articles,
                        "source_urls": [source_url] if source_url else [],
                        "summary": summary_line[:220],
                        "payload": {
                            "event_code": event_code,
                            "root_code": root_code,
                            "goldstein": goldstein,
                            "num_mentions": num_mentions,
                            "quad_class": quad,
                            "source_url": source_url,
                        },
                        "first_ingested_at": now_ts,
                        "last_ingested_at": now_ts,
                    }
                )

    return events


def fetch_gdelt_gkg_events(hours: int = 24) -> tuple[list[dict], dict]:
    """Download recent GDELT Events 2.0 update files and return conflict events.

    Downloads up to `file_count` 15-minute update files (capped to avoid
    excess traffic on repeated calls).
    """
    # Each 15-min file covers one slice; cap at 16 files (~4 h) per refresh
    file_count = min(max(1, int(hours * 4)), 16)

    all_events: list[dict] = []
    seen_ids: set[str] = set()
    errors: list[str] = []

    try:
        urls = _get_recent_update_urls(count=file_count)
    except Exception as exc:
        err = f"master file list: {exc}"
        print(f"[gdelt_events] {err}")
        return [], {"fetched": 0, "errors": [err], "files_attempted": 0}

    for url in urls:
        try:
            resp = requests.get(url, timeout=30, verify=not _ALLOW_INSECURE_GDELT)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            events = _parse_events_csv(resp.content)
        except Exception as exc:
            err = f"{url}: {exc}"
            print(f"[gdelt_events] Failed — {err}")
            errors.append(err)
            continue

        for event in events:
            if event["event_id"] not in seen_ids:
                seen_ids.add(event["event_id"])
                all_events.append(event)

    return all_events, {
        "fetched": len(all_events),
        "files_attempted": len(urls),
        "errors": errors,
    }


def ingest_gdelt_gkg_recent(hours: int = 24) -> dict:
    """Fetch recent GDELT events and store them in structured_events."""
    events, meta = fetch_gdelt_gkg_events(hours=hours)
    inserted = upsert_structured_events(events)
    return {
        "dataset": "gdelt_gkg",
        "hours": hours,
        "fetched": meta["fetched"],
        "inserted_or_updated": inserted,
        "errors": meta["errors"],
        "files_attempted": meta.get("files_attempted", 0),
    }
