# data-job/fetch_members.py

import json
import os
import subprocess
import tempfile
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

try:
    import yaml
except ImportError:
    raise SystemExit("Please `pip install pyyaml` first.")

CONGRESS_REPO = "https://github.com/unitedstates/congress-legislators.git"

# The base for the Congress.gov API. Your key from api.data.gov must be passed
# as a *query parameter* (api_key=...) – NOT as an X-API-Key header.
API_BASE = "https://api.congress.gov/v3"
API_KEY = os.environ.get("CONGRESS_GOV_API_KEY")


# ---------- HTTP helper ----------
def http_json(path: str, params=None, headers=None, timeout=30, max_retries=3):
    """
    GET JSON from Congress.gov via api.data.gov.
    - Adds `api_key` query param (required by api.data.gov gateway)
    - URL-encodes parameters
    - Sets a User-Agent
    - Retries on transient HTTP errors
    """
    params = dict(params or {})
    params.setdefault("format", "json")
    if API_KEY:
        params["api_key"] = API_KEY  # <-- REQUIRED for api.data.gov proxy

    q = urllib.parse.urlencode(params, doseq=True)
    full_url = f"{API_BASE}{path}?{q}"

    hdrs = {"User-Agent": "creto/1.0 (+https://github.com/devoid00/creto)"}
    if headers:
        hdrs.update(headers)

    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(full_url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            # Don't retry auth errors
            if e.code in (401, 403):
                raise
            # Retry rate limits / server errors
            if e.code in (429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep(1.0 * attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < max_retries:
                time.sleep(1.0 * attempt)
                continue
            raise


# ---------- YAML -> list ----------
def load_from_repo(repo_dir: Path):
    """Load bios + terms + ids from YAML."""
    cur = yaml.safe_load(open(repo_dir / "legislators-current.yaml", "rb"))
    social_map = {}
    try:
        social_map = {
            i["id"]["bioguide"]: i.get("social", {})
            for i in yaml.safe_load(open(repo_dir / "legislators-social-media.yaml", "rb"))
        }
    except FileNotFoundError:
        pass

    out = []
    for leg in cur:
        ids = leg.get("id", {})
        name = leg.get("name", {})
        terms = leg.get("terms", [])
        if not terms:
            continue
        last = terms[-1]
        bioguide = ids.get("bioguide")
        out.append(
            {
                "bioguide": bioguide,
                "govtrack": ids.get("govtrack"),
                "fec": ids.get("fec", []),
                "first": name.get("first"),
                "last": name.get("last"),
                "official_full": name.get("official_full")
                or f"{name.get('first','')} {name.get('last','')}".strip(),
                "party": last.get("party"),
                "state": last.get("state"),
                "district": last.get("district"),
                "chamber": last.get("type"),  # "rep" or "sen"
                "start": last.get("start"),
                "end": last.get("end"),
                "url": last.get("url"),
                "photo": f"https://theunitedstates.io/images/congress/450x550/{bioguide}.jpg"
                if bioguide
                else None,
                "social": social_map.get(bioguide, {}),
            }
        )
    return out


# ---------- Optional live filter via Congress.gov ----------
def current_bioguide_ids_from_congressgov():
    """Confirm who's currently seated via Congress.gov (needs API key)."""
    ids = set()
    offset, page_size = 0, 250
    while True:
        data = http_json("/member", {"limit": page_size, "offset": offset})
        members = data.get("members", [])
        if not members:
            break
        for m in members:
            bid = (m.get("bioguideId") or "").strip()
            if bid:
                ids.add(bid)
        offset += page_size
        time.sleep(0.15)  # be polite
    return ids


def main():
    root = Path(__file__).resolve().parents[1]
    web = root / "web"
    web.mkdir(parents=True, exist_ok=True)
    out_path = web / "members-current.json"

    # Pull upstream data
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        subprocess.check_call(["git", "clone", "--depth", "1", CONGRESS_REPO, str(td)])
        members = load_from_repo(td)

    # Optionally filter by who is currently in office via Congress.gov
    if API_KEY:
        try:
            current_ids = current_bioguide_ids_from_congressgov()
            members = [m for m in members if m.get("bioguide") in current_ids]
        except Exception as e:
            # Fail soft so the site still deploys
            print(f"WARN: Congress.gov check failed ({e}); keeping all current.yaml members.")

    members.sort(key=lambda m: (m["chamber"], m["state"], m.get("district") or 0, m["last"] or ""))

    out_path.write_text(json.dumps(members, indent=2))
    print(f"Wrote {out_path} with {len(members)} records.")


if __name__ == "__main__":
    main()
