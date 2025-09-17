import json, os, subprocess, tempfile, time, urllib.request
from pathlib import Path

try:
    import yaml
except ImportError:
    raise SystemExit("Please `pip install pyyaml` first.")

CONGRESS_REPO = "https://github.com/unitedstates/congress-legislators.git"
API_KEY = os.environ.get("CONGRESS_GOV_API_KEY")
API_BASE = "https://api.congress.gov/v3"

def http_json(url, params=None, headers=None):
    if params:
        q = "&".join(f"{k}={v}" for k,v in params.items())
        url = f"{url}?{q}"
    hdrs = headers or {}
    if API_KEY:
        hdrs["X-API-Key"] = API_KEY
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req) as r:
        return json.load(r)

def load_from_repo(repo_dir: Path):
    """Load bios + terms + ids from YAML."""
    cur = yaml.safe_load(open(repo_dir/"legislators-current.yaml", "rb"))
    social_map = {}
    try:
        social_map = {i["id"]["bioguide"]: i.get("social", {}) 
                      for i in yaml.safe_load(open(repo_dir/"legislators-social-media.yaml","rb"))}
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
        out.append({
            "bioguide": bioguide,
            "govtrack": ids.get("govtrack"),
            "fec": ids.get("fec", []),
            "first": name.get("first"),
            "last": name.get("last"),
            "official_full": name.get("official_full") or f"{name.get('first','')} {name.get('last','')}".strip(),
            "party": last.get("party"),
            "state": last.get("state"),
            "district": last.get("district"),
            "chamber": last.get("type"),  # "rep" or "sen"
            "start": last.get("start"),
            "end": last.get("end"),
            "url": last.get("url"),
            "photo": f"https://theunitedstates.io/images/congress/450x550/{bioguide}.jpg" if bioguide else None,
            "social": social_map.get(bioguide, {})
        })
    return out

def current_bioguide_ids_from_congressgov():
    """Optional: confirm who's currently seated via Congress.gov (needs API key)."""
    ids = set()
    offset, page_size = 0, 250
    while True:
        data = http_json(f"{API_BASE}/member", {"format": "json", "limit": page_size, "offset": offset})
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
    web = root/"web"
    web.mkdir(parents=True, exist_ok=True)
    out_path = web/"members-current.json"

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        subprocess.check_call(["git","clone","--depth","1",CONGRESS_REPO,str(td)])
        members = load_from_repo(td)

    if API_KEY:
        current_ids = current_bioguide_ids_from_congressgov()
        members = [m for m in members if m.get("bioguide") in current_ids]

    members.sort(key=lambda m: (m["chamber"], m["state"], m.get("district") or 0, m["last"] or ""))

    out_path.write_text(json.dumps(members, indent=2))
    print(f"Wrote {out_path} with {len(members)} records.")

if __name__ == "__main__":
    main()
