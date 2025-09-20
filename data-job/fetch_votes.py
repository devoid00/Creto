# data-job/fetch_votes.py
import os
import json
import time
import math
import concurrent.futures as futures
from pathlib import Path
from datetime import datetime
from dateutil import tz
import requests
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
WEB_DATA = ROOT / "web" / "data"
WEB_DATA.mkdir(parents=True, exist_ok=True)

SESS = requests.Session()
SESS.headers.update({"User-Agent": "CretoVotes/1.0 (+github.com/yourhandle)"})

# ---------------------------
# Utilities
# ---------------------------
def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":" ))
    tmp.replace(path)

def get_text(el, tag, default=""):
    x = el.find(tag)
    return x.text.strip() if x is not None and x.text else default

def parse_dt(s):
    # Senate uses e.g. "January 09, 2025"
    try:
        return datetime.strptime(s.strip(), "%B %d, %Y").date().isoformat()
    except Exception:
        return s

# ---------------------------
# Senate (authoritative XML)
# ---------------------------
SENATE_MENU = "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml"
SENATE_VOTE_XML = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session:01d}/vote_{congress}_{session:01d}_{roll:05d}.xml"

def fetch_senate_menu(congress: int, session: int):
    url = SENATE_MENU.format(congress=congress, session=session)
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return ET.fromstring(r.content)

def normalize_senate_menu(root, congress, session):
    # <vote_summary> nodes
    rows = []
    for vs in root.findall(".//vote_summary"):
        roll = int(get_text(vs, "roll_call_vote_number", "0"))
        question = get_text(vs, "vote_question_text")
        desc = get_text(vs, "vote_title")
        issue = get_text(vs, "issue")
        result = get_text(vs, "vote_result_text")
        date = parse_dt(get_text(vs, "vote_date"))
        bill = issue if issue else ""
        rows.append({
            "congress": congress,
            "chamber": "senate",
            "session": session,
            "rollcall": roll,
            "date": date,
            "result": result,
            "question": question,
            "bill_number": bill,
            "title": desc
        })
    rows.sort(key=lambda x: x["rollcall"])
    return rows

def fetch_senate_roll(congress: int, session: int, roll: int):
    url = SENATE_VOTE_XML.format(congress=congress, session=session, roll=roll)
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return ET.fromstring(r.content)

def normalize_senate_roll(root, congress, session, roll):
    meta = root.find(".//vote_metadata")
    question = get_text(meta, "vote_question_text")
    title = get_text(meta, "vote_title")
    result = get_text(meta, "vote_result_text")
    date = parse_dt(get_text(meta, "vote_date"))
    issue = get_text(meta, "issue")
    tot = root.find(".//count")
    totals = {
        "yea": int(get_text(tot, "yeas", "0")),
        "nay": int(get_text(tot, "nays", "0")),
        "present": int(get_text(tot, "present", "0")),
        "nv": int(get_text(tot, "not_voting", "0")),
    }
    members = []
    for m in root.findall(".//member"):
        members.append({
            "bioguide_id": get_text(m, "member_full").split()[-1],  # unreliable; Senate XML lacks bioguide
            "last_name": get_text(m, "last_name"),
            "first_name": get_text(m, "first_name"),
            "party": get_text(m, "party"),
            "state": get_text(m, "state"),
            "vote": get_text(m, "vote_cast"),
            "lis_member_id": get_text(m, "lis_member_id"),
        })
    return {
        "key": f"{congress}-senate-{session}-{roll}",
        "congress": congress,
        "chamber": "senate",
        "session": session,
        "rollcall": roll,
        "date": date,
        "result": result,
        "question": question,
        "bill_number": issue,
        "title": title,
        "totals": totals,
        "members": members,
        "source": {
            "menu": SENATE_MENU.format(congress=congress, session=session),
            "vote_xml": SENATE_VOTE_XML.format(congress=congress, session=session, roll=roll),
        }
    }

# ---------------------------
# House (best-effort XML enumerator)
# NOTE: The Clerk provides per-vote XML; the index endpoints vary by year/session.
# We attempt roll numbers until 404 streak; adjust as needed.
# ---------------------------
HOUSE_VOTE_XML = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"

def year_for(congress: int, session: int):
    # Congress runs 2 years; 119th spans 2025 (1st session) and 2026 (2nd).
    # This simple mapping works going forward; adjust if pulling historical further back.
    base = 1789  # 1st Congress start
    # Each Congress is 2 years; year start approx:
    # 1st: 1789, 2nd: 1791, ... -> year = 1789 + (congress-1)*2 + (session-1)
    return 1789 + (congress - 1) * 2 + (session - 1)

def fetch_house_roll(congress: int, session: int, roll: int):
    year = year_for(congress, session)
    url = HOUSE_VOTE_XML.format(year=year, roll=roll)
    r = SESS.get(url, timeout=20)
    if r.status_code == 404:
        return None, url
    r.raise_for_status()
    return ET.fromstring(r.content), url

def normalize_house_roll(root, congress, session, roll, vote_url):
    # House EVS XML uses different tags
    # <rollcall-vote><vote-metadata>...</vote-metadata><recorded-vote>...</recorded-vote>...</rollcall-vote>
    meta = root.find(".//vote-metadata")
    question = get_text(meta, "vote-question")
    result = get_text(meta, "vote-result")
    date = get_text(meta, "vote-date")
    billnum = get_text(meta, "legis-num")
    title = get_text(meta, "vote-desc") or get_text(meta, "vote-question")
    # Totals
    totals = {
        "yea": int(get_text(meta, "yea-total", "0")),
        "nay": int(get_text(meta, "nay-total", "0")),
        "present": int(get_text(meta, "present-total", "0")),
        "nv": int(get_text(meta, "not-voting-total", "0")),
    }
    members = []
    for rv in root.findall(".//recorded-vote"):
        who = rv.find("legislator")
        members.append({
            "bioguide_id": who.attrib.get("name-id", ""),
            "last_name": who.attrib.get("unaccented-name", "").split(",")[0].strip(),
            "first_name": who.attrib.get("first", ""),
            "party": who.attrib.get("party", ""),
            "state": who.attrib.get("state", ""),
            "district": who.attrib.get("district", ""),
            "vote": get_text(rv, "vote")
        })
    return {
        "key": f"{congress}-house-{session}-{roll}",
        "congress": congress,
        "chamber": "house",
        "session": session,
        "rollcall": roll,
        "date": date,
        "result": result,
        "question": question,
        "bill_number": billnum,
        "title": title,
        "totals": totals,
        "members": members,
        "source": {"vote_xml": vote_url}
    }

# ---------------------------
# Driver
# ---------------------------
def collect_senate(congress: int, session: int):
    root = fetch_senate_menu(congress, session)
    rows = normalize_senate_menu(root, congress, session)
    # Save list file
    save_json(WEB_DATA / f"votes-{congress}-senate-{session}.json", rows)
    # Fetch per-vote detail (parallel)
    def work(row):
        try:
            x = fetch_senate_roll(congress, session, row["rollcall"])
            data = normalize_senate_roll(x, congress, session, row["rollcall"])
            save_json(WEB_DATA / f"vote-{congress}-senate-{session}-{row['rollcall']}.json", data)
            return True
        except Exception:
            return False
    with futures.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(work, rows))
    return {"congress": congress, "chamber": "senate", "session": session, "count": len(rows)}

def collect_house(congress: int, session: int, max_probe=1200, stop_gap=30):
    # Probe roll numbers until a streak of 404s suggests the end.
    found = []
    misses = 0
    for roll in range(1, max_probe+1):
        try:
            root, url = fetch_house_roll(congress, session, roll)
            if root is None:
                misses += 1
                if misses >= stop_gap and len(found) > 0:
                    break
                continue
            misses = 0
            data = normalize_house_roll(root, congress, session, roll, url)
            found.append({
                "congress": congress,
                "chamber": "house",
                "session": session,
                "rollcall": roll,
                "date": data["date"],
                "result": data["result"],
                "question": data["question"],
                "bill_number": data["bill_number"],
                "title": data["title"],
            })
            save_json(WEB_DATA / f"vote-{congress}-house-{session}-{roll}.json", data)
            # be kind to servers
            if roll % 20 == 0:
                time.sleep(0.5)
        except requests.HTTPError as e:
            # treat like miss
            misses += 1
            if misses >= stop_gap and len(found) > 0:
                break
        except Exception:
            # skip bad parse, continue
            continue
    found.sort(key=lambda x: x["rollcall"])
    save_json(WEB_DATA / f"votes-{congress}-house-{session}.json", found)
    return {"congress": congress, "chamber": "house", "session": session, "count": len(found)}

def build_index(entries):
    save_json(WEB_DATA / "votes-index.json", {
        "generated_at": datetime.now(tz.tzlocal()).isoformat(),
        "datasets": entries
    })

def main():
    # Target the current Congress/session first; expand as you like.
    targets = [
        (119, "house", 1),
        (119, "senate", 1),
    ]
    entries = []
    for congress, chamber, session in targets:
        if chamber == "senate":
            entries.append(collect_senate(congress, session))
        else:
            entries.append(collect_house(congress, session))
    build_index(entries)
    print("Done:", entries)

if __name__ == "__main__":
    main()
