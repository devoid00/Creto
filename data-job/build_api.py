#!/usr/bin/env python3
import json, time
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"
API = WEB / "api"

def load_json(p: Path, default):
    if not p.exists():
        return default
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_json(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(p)

def build():
    members = load_json(WEB / "members-current.json", [])
    promises = load_json(WEB / "promises.json", {})

    # normalize promise keys by bioguide
    prom = { (k or "").strip(): v for k, v in promises.items() }

    # --- /api/states/XX.json ---
    states = defaultdict(lambda: {"sen": [], "rep": []})
    for m in members:
        st = (m.get("state") or "").strip()
        ch = m.get("chamber")
        if not st or ch not in ("sen","rep"):
            continue
        mm = dict(m)
        mm["promises_count"] = len(prom.get(m.get("bioguide",""), []))
        states[st][ch].append(mm)

    # sort entries for stable output
    for st in states:
        for ch in ("sen","rep"):
            states[st][ch].sort(
                key=lambda x: (x["chamber"], x.get("district") or 0, (x.get("last") or "").lower())
            )

    # write per-state files + index
    index = []
    for st, groups in states.items():
        write_json(API / "states" / f"{st}.json", groups)
        index.append({
            "state": st,
            "senators": len(groups["sen"]),
            "representatives": len(groups["rep"])
        })
    index.sort(key=lambda i: i["state"])
    write_json(API / "states" / "index.json", index)

    # --- per-member detail + promises
    for m in members:
        bid = (m.get("bioguide") or "").strip()
        if not bid:
            continue
        detail = dict(m)
        detail["promises"] = prom.get(bid, [])
        write_json(API / "member" / f"{bid}.json", detail)
        write_json(API / "promises" / f"{bid}.json", prom.get(bid, []))

    # --- summary
    summary = {
        "generated_at": int(time.time()),
        "total": len(members),
        "states": len(states),
        "with_promises": sum(1 for m in members if len(prom.get(m.get("bioguide",""), [])) > 0)
    }
    write_json(API / "members" / "summary.json", summary)

    print(f"Built static API: {len(states)} states, {len(members)} members")

if __name__ == "__main__":
    build()
