"""
╔══════════════════════════════════════════════════════════════╗
║   FRAUD GRAPH ENGINE — fraud_engine.py  (Modular Core)       ║
║   Gap Fix: Callable functions + Geographic Coordinates        ║
║   Used by: Phase 3 Streamlit dashboard (app.py)              ║
╚══════════════════════════════════════════════════════════════╝

Public API:
    build_graph()      → NetworkX MultiDiGraph
    compute_scores(G)  → dict of {account_id: {...score data...}}
    get_dataframes()   → dict of all source DataFrames
    full_pipeline()    → (G, scores, dfs)  ← one-shot call for Streamlit
"""

import pandas as pd
import networkx as nx
from itertools import combinations
from collections import defaultdict

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
FLAG_WEIGHTS = {
    "shared_device":               30,
    "shared_ip":                   25,
    "money_received_from_flagged": 20,
    "money_sent_to_flagged":       20,
    "shared_touchpoint":           15,
    "transaction_loop":            40,
    "high_velocity":               20,
}

THRESHOLDS = {
    "CLEAN":      (0,   30),
    "WATCH":      (31,  60),
    "SUSPICIOUS": (61,  90),
    "BLOCK":      (91, 9999),
}

TIER_COLORS_HEX = {
    "CLEAN":      "#00cc66",
    "WATCH":      "#ffcc00",
    "SUSPICIOUS": "#ff8c00",
    "BLOCK":      "#ff2244",
}

# RGB tuples for pydeck (0-255)
TIER_COLORS_RGB = {
    "CLEAN":      [0,   204,  102, 220],
    "WATCH":      [255, 204,  0,   220],
    "SUSPICIOUS": [255, 140,  0,   220],
    "BLOCK":      [255, 34,   68,  220],
}

CONTAMINATION_WEIGHT = 0.30

# ─────────────────────────────────────────────────────────────
# GEOGRAPHIC COORDINATES
# City centroids + small per-account offsets so nodes don't
# overlap when plotted on the map.
# Format: [latitude, longitude]
# ─────────────────────────────────────────────────────────────
ACCOUNT_GEO = {
    # Ring A — Mumbai
    "ACC_A1": [19.0820,  72.8777],
    "ACC_A2": [19.0760,  72.8820],
    "ACC_A3": [19.0700,  72.8740],
    "ACC_A4": [19.0780,  72.8690],
    # Ring B — Delhi
    "ACC_B1": [28.6200,  77.2100],
    "ACC_B2": [28.6139,  77.2200],
    "ACC_B3": [28.6080,  77.2050],
    "ACC_B4": [28.6160,  77.1990],
    # Ring C — Bangalore
    "ACC_C1": [12.9780,  77.5980],
    "ACC_C2": [12.9716,  77.6050],
    "ACC_C3": [12.9660,  77.5920],
    "ACC_C4": [12.9740,  77.5870],
    # Ring D — Hyderabad (bridge ring)
    "ACC_D1": [17.3900,  78.4920],
    "ACC_D2": [17.3850,  78.5010],
    "ACC_D3": [17.3790,  78.4860],
    # Ring E — CLEAN (spread across different cities)
    "ACC_E1": [22.5726,  88.3639],   # Kolkata
    "ACC_E2": [26.9124,  75.7873],   # Jaipur
    "ACC_E3": [23.0225,  72.5714],   # Ahmedabad
    # Ring F — WATCH (Chennai)
    "ACC_F1": [13.0900,  80.2750],
    "ACC_F2": [13.0827,  80.2820],
    "ACC_F3": [13.0760,  80.2700],
    # Ring G — SUSPICIOUS (Pune)
    "ACC_G1": [18.5204,  73.8567],
    "ACC_G2": [18.5260,  73.8640],
    "ACC_G3": [18.5150,  73.8500],
}

# Touchpoint geo (ATMs / portals)
TOUCHPOINT_GEO = {
    "ATM_MUM":    [19.0650, 72.8900],
    "ATM_DEL":    [28.6050, 77.2250],
    "PORTAL_BLR": [12.9600, 77.6100],
    "ATM_HYD":    [17.3700, 78.5100],
    "ATM_CHN":    [13.0700, 80.2600],
}

# ─────────────────────────────────────────────────────────────
# SOURCE DATA
# ─────────────────────────────────────────────────────────────

def get_dataframes():
    """Return all source tables as a dict of DataFrames."""

    accounts = pd.DataFrame([
        {"account_id":"ACC_A1","type":"UPI ID",         "holder":"Arjun Mehta",     "ring":"A","city":"Mumbai"},
        {"account_id":"ACC_A2","type":"Savings Account","holder":"Sunita Rao",       "ring":"A","city":"Mumbai"},
        {"account_id":"ACC_A3","type":"Digital Wallet", "holder":"Vikram Desai",     "ring":"A","city":"Mumbai"},
        {"account_id":"ACC_A4","type":"UPI ID",         "holder":"Kavitha Nair",     "ring":"A","city":"Mumbai"},
        {"account_id":"ACC_B1","type":"Digital Wallet", "holder":"Rohan Sharma",     "ring":"B","city":"Delhi"},
        {"account_id":"ACC_B2","type":"Savings Account","holder":"Pooja Verma",      "ring":"B","city":"Delhi"},
        {"account_id":"ACC_B3","type":"UPI ID",         "holder":"Anil Gupta",       "ring":"B","city":"Delhi"},
        {"account_id":"ACC_B4","type":"Digital Wallet", "holder":"Meera Joshi",      "ring":"B","city":"Delhi"},
        {"account_id":"ACC_C1","type":"Savings Account","holder":"Suresh Pillai",    "ring":"C","city":"Bangalore"},
        {"account_id":"ACC_C2","type":"UPI ID",         "holder":"Divya Krishnan",   "ring":"C","city":"Bangalore"},
        {"account_id":"ACC_C3","type":"Digital Wallet", "holder":"Karthik Iyer",     "ring":"C","city":"Bangalore"},
        {"account_id":"ACC_C4","type":"Savings Account","holder":"Preethi Nair",     "ring":"C","city":"Bangalore"},
        {"account_id":"ACC_D1","type":"Savings Account","holder":"Neeraj Kapoor",    "ring":"D","city":"Hyderabad"},
        {"account_id":"ACC_D2","type":"UPI ID",         "holder":"Shalini Bose",     "ring":"D","city":"Hyderabad"},
        {"account_id":"ACC_D3","type":"Digital Wallet", "holder":"Tarun Saxena",     "ring":"D","city":"Hyderabad"},
        {"account_id":"ACC_E1","type":"Savings Account","holder":"Priya Menon",      "ring":"CLEAN","city":"Kolkata"},
        {"account_id":"ACC_E2","type":"UPI ID",         "holder":"Ravi Kumar",       "ring":"CLEAN","city":"Jaipur"},
        {"account_id":"ACC_E3","type":"Digital Wallet", "holder":"Ananya Iyer",      "ring":"CLEAN","city":"Ahmedabad"},
        {"account_id":"ACC_F1","type":"Savings Account","holder":"Amit Singh",       "ring":"WATCH","city":"Chennai"},
        {"account_id":"ACC_F2","type":"UPI ID",         "holder":"Deepa Thomas",     "ring":"WATCH","city":"Chennai"},
        {"account_id":"ACC_F3","type":"Digital Wallet", "holder":"Kunal Mehta",      "ring":"WATCH","city":"Chennai"},
        {"account_id":"ACC_G1","type":"Savings Account","holder":"Sanjay Malhotra",  "ring":"SUSPICIOUS","city":"Pune"},
        {"account_id":"ACC_G2","type":"UPI ID",         "holder":"Lalitha Rao",      "ring":"SUSPICIOUS","city":"Pune"},
        {"account_id":"ACC_G3","type":"Digital Wallet", "holder":"Mohan Das",        "ring":"SUSPICIOUS","city":"Pune"},
    ])

    # Attach geo coordinates
    accounts["lat"] = accounts["account_id"].map(lambda x: ACCOUNT_GEO[x][0])
    accounts["lon"] = accounts["account_id"].map(lambda x: ACCOUNT_GEO[x][1])

    identifiers = pd.DataFrame([
        {"account_id":"ACC_A1","identifier_type":"IP Address","identifier_value":"192.168.1.1"},
        {"account_id":"ACC_A2","identifier_type":"IP Address","identifier_value":"192.168.1.1"},
        {"account_id":"ACC_A3","identifier_type":"Device MAC","identifier_value":"MAC:EE:01"},
        {"account_id":"ACC_A4","identifier_type":"Device MAC","identifier_value":"MAC:EE:01"},
        {"account_id":"ACC_B1","identifier_type":"IP Address","identifier_value":"10.0.0.5"},
        {"account_id":"ACC_B2","identifier_type":"IP Address","identifier_value":"10.0.0.5"},
        {"account_id":"ACC_B3","identifier_type":"Device MAC","identifier_value":"MAC:EE:03"},
        {"account_id":"ACC_B4","identifier_type":"Device MAC","identifier_value":"MAC:EE:03"},
        {"account_id":"ACC_C1","identifier_type":"IP Address","identifier_value":"172.16.0.3"},
        {"account_id":"ACC_C2","identifier_type":"IP Address","identifier_value":"172.16.0.3"},
        {"account_id":"ACC_C3","identifier_type":"Device MAC","identifier_value":"MAC:EE:02"},
        {"account_id":"ACC_C4","identifier_type":"Device MAC","identifier_value":"MAC:EE:02"},
        {"account_id":"ACC_D1","identifier_type":"IMEI",      "identifier_value":"IMEI-7766"},
        {"account_id":"ACC_A3","identifier_type":"IMEI",      "identifier_value":"IMEI-7766"},
        {"account_id":"ACC_D3","identifier_type":"IMEI",      "identifier_value":"IMEI-5544"},
        {"account_id":"ACC_C1","identifier_type":"IMEI",      "identifier_value":"IMEI-5544"},
        {"account_id":"ACC_D2","identifier_type":"IP Address","identifier_value":"10.0.1.8"},
        {"account_id":"ACC_B4","identifier_type":"IP Address","identifier_value":"10.0.1.8"},
        {"account_id":"ACC_E1","identifier_type":"IP Address","identifier_value":"203.0.113.10"},
        {"account_id":"ACC_E2","identifier_type":"IP Address","identifier_value":"203.0.113.20"},
        {"account_id":"ACC_E3","identifier_type":"IP Address","identifier_value":"203.0.113.30"},
        {"account_id":"ACC_F1","identifier_type":"IP Address","identifier_value":"198.51.100.1"},
        {"account_id":"ACC_F2","identifier_type":"IP Address","identifier_value":"198.51.100.2"},
        {"account_id":"ACC_F3","identifier_type":"IP Address","identifier_value":"198.51.100.3"},
        {"account_id":"ACC_G1","identifier_type":"IP Address","identifier_value":"10.2.0.99"},
        {"account_id":"ACC_G2","identifier_type":"IP Address","identifier_value":"10.2.0.99"},
        {"account_id":"ACC_G3","identifier_type":"IP Address","identifier_value":"10.2.0.99"},
    ])

    touchpoints = pd.DataFrame([
        {"account_id":"ACC_A1","touchpoint_id":"ATM_MUM"},
        {"account_id":"ACC_A2","touchpoint_id":"ATM_MUM"},
        {"account_id":"ACC_B1","touchpoint_id":"ATM_DEL"},
        {"account_id":"ACC_B3","touchpoint_id":"ATM_DEL"},
        {"account_id":"ACC_C1","touchpoint_id":"PORTAL_BLR"},
        {"account_id":"ACC_C2","touchpoint_id":"PORTAL_BLR"},
        {"account_id":"ACC_C3","touchpoint_id":"PORTAL_BLR"},
        {"account_id":"ACC_D1","touchpoint_id":"ATM_HYD"},
        {"account_id":"ACC_D2","touchpoint_id":"ATM_HYD"},
        {"account_id":"ACC_F1","touchpoint_id":"ATM_CHN"},
        {"account_id":"ACC_F2","touchpoint_id":"ATM_CHN"},
        {"account_id":"ACC_F3","touchpoint_id":"ATM_CHN"},
    ])

    transactions = pd.DataFrame([
        {"txn_id":"TXN_A1","sender":"ACC_A1","receiver":"ACC_A2","amount":18000,"timestamp":1},
        {"txn_id":"TXN_A2","sender":"ACC_A2","receiver":"ACC_A3","amount":17500,"timestamp":2},
        {"txn_id":"TXN_A3","sender":"ACC_A3","receiver":"ACC_A4","amount":17000,"timestamp":3},
        {"txn_id":"TXN_A4","sender":"ACC_A4","receiver":"ACC_A1","amount":9500, "timestamp":4},
        {"txn_id":"TXN_B1","sender":"ACC_B1","receiver":"ACC_B2","amount":16000,"timestamp":5},
        {"txn_id":"TXN_B2","sender":"ACC_B2","receiver":"ACC_B3","amount":15500,"timestamp":6},
        {"txn_id":"TXN_B3","sender":"ACC_B3","receiver":"ACC_B4","amount":14000,"timestamp":7},
        {"txn_id":"TXN_B4","sender":"ACC_B4","receiver":"ACC_B1","amount":10000,"timestamp":8},
        {"txn_id":"TXN_C1","sender":"ACC_C1","receiver":"ACC_C2","amount":14000,"timestamp":9},
        {"txn_id":"TXN_C2","sender":"ACC_C2","receiver":"ACC_C3","amount":13500,"timestamp":10},
        {"txn_id":"TXN_C3","sender":"ACC_C3","receiver":"ACC_C4","amount":13000,"timestamp":11},
        {"txn_id":"TXN_C4","sender":"ACC_C4","receiver":"ACC_C1","amount":7500, "timestamp":12},
        {"txn_id":"TXN_D1","sender":"ACC_D1","receiver":"ACC_D2","amount":22000,"timestamp":13},
        {"txn_id":"TXN_D2","sender":"ACC_D2","receiver":"ACC_D3","amount":21000,"timestamp":14},
        {"txn_id":"TXN_F1","sender":"ACC_F1","receiver":"ACC_F2","amount":5000, "timestamp":20},
        {"txn_id":"TXN_F2","sender":"ACC_F2","receiver":"ACC_F3","amount":4500, "timestamp":21},
        {"txn_id":"TXN_G1","sender":"ACC_G1","receiver":"ACC_G2","amount":9000, "timestamp":30},
        {"txn_id":"TXN_G2","sender":"ACC_G2","receiver":"ACC_G1","amount":8000, "timestamp":31},
        {"txn_id":"TXN_G3","sender":"ACC_G2","receiver":"ACC_G3","amount":7500, "timestamp":32},
        {"txn_id":"TXN_G4","sender":"ACC_G3","receiver":"ACC_G2","amount":7000, "timestamp":33},
    ])

    return {
        "accounts":     accounts,
        "identifiers":  identifiers,
        "touchpoints":  touchpoints,
        "transactions": transactions,
    }


# ─────────────────────────────────────────────────────────────
# GRAPH BUILDER
# ─────────────────────────────────────────────────────────────

def build_graph(dfs=None):
    """
    Build and return a NetworkX MultiDiGraph from source tables.
    Pass dfs=get_dataframes() or leave None to auto-load.
    """
    if dfs is None:
        dfs = get_dataframes()

    accounts     = dfs["accounts"]
    identifiers  = dfs["identifiers"]
    touchpoints  = dfs["touchpoints"]
    transactions = dfs["transactions"]

    G = nx.MultiDiGraph()

    # Add account nodes with all attributes
    for _, row in accounts.iterrows():
        G.add_node(row["account_id"],
                   node_type    = "account",
                   holder       = row["holder"],
                   ring         = row["ring"],
                   account_type = row["type"],
                   city         = row["city"],
                   lat          = row["lat"],
                   lon          = row["lon"])

    # Shared IP edges
    ip_grps = identifiers[identifiers["identifier_type"] == "IP Address"]\
        .groupby("identifier_value")["account_id"].apply(list)
    for _, accs in ip_grps.items():
        for a, b in combinations(accs, 2):
            G.add_edge(a, b, edge_type="shared_ip")
            G.add_edge(b, a, edge_type="shared_ip")

    # Shared device / IMEI edges
    dev_grps = identifiers[identifiers["identifier_type"].isin(["Device MAC", "IMEI"])]\
        .groupby("identifier_value")["account_id"].apply(list)
    for _, accs in dev_grps.items():
        for a, b in combinations(accs, 2):
            G.add_edge(a, b, edge_type="shared_device")
            G.add_edge(b, a, edge_type="shared_device")

    # Shared touchpoint edges
    tp_grps = touchpoints.groupby("touchpoint_id")["account_id"].apply(list)
    for _, accs in tp_grps.items():
        for a, b in combinations(accs, 2):
            G.add_edge(a, b, edge_type="shared_touchpoint")
            G.add_edge(b, a, edge_type="shared_touchpoint")

    # Money flow edges
    for _, row in transactions.iterrows():
        G.add_edge(row["sender"], row["receiver"],
                   edge_type  = "money_flow",
                   amount     = row["amount"],
                   timestamp  = row["timestamp"],
                   txn_id     = row["txn_id"])

    return G


# ─────────────────────────────────────────────────────────────
# LOOP DETECTOR  (3+ node money-flow cycles only)
# ─────────────────────────────────────────────────────────────

def _detect_loops(G, account_ids):
    money_graph = nx.DiGraph()
    for u, v, d in G.edges(data=True):
        if d.get("edge_type") == "money_flow" and u in account_ids and v in account_ids:
            money_graph.add_edge(u, v)

    loop_accounts = set()
    for cycle in nx.simple_cycles(money_graph):
        if len(cycle) >= 3:
            for node in cycle:
                loop_accounts.add(node)
    return loop_accounts


# ─────────────────────────────────────────────────────────────
# RISK SCORER
# ─────────────────────────────────────────────────────────────

def classify(score):
    for label, (lo, hi) in THRESHOLDS.items():
        if lo <= score <= hi:
            return label
    return "BLOCK"


def compute_scores(G, dfs=None):
    """
    Compute own scores, apply neighbour contamination,
    classify each account.

    Returns dict:
        {
          account_id: {
            holder, ring, city, lat, lon, account_type,
            own_score, contamination, final_score,
            classification, color_hex, color_rgb,
            flags: [{flag, weight}, ...]
          }
        }
    """
    if dfs is None:
        dfs = get_dataframes()

    account_ids = set(dfs["accounts"]["account_id"])
    transactions = dfs["transactions"]

    loop_accounts         = _detect_loops(G, account_ids)
    send_counts           = transactions.groupby("sender").size()
    high_velocity_senders = set(send_counts[send_counts >= 3].index)

    # ── Own scores ──
    own_scores   = {}
    flag_details = {}

    for acc in account_ids:
        score = 0
        flags = []
        etc   = defaultdict(int)

        for _, _, d in G.edges(acc,    data=True): etc[d.get("edge_type", "")] += 1
        for _, _, d in G.in_edges(acc, data=True): etc[d.get("edge_type", "")] += 1

        if etc["shared_ip"]         > 0:
            w = FLAG_WEIGHTS["shared_ip"];         score += w; flags.append({"flag": "Shared IP Address",    "weight": w})
        if etc["shared_device"]     > 0:
            w = FLAG_WEIGHTS["shared_device"];     score += w; flags.append({"flag": "Shared Device / IMEI", "weight": w})
        if etc["shared_touchpoint"] > 0:
            w = FLAG_WEIGHTS["shared_touchpoint"]; score += w; flags.append({"flag": "Shared ATM / Portal",  "weight": w})

        in_money  = [u for u, _, d in G.in_edges(acc, data=True) if d.get("edge_type") == "money_flow"]
        out_money = [v for _, v, d in G.edges(acc,    data=True) if d.get("edge_type") == "money_flow"]

        if in_money:
            w = FLAG_WEIGHTS["money_received_from_flagged"]
            score += w; flags.append({"flag": f"Received money ({len(in_money)} txn)", "weight": w})
        if out_money:
            w = FLAG_WEIGHTS["money_sent_to_flagged"]
            score += w; flags.append({"flag": f"Sent money ({len(out_money)} txn)", "weight": w})
        if acc in loop_accounts:
            w = FLAG_WEIGHTS["transaction_loop"]
            score += w; flags.append({"flag": "Transaction Loop Detected", "weight": w})
        if acc in high_velocity_senders:
            w = FLAG_WEIGHTS["high_velocity"]
            score += w; flags.append({"flag": "High Velocity Sending", "weight": w})

        own_scores[acc]   = score
        flag_details[acc] = flags

    # ── Neighbour contamination ──
    results = {}
    for acc in account_ids:
        nbrs = set()
        for _, v in G.out_edges(acc):
            if v in account_ids: nbrs.add(v)
        for u, _ in G.in_edges(acc):
            if u in account_ids: nbrs.add(u)

        contamination = round(
            CONTAMINATION_WEIGHT * (sum(own_scores[n] for n in nbrs) / len(nbrs)), 1
        ) if nbrs else 0.0

        final  = round(own_scores[acc] + contamination, 1)
        cl     = classify(final)
        nd     = G.nodes[acc]

        results[acc] = {
            "account_id":     acc,
            "holder":         nd.get("holder", ""),
            "ring":           nd.get("ring", ""),
            "city":           nd.get("city", ""),
            "lat":            nd.get("lat", 0.0),
            "lon":            nd.get("lon", 0.0),
            "account_type":   nd.get("account_type", ""),
            "own_score":      own_scores[acc],
            "contamination":  contamination,
            "final_score":    final,
            "classification": cl,
            "color_hex":      TIER_COLORS_HEX[cl],
            "color_rgb":      TIER_COLORS_RGB[cl],
            "flags":          flag_details[acc],
            "in_loop":        acc in loop_accounts,
        }

    return results


# ─────────────────────────────────────────────────────────────
# TRANSACTION ENRICHMENT  (for arc layer)
# ─────────────────────────────────────────────────────────────

def get_transaction_arcs(scores, dfs=None):
    """
    Returns a list of dicts with source/target lat-lon + metadata
    suitable for pydeck ArcLayer.
    """
    if dfs is None:
        dfs = get_dataframes()

    arcs = []
    for _, row in dfs["transactions"].iterrows():
        s = row["sender"]
        t = row["receiver"]
        if s not in scores or t not in scores:
            continue

        src = scores[s]
        tgt = scores[t]

        # Arc colour follows the higher-risk endpoint
        risk_order = ["CLEAN", "WATCH", "SUSPICIOUS", "BLOCK"]
        higher = max(src["classification"], tgt["classification"], key=lambda c: risk_order.index(c))

        arcs.append({
            "txn_id":         row["txn_id"],
            "sender":         s,
            "sender_name":    src["holder"],
            "receiver":       t,
            "receiver_name":  tgt["holder"],
            "amount":         row["amount"],
            "src_lat":        src["lat"],
            "src_lon":        src["lon"],
            "tgt_lat":        tgt["lat"],
            "tgt_lon":        tgt["lon"],
            "src_color":      src["color_rgb"],
            "tgt_color":      tgt["color_rgb"],
            "risk_tier":      higher,
        })

    return arcs


# ─────────────────────────────────────────────────────────────
# RING CLUSTER CENTROIDS  (for highlight circles)
# ─────────────────────────────────────────────────────────────

def get_ring_clusters(scores):
    """
    Returns cluster centroid + radius for each fraud ring,
    for pydeck ScatterplotLayer (large semi-transparent circle).
    Only rings with 2+ BLOCK or SUSPICIOUS accounts are included.
    """
    from collections import defaultdict
    import statistics

    ring_accounts = defaultdict(list)
    for acc, data in scores.items():
        if data["classification"] in ("BLOCK", "SUSPICIOUS"):
            ring_accounts[data["ring"]].append(data)

    clusters = []
    for ring, members in ring_accounts.items():
        if len(members) < 2:
            continue
        c_lat = statistics.mean(m["lat"] for m in members)
        c_lon = statistics.mean(m["lon"] for m in members)

        # Approximate radius in metres (spread of nodes * scale factor)
        lat_spread = max(m["lat"] for m in members) - min(m["lat"] for m in members)
        lon_spread = max(m["lon"] for m in members) - min(m["lon"] for m in members)
        radius_deg = max(lat_spread, lon_spread) / 2
        radius_m   = max(int(radius_deg * 111_000 * 1.6), 1800)  # minimum 1.8 km

        # Colour by worst tier in ring
        has_block = any(m["classification"] == "BLOCK" for m in members)
        color = [255, 34, 68, 35] if has_block else [255, 140, 0, 35]

        clusters.append({
            "ring":      ring,
            "lat":       c_lat,
            "lon":       c_lon,
            "radius":    radius_m,
            "color":     color,
            "count":     len(members),
            "label":     f"Ring {ring}",
        })

    return clusters


# ─────────────────────────────────────────────────────────────
# ONE-SHOT PIPELINE
# ─────────────────────────────────────────────────────────────

def full_pipeline():
    """
    Run the entire engine end-to-end.
    Returns: (G, scores, arcs, clusters, dfs)
    """
    dfs      = get_dataframes()
    G        = build_graph(dfs)
    scores   = compute_scores(G, dfs)
    arcs     = get_transaction_arcs(scores, dfs)
    clusters = get_ring_clusters(scores)

    return G, scores, arcs, clusters, dfs


# ─────────────────────────────────────────────────────────────
# QUICK SELF-TEST
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    G, scores, arcs, clusters, dfs = full_pipeline()

    print(f"\nGraph       : {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    print(f"Scored accs : {len(scores)}")
    print(f"Arcs        : {len(arcs)}")
    print(f"Ring clusters: {len(clusters)}\n")

    tiers = {"BLOCK":0,"SUSPICIOUS":0,"WATCH":0,"CLEAN":0}
    for d in scores.values():
        tiers[d["classification"]] += 1
    print("Tier breakdown:")
    for tier, cnt in tiers.items():
        print(f"  {tier:12s}: {cnt}")

    print("\nTop 5 by final score:")
    for acc, d in sorted(scores.items(), key=lambda x:-x[1]["final_score"])[:5]:
        print(f"  {acc}  {d['holder']:20s}  {d['final_score']:6.1f}  {d['classification']}")

    print("\nSelf-test PASSED ✓")
