"""
╔══════════════════════════════════════════════════════════╗
║   FRAUD GRAPH ENGINE — PHASE 2: RISK SCORER  (v2)        ║
║   All Four Risk Tiers: CLEAN / WATCH / SUSPICIOUS / BLOCK ║
╚══════════════════════════════════════════════════════════╝

Tier design:
  CLEAN      (0–30)  : No shared identifiers, no flagged money flow
  WATCH     (31–60)  : Shared touchpoint + one-way money flow (circumstantial)
  SUSPICIOUS (61–90) : Shared IP + bidirectional money flow (no loop)
  BLOCK      (91+)   : Shared device + loop + multiple flags

Score Formula:
  own_score   = Σ (flag_weight × flag_fired)
  final_score = own_score + (0.30 × avg_score_of_direct_neighbours)
"""

import pandas as pd
import networkx as nx
import json
from itertools import combinations
from collections import defaultdict

FLAG_WEIGHTS = {
    "shared_device":              30,
    "shared_ip":                  25,
    "money_received_from_flagged":20,
    "money_sent_to_flagged":      20,
    "shared_touchpoint":          15,
    "transaction_loop":           40,
    "high_velocity":              20,
}
THRESHOLDS = {
    "CLEAN":      (0,  30),
    "WATCH":      (31, 60),
    "SUSPICIOUS": (61, 90),
    "BLOCK":      (91, 9999),
}
THRESHOLD_COLORS = {
    "CLEAN":      "#00cc66",
    "WATCH":      "#ffcc00",
    "SUSPICIOUS": "#ff8c00",
    "BLOCK":      "#ff2244",
}
CONTAMINATION_WEIGHT = 0.30

# ─────────────────────────────────────────────────────────────
# SOURCE DATA
# ─────────────────────────────────────────────────────────────

accounts_data = pd.DataFrame([
    # ── EXISTING BLOCK RING ACCOUNTS ──────────────────────────
    {"account_id":"ACC_A1","type":"UPI ID",         "holder":"Arjun Mehta",    "ring":"A"},
    {"account_id":"ACC_A2","type":"Savings Account","holder":"Sunita Rao",     "ring":"A"},
    {"account_id":"ACC_A3","type":"Digital Wallet", "holder":"Vikram Desai",   "ring":"A"},
    {"account_id":"ACC_A4","type":"UPI ID",         "holder":"Kavitha Nair",   "ring":"A"},
    {"account_id":"ACC_B1","type":"Digital Wallet", "holder":"Rohan Sharma",   "ring":"B"},
    {"account_id":"ACC_B2","type":"Savings Account","holder":"Pooja Verma",    "ring":"B"},
    {"account_id":"ACC_B3","type":"UPI ID",         "holder":"Anil Gupta",     "ring":"B"},
    {"account_id":"ACC_B4","type":"Digital Wallet", "holder":"Meera Joshi",    "ring":"B"},
    {"account_id":"ACC_C1","type":"Savings Account","holder":"Suresh Pillai",  "ring":"C"},
    {"account_id":"ACC_C2","type":"UPI ID",         "holder":"Divya Krishnan", "ring":"C"},
    {"account_id":"ACC_C3","type":"Digital Wallet", "holder":"Karthik Iyer",   "ring":"C"},
    {"account_id":"ACC_C4","type":"Savings Account","holder":"Preethi Nair",   "ring":"C"},
    {"account_id":"ACC_D1","type":"Savings Account","holder":"Neeraj Kapoor",  "ring":"D"},
    {"account_id":"ACC_D2","type":"UPI ID",         "holder":"Shalini Bose",   "ring":"D"},
    {"account_id":"ACC_D3","type":"Digital Wallet", "holder":"Tarun Saxena",   "ring":"D"},

    # ── CLEAN ACCOUNTS (no shared identifiers, no suspicious links)
    # Design: Completely isolated. own = 0, contamination = 0, final = 0
    {"account_id":"ACC_E1","type":"Savings Account","holder":"Priya Menon",    "ring":"CLEAN"},
    {"account_id":"ACC_E2","type":"UPI ID",         "holder":"Ravi Kumar",     "ring":"CLEAN"},
    {"account_id":"ACC_E3","type":"Digital Wallet", "holder":"Ananya Iyer",    "ring":"CLEAN"},

    # ── WATCH ACCOUNTS
    # Design: share one ATM (Chennai) + one-way money flow
    # shared_touchpoint: +15, sent/received money: +20 each
    # own = 35, neighbour_own = 35, contamination = 0.3 * 35 = 10.5, final = 45.5
    {"account_id":"ACC_F1","type":"Savings Account","holder":"Amit Singh",     "ring":"WATCH"},
    {"account_id":"ACC_F2","type":"UPI ID",         "holder":"Deepa Thomas",   "ring":"WATCH"},
    {"account_id":"ACC_F3","type":"Digital Wallet", "holder":"Kunal Mehta",    "ring":"WATCH"},

    # ── SUSPICIOUS ACCOUNTS
    # Design: share IP + bidirectional money flow (no loop detected by cycle algo)
    # shared_ip: +25, sent_money: +20, received_money: +20 = own 65
    # neighbour own = 65, contamination = 0.3 * 65 = 19.5, final = 84.5
    {"account_id":"ACC_G1","type":"Savings Account","holder":"Sanjay Malhotra","ring":"SUSPICIOUS"},
    {"account_id":"ACC_G2","type":"UPI ID",         "holder":"Lalitha Rao",    "ring":"SUSPICIOUS"},
    {"account_id":"ACC_G3","type":"Digital Wallet", "holder":"Mohan Das",      "ring":"SUSPICIOUS"},
])

identifiers_data = pd.DataFrame([
    # ── RING A ──────────────────────────────────────────────────
    {"account_id":"ACC_A1","identifier_type":"IP Address","identifier_value":"192.168.1.1"},
    {"account_id":"ACC_A2","identifier_type":"IP Address","identifier_value":"192.168.1.1"},
    {"account_id":"ACC_A3","identifier_type":"Device MAC","identifier_value":"MAC:EE:01"},
    {"account_id":"ACC_A4","identifier_type":"Device MAC","identifier_value":"MAC:EE:01"},
    # ── RING B ──────────────────────────────────────────────────
    {"account_id":"ACC_B1","identifier_type":"IP Address","identifier_value":"10.0.0.5"},
    {"account_id":"ACC_B2","identifier_type":"IP Address","identifier_value":"10.0.0.5"},
    {"account_id":"ACC_B3","identifier_type":"Device MAC","identifier_value":"MAC:EE:03"},
    {"account_id":"ACC_B4","identifier_type":"Device MAC","identifier_value":"MAC:EE:03"},
    # ── RING C ──────────────────────────────────────────────────
    {"account_id":"ACC_C1","identifier_type":"IP Address","identifier_value":"172.16.0.3"},
    {"account_id":"ACC_C2","identifier_type":"IP Address","identifier_value":"172.16.0.3"},
    {"account_id":"ACC_C3","identifier_type":"Device MAC","identifier_value":"MAC:EE:02"},
    {"account_id":"ACC_C4","identifier_type":"Device MAC","identifier_value":"MAC:EE:02"},
    # ── RING D (bridge) ─────────────────────────────────────────
    {"account_id":"ACC_D1","identifier_type":"IMEI",      "identifier_value":"IMEI-7766"},
    {"account_id":"ACC_A3","identifier_type":"IMEI",      "identifier_value":"IMEI-7766"},  # Bridge A↔D
    {"account_id":"ACC_D3","identifier_type":"IMEI",      "identifier_value":"IMEI-5544"},
    {"account_id":"ACC_C1","identifier_type":"IMEI",      "identifier_value":"IMEI-5544"},  # Bridge C↔D
    {"account_id":"ACC_D2","identifier_type":"IP Address","identifier_value":"10.0.1.8"},
    {"account_id":"ACC_B4","identifier_type":"IP Address","identifier_value":"10.0.1.8"},   # Bridge B↔D
    # ── CLEAN — unique IPs, no shared identifiers ────────────────
    {"account_id":"ACC_E1","identifier_type":"IP Address","identifier_value":"203.0.113.10"},
    {"account_id":"ACC_E2","identifier_type":"IP Address","identifier_value":"203.0.113.20"},
    {"account_id":"ACC_E3","identifier_type":"IP Address","identifier_value":"203.0.113.30"},
    # ── WATCH — all unique IPs, only shared touchpoint ───────────
    {"account_id":"ACC_F1","identifier_type":"IP Address","identifier_value":"198.51.100.1"},
    {"account_id":"ACC_F2","identifier_type":"IP Address","identifier_value":"198.51.100.2"},
    {"account_id":"ACC_F3","identifier_type":"IP Address","identifier_value":"198.51.100.3"},
    # ── SUSPICIOUS — shared IP, no shared device ─────────────────
    {"account_id":"ACC_G1","identifier_type":"IP Address","identifier_value":"10.2.0.99"},
    {"account_id":"ACC_G2","identifier_type":"IP Address","identifier_value":"10.2.0.99"},
    {"account_id":"ACC_G3","identifier_type":"IP Address","identifier_value":"10.2.0.99"},
])

touchpoints_data = pd.DataFrame([
    # ── BLOCK rings ──────────────────────────────────────────────
    {"account_id":"ACC_A1","touchpoint_id":"ATM_MUM"},
    {"account_id":"ACC_A2","touchpoint_id":"ATM_MUM"},
    {"account_id":"ACC_B1","touchpoint_id":"ATM_DEL"},
    {"account_id":"ACC_B3","touchpoint_id":"ATM_DEL"},
    {"account_id":"ACC_C1","touchpoint_id":"PORTAL_BLR"},
    {"account_id":"ACC_C2","touchpoint_id":"PORTAL_BLR"},
    {"account_id":"ACC_C3","touchpoint_id":"PORTAL_BLR"},
    {"account_id":"ACC_D1","touchpoint_id":"ATM_HYD"},
    {"account_id":"ACC_D2","touchpoint_id":"ATM_HYD"},
    # ── WATCH — shared Chennai ATM ───────────────────────────────
    {"account_id":"ACC_F1","touchpoint_id":"ATM_CHN"},
    {"account_id":"ACC_F2","touchpoint_id":"ATM_CHN"},
    {"account_id":"ACC_F3","touchpoint_id":"ATM_CHN"},
])

transactions_data = pd.DataFrame([
    # ── RING A (loop: A1→A2→A3→A4→A1) ──────────────────────────
    {"txn_id":"TXN_A1","sender":"ACC_A1","receiver":"ACC_A2","amount":18000,"timestamp":1},
    {"txn_id":"TXN_A2","sender":"ACC_A2","receiver":"ACC_A3","amount":17500,"timestamp":2},
    {"txn_id":"TXN_A3","sender":"ACC_A3","receiver":"ACC_A4","amount":17000,"timestamp":3},
    {"txn_id":"TXN_A4","sender":"ACC_A4","receiver":"ACC_A1","amount":9500, "timestamp":4},
    # ── RING B (loop: B1→B2→B3→B4→B1) ──────────────────────────
    {"txn_id":"TXN_B1","sender":"ACC_B1","receiver":"ACC_B2","amount":16000,"timestamp":5},
    {"txn_id":"TXN_B2","sender":"ACC_B2","receiver":"ACC_B3","amount":15500,"timestamp":6},
    {"txn_id":"TXN_B3","sender":"ACC_B3","receiver":"ACC_B4","amount":14000,"timestamp":7},
    {"txn_id":"TXN_B4","sender":"ACC_B4","receiver":"ACC_B1","amount":10000,"timestamp":8},
    # ── RING C (loop: C1→C2→C3→C4→C1) ──────────────────────────
    {"txn_id":"TXN_C1","sender":"ACC_C1","receiver":"ACC_C2","amount":14000,"timestamp":9},
    {"txn_id":"TXN_C2","sender":"ACC_C2","receiver":"ACC_C3","amount":13500,"timestamp":10},
    {"txn_id":"TXN_C3","sender":"ACC_C3","receiver":"ACC_C4","amount":13000,"timestamp":11},
    {"txn_id":"TXN_C4","sender":"ACC_C4","receiver":"ACC_C1","amount":7500, "timestamp":12},
    # ── RING D (chain, no full loop in D alone) ───────────────────
    {"txn_id":"TXN_D1","sender":"ACC_D1","receiver":"ACC_D2","amount":22000,"timestamp":13},
    {"txn_id":"TXN_D2","sender":"ACC_D2","receiver":"ACC_D3","amount":21000,"timestamp":14},
    # ── WATCH — one-way flow: F1→F2, F2→F3 ──────────────────────
    # No loop possible; own_score = shared_touchpoint(15) + sent/recv(20) = 35
    {"txn_id":"TXN_F1","sender":"ACC_F1","receiver":"ACC_F2","amount":5000, "timestamp":20},
    {"txn_id":"TXN_F2","sender":"ACC_F2","receiver":"ACC_F3","amount":4500, "timestamp":21},
    # ── SUSPICIOUS — bidirectional but NOT a loop by cycle detection
    # G1→G2, G2→G3, G3→G1 would be a loop; use G1↔G2 and G2↔G3 without closing
    {"txn_id":"TXN_G1","sender":"ACC_G1","receiver":"ACC_G2","amount":9000, "timestamp":30},
    {"txn_id":"TXN_G2","sender":"ACC_G2","receiver":"ACC_G1","amount":8000, "timestamp":31},  # reverse
    {"txn_id":"TXN_G3","sender":"ACC_G2","receiver":"ACC_G3","amount":7500, "timestamp":32},
    {"txn_id":"TXN_G4","sender":"ACC_G3","receiver":"ACC_G2","amount":7000, "timestamp":33},  # reverse
    # ── CLEAN — no transactions at all (E1, E2, E3 are dormant) ──
])

# ─────────────────────────────────────────────────────────────
# BUILD GRAPH
# ─────────────────────────────────────────────────────────────
G = nx.MultiDiGraph()
account_ids = set(accounts_data["account_id"])

print("\n" + "="*66)
print("  PHASE 2 — RISK SCORER  (All Four Tiers)")
print("="*66)
print("\n[1/5] Building graph...")

for _, row in accounts_data.iterrows():
    G.add_node(row["account_id"], node_type="account",
               holder=row["holder"], ring=row["ring"], account_type=row["type"])

ip_groups = identifiers_data[identifiers_data["identifier_type"]=="IP Address"]\
    .groupby("identifier_value")["account_id"].apply(list)
for ip, accs in ip_groups.items():
    for a, b in combinations(accs, 2):
        G.add_edge(a, b, edge_type="shared_ip")
        G.add_edge(b, a, edge_type="shared_ip")

dev_groups = identifiers_data[identifiers_data["identifier_type"].isin(["Device MAC","IMEI"])]\
    .groupby("identifier_value")["account_id"].apply(list)
for dev, accs in dev_groups.items():
    for a, b in combinations(accs, 2):
        G.add_edge(a, b, edge_type="shared_device")
        G.add_edge(b, a, edge_type="shared_device")

tp_groups = touchpoints_data.groupby("touchpoint_id")["account_id"].apply(list)
for tp, accs in tp_groups.items():
    for a, b in combinations(accs, 2):
        G.add_edge(a, b, edge_type="shared_touchpoint")
        G.add_edge(b, a, edge_type="shared_touchpoint")

for _, row in transactions_data.iterrows():
    G.add_edge(row["sender"], row["receiver"],
               edge_type="money_flow", amount=row["amount"], timestamp=row["timestamp"])

print(f"  ✓ Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# ─────────────────────────────────────────────────────────────
# DETECT LOOPS AND VELOCITY
# ─────────────────────────────────────────────────────────────
print("\n[2/5] Detecting loops and high velocity...")

# Only detect loops in MONEY FLOW edges, minimum 3-node cycles
# 2-node back-edges (A→B, B→A) are NOT laundering loops
money_graph = nx.DiGraph()
for u, v, d in G.edges(data=True):
    if d.get("edge_type") == "money_flow" and u in account_ids and v in account_ids:
        money_graph.add_edge(u, v)

loop_accounts = set()
for cycle in nx.simple_cycles(money_graph):
    if len(cycle) >= 3:   # 3+ node cycle = genuine laundering loop
        for node in cycle:
            loop_accounts.add(node)

send_counts = transactions_data.groupby("sender").size()
high_velocity_senders = set(send_counts[send_counts >= 3].index)
print(f"  ✓ Loop accounts    : {len(loop_accounts)} found")
print(f"  ✓ High velocity    : {', '.join(sorted(high_velocity_senders)) or 'None'}")

# ─────────────────────────────────────────────────────────────
# OWN SCORES
# ─────────────────────────────────────────────────────────────
print("\n[3/5] Computing own scores...")

own_scores   = {}
flag_details = {}

for acc in account_ids:
    score = 0; flags = []
    etc = defaultdict(int)
    for _, _, d in G.edges(acc, data=True):    etc[d.get("edge_type","")] += 1
    for _, _, d in G.in_edges(acc, data=True): etc[d.get("edge_type","")] += 1

    if etc["shared_ip"]         > 0: w=FLAG_WEIGHTS["shared_ip"];         score+=w; flags.append({"flag":"Shared IP Address",     "weight":w})
    if etc["shared_device"]     > 0: w=FLAG_WEIGHTS["shared_device"];     score+=w; flags.append({"flag":"Shared Device / IMEI",  "weight":w})
    if etc["shared_touchpoint"] > 0: w=FLAG_WEIGHTS["shared_touchpoint"]; score+=w; flags.append({"flag":"Shared ATM / Portal",   "weight":w})

    in_money = [u for u,_,d in G.in_edges(acc,data=True) if d.get("edge_type")=="money_flow"]
    if in_money:  w=FLAG_WEIGHTS["money_received_from_flagged"]; score+=w; flags.append({"flag":f"Received money ({len(in_money)} txn)","weight":w})
    out_money = [v for _,v,d in G.edges(acc,data=True) if d.get("edge_type")=="money_flow"]
    if out_money: w=FLAG_WEIGHTS["money_sent_to_flagged"];       score+=w; flags.append({"flag":f"Sent money ({len(out_money)} txn)",    "weight":w})

    if acc in loop_accounts:          w=FLAG_WEIGHTS["transaction_loop"]; score+=w; flags.append({"flag":"Transaction Loop Detected","weight":w})
    if acc in high_velocity_senders:  w=FLAG_WEIGHTS["high_velocity"];   score+=w; flags.append({"flag":"High Velocity Sending",    "weight":w})

    own_scores[acc]=score; flag_details[acc]=flags
    ring=G.nodes[acc]["ring"]; holder=G.nodes[acc]["holder"]
    print(f"  {acc} ({holder:20s}) Ring {ring:10s}  own={score:3d}  flags={len(flags)}")

# ─────────────────────────────────────────────────────────────
# NEIGHBOUR CONTAMINATION
# ─────────────────────────────────────────────────────────────
print("\n[4/5] Applying neighbour contamination (30%)...")

final_scores = {}
for acc in account_ids:
    nbrs = set()
    for _,v in G.out_edges(acc):
        if v in account_ids: nbrs.add(v)
    for u,_ in G.in_edges(acc):
        if u in account_ids: nbrs.add(u)

    contamination = CONTAMINATION_WEIGHT * (sum(own_scores[n] for n in nbrs)/len(nbrs)) if nbrs else 0
    final_scores[acc] = round(own_scores[acc] + contamination, 1)

    holder=G.nodes[acc]["holder"]
    print(f"  {acc} ({holder:20s})  own={own_scores[acc]:3d}  +contam={contamination:5.1f}  →  final={final_scores[acc]}")

# ─────────────────────────────────────────────────────────────
# CLASSIFY
# ─────────────────────────────────────────────────────────────
def classify(s):
    for label,(lo,hi) in THRESHOLDS.items():
        if lo<=s<=hi: return label
    return "BLOCK"

classifications = {acc: classify(s) for acc, s in final_scores.items()}

print("\n" + "="*66)
print("  RESULTS BY TIER")
print("="*66)
icons={"BLOCK":"🔴","SUSPICIOUS":"🟠","WATCH":"🟡","CLEAN":"🟢"}
for level in ["BLOCK","SUSPICIOUS","WATCH","CLEAN"]:
    grp = sorted([(a,final_scores[a]) for a,c in classifications.items() if c==level], key=lambda x:-x[1])
    if grp:
        print(f"\n  {icons[level]} {level}")
        for acc,score in grp:
            holder=G.nodes[acc]["holder"]; ring=G.nodes[acc]["ring"]
            print(f"     {acc} ({holder})  Score: {score}  Ring: {ring}")

# ─────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────
print("\n  Exporting scored_graph_v2.json...")

export_nodes=[]
for acc in account_ids:
    nd=dict(G.nodes[acc])
    nd.update({"id":acc,"own_score":own_scores[acc],"final_score":final_scores[acc],
               "classification":classifications[acc],"color":THRESHOLD_COLORS[classifications[acc]],
               "flags":flag_details[acc],"label":acc})
    export_nodes.append(nd)

identifier_nodes=[]
seen_ids=set()
for _,row in identifiers_data.iterrows():
    v=row["identifier_value"]
    if v not in seen_ids:
        seen_ids.add(v)
        identifier_nodes.append({"id":v,"node_type":"identifier","label":v[:12],
            "identifier_type":row["identifier_type"],"own_score":0,"final_score":0,
            "classification":"","color":"#bf7aff","flags":[]})

tp_nodes=[
    {"id":"ATM_MUM",   "node_type":"touchpoint","label":"ATM Mumbai",   "city":"Mumbai",    "own_score":0,"final_score":0,"classification":"","color":"#ff8c42","flags":[]},
    {"id":"ATM_DEL",   "node_type":"touchpoint","label":"ATM Delhi",    "city":"Delhi",     "own_score":0,"final_score":0,"classification":"","color":"#ff8c42","flags":[]},
    {"id":"PORTAL_BLR","node_type":"touchpoint","label":"Portal BLR",   "city":"Bangalore", "own_score":0,"final_score":0,"classification":"","color":"#ff8c42","flags":[]},
    {"id":"ATM_HYD",   "node_type":"touchpoint","label":"ATM Hyderabad","city":"Hyderabad", "own_score":0,"final_score":0,"classification":"","color":"#ff8c42","flags":[]},
    {"id":"ATM_CHN",   "node_type":"touchpoint","label":"ATM Chennai",  "city":"Chennai",   "own_score":0,"final_score":0,"classification":"","color":"#ff8c42","flags":[]},
]

all_nodes=export_nodes+identifier_nodes+tp_nodes

export_edges=[]
seen_e=set()
for u,v,d in G.edges(data=True):
    k=(u,v,d.get("edge_type"))
    if k not in seen_e:
        seen_e.add(k)
        export_edges.append({"source":u,"target":v,"edge_type":d.get("edge_type",""),"amount":d.get("amount",0)})

for _,row in identifiers_data.iterrows():
    export_edges.append({"source":row["account_id"],"target":row["identifier_value"],"edge_type":"uses_identifier","amount":0})
for _,row in touchpoints_data.iterrows():
    export_edges.append({"source":row["account_id"],"target":row["touchpoint_id"],"edge_type":"used_touchpoint","amount":0})

summary={
    "BLOCK":      sum(1 for c in classifications.values() if c=="BLOCK"),
    "SUSPICIOUS": sum(1 for c in classifications.values() if c=="SUSPICIOUS"),
    "WATCH":      sum(1 for c in classifications.values() if c=="WATCH"),
    "CLEAN":      sum(1 for c in classifications.values() if c=="CLEAN"),
    "max_score":  max(final_scores.values()),
    "avg_score":  round(sum(final_scores.values())/len(final_scores),1),
}

with open("/mnt/user-data/outputs/scored_graph_v2.json","w") as f:
    json.dump({"nodes":all_nodes,"edges":export_edges,"summary":summary},f,indent=2)

print(f"  ✅ Done  |  BLOCK:{summary['BLOCK']}  SUSPICIOUS:{summary['SUSPICIOUS']}  WATCH:{summary['WATCH']}  CLEAN:{summary['CLEAN']}")
print(f"           |  Max:{summary['max_score']}  Avg:{summary['avg_score']}")
print("="*66+"\n")
