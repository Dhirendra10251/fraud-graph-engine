"""
╔══════════════════════════════════════════════════════════╗
║       FRAUD GRAPH BUILDER — Identity-Agnostic Engine     ║
║  Stitches Accounts + Physical Identifiers + Touchpoints  ║
╚══════════════════════════════════════════════════════════╝

Node Types:
  • Account       — Savings/UPI/Wallet
  • Identifier    — IP Address, Device MAC ID
  • Touchpoint    — ATM Location, Web Portal

Edge Types:
  • money_flow    — Account A sent money to Account B
  • shared_ip     — Both accounts logged in from same IP
  • shared_device — Both accounts used same Device/IMEI
  • used_at       — Account accessed a physical Touchpoint
"""

import pandas as pd
import networkx as nx
import json
from itertools import combinations

# ─────────────────────────────────────────────
# STEP 1: Define the Three Source Tables
# ─────────────────────────────────────────────

# TABLE 1: Accounts
accounts_data = pd.DataFrame([
    {"account_id": "ACC_001", "type": "Savings Account", "holder": "Ravi Kumar",      "registered_ip": "192.168.1.1"},
    {"account_id": "ACC_002", "type": "UPI ID",          "holder": "Priya Sharma",    "registered_ip": "192.168.1.1"},
    {"account_id": "ACC_003", "type": "Digital Wallet",  "holder": "Fake Name A",     "registered_ip": "10.0.0.5"},
    {"account_id": "ACC_004", "type": "Savings Account", "holder": "Amit Singh",      "registered_ip": "172.16.0.3"},
    {"account_id": "ACC_005", "type": "UPI ID",          "holder": "Fake Name B",     "registered_ip": "10.0.0.5"},
    {"account_id": "ACC_006", "type": "Digital Wallet",  "holder": "Deepa Nair",      "registered_ip": "172.16.0.3"},
])

# TABLE 2: Physical Identifiers (IP logins & Device usage)
identifiers_data = pd.DataFrame([
    {"account_id": "ACC_001", "identifier_type": "IP Address",    "identifier_value": "192.168.1.1",       "event": "login"},
    {"account_id": "ACC_002", "identifier_type": "IP Address",    "identifier_value": "192.168.1.1",       "event": "login"},
    {"account_id": "ACC_003", "identifier_type": "IP Address",    "identifier_value": "10.0.0.5",          "event": "login"},
    {"account_id": "ACC_005", "identifier_type": "IP Address",    "identifier_value": "10.0.0.5",          "event": "login"},
    {"account_id": "ACC_001", "identifier_type": "Device MAC ID", "identifier_value": "AA:BB:CC:DD:EE:01", "event": "transaction"},
    {"account_id": "ACC_004", "identifier_type": "Device MAC ID", "identifier_value": "AA:BB:CC:DD:EE:01", "event": "transaction"},
    {"account_id": "ACC_003", "identifier_type": "Device MAC ID", "identifier_value": "FF:EE:DD:CC:BB:02", "event": "login"},
    {"account_id": "ACC_005", "identifier_type": "Device MAC ID", "identifier_value": "FF:EE:DD:CC:BB:02", "event": "login"},
    {"account_id": "ACC_002", "identifier_type": "IMEI",          "identifier_value": "IMEI-9988776655",   "event": "app_login"},
    {"account_id": "ACC_006", "identifier_type": "IMEI",          "identifier_value": "IMEI-9988776655",   "event": "app_login"},
])

# TABLE 3: Touchpoints (Where physical interactions happen)
touchpoints_data = pd.DataFrame([
    {"account_id": "ACC_001", "touchpoint_type": "ATM Location", "touchpoint_id": "ATM_MUM_001", "city": "Mumbai"},
    {"account_id": "ACC_002", "touchpoint_type": "ATM Location", "touchpoint_id": "ATM_MUM_001", "city": "Mumbai"},
    {"account_id": "ACC_003", "touchpoint_type": "Web Portal",   "touchpoint_id": "PORTAL_NET",  "city": "Online"},
    {"account_id": "ACC_004", "touchpoint_type": "ATM Location", "touchpoint_id": "ATM_DEL_007", "city": "Delhi"},
    {"account_id": "ACC_005", "touchpoint_type": "Web Portal",   "touchpoint_id": "PORTAL_NET",  "city": "Online"},
    {"account_id": "ACC_006", "touchpoint_type": "ATM Location", "touchpoint_id": "ATM_DEL_007", "city": "Delhi"},
])

# TABLE 4: Transaction Log (Money Flow)
transactions_data = pd.DataFrame([
    {"txn_id": "TXN_001", "sender": "ACC_001", "receiver": "ACC_003", "amount": 15000, "currency": "INR"},
    {"txn_id": "TXN_002", "sender": "ACC_003", "receiver": "ACC_005", "amount": 14500, "currency": "INR"},
    {"txn_id": "TXN_003", "sender": "ACC_002", "receiver": "ACC_004", "amount": 8000,  "currency": "INR"},
    {"txn_id": "TXN_004", "sender": "ACC_004", "receiver": "ACC_006", "amount": 7800,  "currency": "INR"},
    {"txn_id": "TXN_005", "sender": "ACC_005", "receiver": "ACC_006", "amount": 5000,  "currency": "INR"},
])


# ─────────────────────────────────────────────
# STEP 2: Initialize the Graph
# ─────────────────────────────────────────────

G = nx.MultiDiGraph()  # Directed, multi-edge (multiple edge types allowed)

print("\n" + "="*60)
print("  FRAUD GRAPH BUILDER — Processing Tables")
print("="*60)


# ─────────────────────────────────────────────
# STEP 3: Add Account Nodes
# ─────────────────────────────────────────────

print("\n[1/4] Adding ACCOUNT nodes...")
for _, row in accounts_data.iterrows():
    G.add_node(
        row["account_id"],
        node_type="account",
        label=row["account_id"],
        account_type=row["type"],
        holder=row["holder"],
    )
    print(f"  ✓ Node: {row['account_id']} ({row['type']}) — {row['holder']}")


# ─────────────────────────────────────────────
# STEP 4: STITCH — Shared IP Links
# ─────────────────────────────────────────────

print("\n[2/4] Stitching IDENTITY LINKS (Shared IP / Device)...")

ip_groups = identifiers_data[
    identifiers_data["identifier_type"] == "IP Address"
].groupby("identifier_value")["account_id"].apply(list)

for ip, accs in ip_groups.items():
    for acc_a, acc_b in combinations(accs, 2):
        G.add_node(ip, node_type="identifier", label=ip, identifier_type="IP Address")
        G.add_edge(acc_a, ip,    edge_type="uses_ip",    identifier=ip)
        G.add_edge(acc_b, ip,    edge_type="uses_ip",    identifier=ip)
        G.add_edge(acc_a, acc_b, edge_type="shared_ip",  identifier=ip)
        print(f"  🔗 shared_ip:     {acc_a} ↔ {acc_b}  [IP: {ip}]")

mac_groups = identifiers_data[
    identifiers_data["identifier_type"].isin(["Device MAC ID", "IMEI"])
].groupby("identifier_value")["account_id"].apply(list)

for device, accs in mac_groups.items():
    for acc_a, acc_b in combinations(accs, 2):
        G.add_node(device, node_type="identifier", label=device, identifier_type="Device")
        G.add_edge(acc_a, device, edge_type="uses_device",    device_id=device)
        G.add_edge(acc_b, device, edge_type="uses_device",    device_id=device)
        G.add_edge(acc_a, acc_b,  edge_type="shared_device",  device_id=device)
        print(f"  🔗 shared_device: {acc_a} ↔ {acc_b}  [Device: {device}]")


# ─────────────────────────────────────────────
# STEP 5: STITCH — Money Flow Links
# ─────────────────────────────────────────────

print("\n[3/4] Stitching TRANSACTION LINKS (Money Flow)...")
for _, row in transactions_data.iterrows():
    G.add_edge(
        row["sender"], row["receiver"],
        edge_type="money_flow",
        txn_id=row["txn_id"],
        amount=row["amount"],
    )
    print(f"  💸 money_flow:    {row['sender']} → {row['receiver']}  [₹{row['amount']}]")


# ─────────────────────────────────────────────
# STEP 6: STITCH — Touchpoint Links
# ─────────────────────────────────────────────

print("\n[4/4] Stitching TOUCHPOINT LINKS (ATM / Portal)...")
touchpoint_groups = touchpoints_data.groupby("touchpoint_id")["account_id"].apply(list)

for tp_id, accs in touchpoint_groups.items():
    tp_row = touchpoints_data[touchpoints_data["touchpoint_id"] == tp_id].iloc[0]
    G.add_node(
        tp_id, node_type="touchpoint",
        label=tp_id,
        touchpoint_type=tp_row["touchpoint_type"],
        city=tp_row["city"],
    )
    for acc in accs:
        G.add_edge(acc, tp_id, edge_type="used_touchpoint", touchpoint=tp_id)
        print(f"  📍 touchpoint:    {acc} → {tp_id} ({tp_row['touchpoint_type']}, {tp_row['city']})")

    for acc_a, acc_b in combinations(accs, 2):
        G.add_edge(acc_a, acc_b, edge_type="shared_touchpoint", touchpoint=tp_id)
        print(f"  🔗 shared_touch:  {acc_a} ↔ {acc_b}  [Touchpoint: {tp_id}]")


# ─────────────────────────────────────────────
# STEP 7: Analyze — Flag Suspicious Clusters
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("  GRAPH ANALYSIS RESULTS")
print("="*60)

print(f"\n  Total Nodes : {G.number_of_nodes()}")
print(f"  Total Edges : {G.number_of_edges()}")

# Find connected components (undirected view for clustering)
undirected = G.to_undirected()
components = list(nx.connected_components(undirected))
print(f"\n  Connected Clusters (Fraud Rings): {len(components)}")

for i, cluster in enumerate(components, 1):
    account_nodes = [n for n in cluster if G.nodes[n].get("node_type") == "account"]
    if len(account_nodes) > 1:
        print(f"\n  ⚠️  CLUSTER {i} — SUSPICIOUS RING DETECTED")
        print(f"     Accounts  : {', '.join(account_nodes)}")
        holders = [G.nodes[a].get("holder", "?") for a in account_nodes]
        print(f"     Holders   : {', '.join(holders)}")
        # Count shared link types
        edge_types = []
        for u, v, data in G.edges(data=True):
            if u in cluster and v in cluster:
                edge_types.append(data.get("edge_type", "unknown"))
        from collections import Counter
        for etype, count in Counter(edge_types).items():
            print(f"     {etype:<20}: {count} link(s)")

# ─────────────────────────────────────────────
# STEP 8: Export graph as JSON for visualization
# ─────────────────────────────────────────────

graph_data = {
    "nodes": [],
    "edges": []
}

for node, data in G.nodes(data=True):
    graph_data["nodes"].append({"id": node, **data})

seen_edges = set()
for u, v, data in G.edges(data=True):
    key = (u, v, data.get("edge_type"))
    if key not in seen_edges:
        seen_edges.add(key)
        graph_data["edges"].append({"source": u, "target": v, **{k: v2 for k, v2 in data.items() if isinstance(v2, (str, int, float, bool))}})

with open("/mnt/user-data/outputs/graph_data.json", "w") as f:
    json.dump(graph_data, f, indent=2)

print("\n  ✅ Graph exported to: graph_data.json")
print("\n" + "="*60)
print("  Done! Use the companion HTML visualization to explore.")
print("="*60 + "\n")
