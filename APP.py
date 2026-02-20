"""
╔══════════════════════════════════════════════════════════════╗
║   FRAUD GRAPH ENGINE — Phase 3: Streamlit Dashboard          ║
║   app.py  — Run with: streamlit run app.py                   ║
╚══════════════════════════════════════════════════════════════╝

Dependencies:
    pip install streamlit pydeck pandas networkx

File layout (keep in same folder):
    fraud_engine.py   ← modular engine (Gap Fix)
    app.py            ← this file
"""

import streamlit as st
import pydeck as pdk
import pandas as pd
import sys
import os

# ── Make sure fraud_engine.py is importable from same directory
sys.path.insert(0, os.path.dirname(__file__))
from fraud_engine import (
    full_pipeline, TIER_COLORS_HEX, TIER_COLORS_RGB,
    TOUCHPOINT_GEO
)

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title = "Fraud Graph Engine — Phase 3",
    page_icon  = "🕵️",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

# ─────────────────────────────────────────────────────────────
# CUSTOM CSS  (dark terminal theme matching Phase 1/2)
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;900&family=Share+Tech+Mono&display=swap');

  html, body, [class*="css"] { background-color: #040a12 !important; color: #c8e0ff !important; }

  /* Header */
  .fraud-header {
    font-family: 'Barlow Condensed', sans-serif;
    font-size: 32px; font-weight: 900; letter-spacing: 5px;
    text-transform: uppercase; color: #ffffff;
    border-bottom: 3px solid #0078D4;
    padding-bottom: 10px; margin-bottom: 4px;
  }
  .fraud-sub {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px; color: #3a6a9a; letter-spacing: 2px;
    text-transform: uppercase; margin-bottom: 20px;
  }

  /* Tier metric cards */
  .tier-card {
    border-radius: 3px; padding: 12px 14px;
    margin-bottom: 8px; border-left: 4px solid;
    font-family: 'Share Tech Mono', monospace;
  }
  .tier-block     { background:#1a0408; border-color:#ff2244; }
  .tier-suspicious{ background:#1a0e00; border-color:#ff8c00; }
  .tier-watch     { background:#1a1600; border-color:#ffcc00; }
  .tier-clean     { background:#001a0a; border-color:#00cc66; }
  .tier-name  { font-size:11px; font-weight:bold; letter-spacing:2px; }
  .tier-count { font-size:28px; font-weight:bold; line-height:1; }
  .tier-range { font-size:9px; color:#4a6a8a; }

  /* Breakdown panel */
  .breakdown-card {
    background: #060d1a; border: 1px solid #0e2240;
    border-radius: 3px; padding: 14px; margin-top: 8px;
  }
  .bd-name  { font-family:'Barlow Condensed',sans-serif; font-size:22px; font-weight:900; color:#ffffff; }
  .bd-sub   { font-family:'Share Tech Mono',monospace; font-size:10px; color:#4a6a8a; margin-top:2px; margin-bottom:10px; }
  .bd-score { font-size:32px; font-weight:bold; font-family:'Barlow Condensed',sans-serif; }
  .flag-row { display:flex; justify-content:space-between; padding:4px 0; border-bottom:1px solid #0a1828; font-size:11px; }
  .flag-name{ color:#8aacc8; } .flag-wt{ font-weight:bold; }

  /* Sidebar */
  section[data-testid="stSidebar"] > div { background:#060d1a !important; }
  section[data-testid="stSidebar"] * { color:#c8e0ff !important; }

  /* Map label */
  .map-label {
    font-family:'Share Tech Mono',monospace; font-size:10px;
    color:#3a6a9a; letter-spacing:1.5px; text-transform:uppercase;
    margin-bottom:6px;
  }

  /* Table */
  .stDataFrame { background:#040a12 !important; }

  /* Pulsing alert dot */
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.2} }
  .alert-dot {
    display:inline-block; width:10px; height:10px; border-radius:50%;
    background:#ff2244; box-shadow:0 0 10px #ff2244;
    animation:pulse 1.4s infinite; margin-right:8px;
  }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LOAD DATA  (cached so it only runs once per session)
# ─────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    G, scores, arcs, clusters, dfs = full_pipeline()
    return G, scores, arcs, clusters, dfs

with st.spinner("Running fraud graph engine…"):
    G, scores, arcs, clusters, dfs = load_data()

# Flatten scores to DataFrame
scores_df = pd.DataFrame(scores.values())
tier_order = {"BLOCK": 0, "SUSPICIOUS": 1, "WATCH": 2, "CLEAN": 3}
scores_df["tier_order"] = scores_df["classification"].map(tier_order)
scores_df = scores_df.sort_values("tier_order")

tier_counts = scores_df["classification"].value_counts().to_dict()
for t in ["BLOCK","SUSPICIOUS","WATCH","CLEAN"]:
    tier_counts.setdefault(t, 0)

# ─────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown(
        '<div class="fraud-header"><span class="alert-dot"></span>Fraud Graph Engine</div>'
        '<div class="fraud-sub">Phase 3 · Real-time Risk Map · Identity-Agnostic Detection</div>',
        unsafe_allow_html=True
    )
with col_h2:
    st.markdown("<br>", unsafe_allow_html=True)
    st.metric("Accounts Analysed", len(scores))

# ─────────────────────────────────────────────────────────────
# TIER METRIC CARDS  (4 columns)
# ─────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="tier-card tier-block">
      <div class="tier-name" style="color:#ff2244">🔴 BLOCK</div>
      <div class="tier-count" style="color:#ff2244">{tier_counts['BLOCK']}</div>
      <div class="tier-range">Score 91+ · Immediate action</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="tier-card tier-suspicious">
      <div class="tier-name" style="color:#ff8c00">🟠 SUSPICIOUS</div>
      <div class="tier-count" style="color:#ff8c00">{tier_counts['SUSPICIOUS']}</div>
      <div class="tier-range">Score 61–90 · Investigate</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="tier-card tier-watch">
      <div class="tier-name" style="color:#ffcc00">🟡 WATCH</div>
      <div class="tier-count" style="color:#ffcc00">{tier_counts['WATCH']}</div>
      <div class="tier-range">Score 31–60 · Monitor</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="tier-card tier-clean">
      <div class="tier-name" style="color:#00cc66">🟢 CLEAN</div>
      <div class="tier-count" style="color:#00cc66">{tier_counts['CLEAN']}</div>
      <div class="tier-range">Score 0–30 · No action</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# SIDEBAR — FILTERS + SCORE BREAKDOWN
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🗺 Map Controls")
    st.markdown("---")

    # Tier filter
    show_tiers = st.multiselect(
        "Show Risk Tiers",
        options=["BLOCK", "SUSPICIOUS", "WATCH", "CLEAN"],
        default=["BLOCK", "SUSPICIOUS", "WATCH", "CLEAN"],
    )

    # Layer toggles
    st.markdown("**Map Layers**")
    show_pins     = st.checkbox("Account Pins",           value=True)
    show_arcs     = st.checkbox("Transaction Flow Arrows", value=True)
    show_clusters = st.checkbox("Ring Cluster Highlights", value=True)

    # City filter
    cities = sorted(scores_df["city"].unique().tolist())
    show_cities = st.multiselect("Filter by City", options=cities, default=cities)

    st.markdown("---")
    st.markdown("### 🔍 Score Breakdown")

    # Account selector
    account_options = scores_df[["account_id","holder","classification","final_score"]].copy()
    account_options["label"] = account_options.apply(
        lambda r: f"{r['holder']}  [{r['classification']} · {r['final_score']}]", axis=1
    )
    selected_label = st.selectbox(
        "Select Account",
        options=account_options["label"].tolist(),
        index=0
    )
    selected_id = account_options.loc[account_options["label"]==selected_label, "account_id"].values[0]
    sel         = scores[selected_id]
    sel_color   = TIER_COLORS_HEX[sel["classification"]]

    # Score bar
    bar_pct = min(int((sel["final_score"] / 180) * 100), 100)
    bar_col = sel_color.lstrip("#")
    # map tier to css class name suffix
    tier_css = sel["classification"].lower().replace("suspicious", "suspicious")

    st.markdown(f"""
    <div class="breakdown-card">
      <div class="bd-name">{sel['holder']}</div>
      <div class="bd-sub">{selected_id} · {sel['account_type']} · {sel['city']}</div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
        <span style="font-size:11px;color:#4a6a8a;font-family:monospace">Final Score</span>
        <span class="bd-score" style="color:{sel_color}">{sel['final_score']}</span>
      </div>
      <div style="height:8px;background:#0a1828;border-radius:2px;overflow:hidden;margin-bottom:4px">
        <div style="height:100%;width:{bar_pct}%;background:{sel_color};box-shadow:0 0 8px {sel_color}88;border-radius:2px"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:9px;color:#3a5a7a;font-family:monospace;margin-bottom:12px">
        <span>0</span><span>Own: {sel['own_score']}</span><span>180</span>
      </div>
      <div style="font-size:9px;color:#3a5a7a;font-family:monospace;margin-bottom:6px;border-bottom:1px dashed #1a3a5c;padding-bottom:4px">
        FLAG BREAKDOWN
      </div>
      {''.join(f"""<div class="flag-row">
        <span class="flag-name">⚑ {f['flag']}</span>
        <span class="flag-wt" style="color:{sel_color}">+{f['weight']}</span>
      </div>""" for f in sel['flags'])}
      {'<div style="border-top:1px dashed #1a3a5c;margin-top:6px;padding-top:6px;display:flex;justify-content:space-between;font-size:11px;color:#4a6a8a;font-family:monospace"><span>+ Neighbour Contamination</span><span style="color:#7dd4fc">+' + str(sel["contamination"]) + '</span></div>' if sel['contamination'] > 0 else ''}
      <div style="margin-top:10px;padding:6px 8px;background:rgba(255,255,255,0.03);border-radius:2px;font-size:9px;color:#3a5a7a;font-family:monospace">
        Ring: <span style="color:#7dd4fc">{sel['ring']}</span> &nbsp;|&nbsp;
        City: <span style="color:#7dd4fc">{sel['city']}</span> &nbsp;|&nbsp;
        Loop: <span style="color:{'#ff2244' if sel['in_loop'] else '#00cc66'}">{'Yes' if sel['in_loop'] else 'No'}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# FILTER DATA FOR MAP
# ─────────────────────────────────────────────────────────────
filtered_scores = {
    acc: d for acc, d in scores.items()
    if d["classification"] in show_tiers and d["city"] in show_cities
}
filtered_ids = set(filtered_scores.keys())

# Account pin DataFrame
pin_df = pd.DataFrame([
    {
        "lat":            d["lat"],
        "lon":            d["lon"],
        "account_id":     acc,
        "holder":         d["holder"],
        "city":           d["city"],
        "classification": d["classification"],
        "final_score":    d["final_score"],
        "own_score":      d["own_score"],
        "ring":           d["ring"],
        "r": d["color_rgb"][0],
        "g": d["color_rgb"][1],
        "b": d["color_rgb"][2],
        "a": d["color_rgb"][3],
        "radius": max(1800, int(d["final_score"] * 12)),  # bigger = higher risk
    }
    for acc, d in filtered_scores.items()
])

# Arc DataFrame (only show arcs where both endpoints are in filtered set)
arc_df = pd.DataFrame([
    {
        "src_lat":     a["src_lat"],
        "src_lon":     a["src_lon"],
        "tgt_lat":     a["tgt_lat"],
        "tgt_lon":     a["tgt_lon"],
        "sender_name": a["sender_name"],
        "recv_name":   a["receiver_name"],
        "amount":      a["amount"],
        "txn_id":      a["txn_id"],
        "risk_tier":   a["risk_tier"],
        "sr": a["src_color"][0], "sg": a["src_color"][1],
        "sb": a["src_color"][2], "sa": 200,
        "tr": a["tgt_color"][0], "tg": a["tgt_color"][1],
        "tb": a["tgt_color"][2], "ta": 200,
    }
    for a in arcs
    if a["sender"] in filtered_ids and a["receiver"] in filtered_ids
])

# Cluster DataFrame
cluster_df = pd.DataFrame([
    {
        "lat":    c["lat"],
        "lon":    c["lon"],
        "radius": c["radius"],
        "ring":   c["ring"],
        "count":  c["count"],
        "r": c["color"][0], "g": c["color"][1],
        "b": c["color"][2], "a": c["color"][3],
    }
    for c in clusters
])

# ─────────────────────────────────────────────────────────────
# BUILD PYDECK LAYERS
# ─────────────────────────────────────────────────────────────
layers = []

# Layer 1 — Ring cluster highlight circles
if show_clusters and not cluster_df.empty:
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data          = cluster_df,
        get_position  = ["lon", "lat"],
        get_radius    = "radius",
        get_fill_color= ["r","g","b","a"],
        get_line_color= ["r","g","b",180],
        stroked       = True,
        line_width_min_pixels = 2,
        pickable      = True,
        id            = "clusters",
    ))

# Layer 2 — Transaction flow arcs
if show_arcs and not arc_df.empty:
    layers.append(pdk.Layer(
        "ArcLayer",
        data               = arc_df,
        get_source_position= ["src_lon","src_lat"],
        get_target_position= ["tgt_lon","tgt_lat"],
        get_source_color   = ["sr","sg","sb","sa"],
        get_target_color   = ["tr","tg","tb","ta"],
        get_width          = 2.5,
        pickable           = True,
        auto_highlight     = True,
        id                 = "arcs",
    ))

# Layer 3 — Account pins (risk-colored dots)
if show_pins and not pin_df.empty:
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data          = pin_df,
        get_position  = ["lon","lat"],
        get_radius    = "radius",
        get_fill_color= ["r","g","b","a"],
        get_line_color= ["r","g","b",255],
        stroked       = True,
        line_width_min_pixels = 2,
        pickable      = True,
        auto_highlight= True,
        id            = "pins",
    ))

# Layer 4 — Text labels for city names
if show_pins and not pin_df.empty:
    layers.append(pdk.Layer(
        "TextLayer",
        data          = pin_df,
        get_position  = ["lon","lat"],
        get_text      = "holder",
        get_size      = 13,
        get_color     = [200, 220, 255, 200],
        get_angle     = 0,
        get_alignment_baseline = "'bottom'",
        billboard     = True,
        pickable      = False,
    ))

# Map view — India centred
view_state = pdk.ViewState(
    latitude  = 20.5937,
    longitude = 78.9629,
    zoom      = 4.8,
    pitch     = 45,
    bearing   = 0,
)

# Tooltip
tooltip = {
    "html": """
    <div style='background:#040a12;border:1px solid #0e2240;padding:10px 14px;
                font-family:monospace;font-size:12px;color:#c8e0ff;line-height:1.8'>
      <b style='color:#7dd4fc'>{holder}</b><br>
      ID: {account_id}<br>
      Score: <b style='color:#ff8c00'>{final_score}</b> &nbsp;·&nbsp; {classification}<br>
      Ring: {ring} &nbsp;·&nbsp; {city}
    </div>""",
    "style": {"backgroundColor": "transparent", "color": "white"},
}

# ─────────────────────────────────────────────────────────────
# MAP + ACCOUNT TABLE  (side by side)
# ─────────────────────────────────────────────────────────────
map_col, table_col = st.columns([3, 1])

with map_col:
    st.markdown('<div class="map-label">🗺 &nbsp;India Fraud Activity Map</div>', unsafe_allow_html=True)

    if layers:
        st.pydeck_chart(pdk.Deck(
            layers      = layers,
            initial_view_state = view_state,
            tooltip     = tooltip,
            map_style   = "mapbox://styles/mapbox/dark-v10",
        ), use_container_width=True, height=540)
    else:
        st.info("Select at least one tier and layer to display the map.")

with table_col:
    st.markdown('<div class="map-label">📋 &nbsp;Account Risk Table</div>', unsafe_allow_html=True)

    display_df = scores_df[["holder","city","classification","final_score","own_score","ring"]].copy()
    display_df.columns = ["Name","City","Tier","Score","Own","Ring"]

    # Style the dataframe
    def colour_tier(val):
        col = {"BLOCK":"#ff2244","SUSPICIOUS":"#ff8c00","WATCH":"#ffcc00","CLEAN":"#00cc66"}.get(val,"")
        return f"color: {col}; font-weight: bold"

    styled = display_df.style\
        .applymap(colour_tier, subset=["Tier"])\
        .background_gradient(subset=["Score"], cmap="Reds", vmin=0, vmax=185)\
        .set_properties(**{"font-size":"11px","font-family":"monospace"})

    st.dataframe(styled, use_container_width=True, height=540)

# ─────────────────────────────────────────────────────────────
# BOTTOM SECTION — Algorithm Explainer
# ─────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("### ⚙️ How the Score is Calculated")

exp1, exp2, exp3 = st.columns(3)

with exp1:
    st.markdown("**Flag Weights (Own Score)**")
    weights_df = pd.DataFrame([
        {"Flag": "Transaction Loop",     "Weight": 40, "Why": "Strongest laundering signal"},
        {"Flag": "Shared Device / IMEI", "Weight": 30, "Why": "Devices don't lie"},
        {"Flag": "Shared IP Address",    "Weight": 25, "Why": "Strong identity link"},
        {"Flag": "Received Money",       "Weight": 20, "Why": "Financial contamination"},
        {"Flag": "Sent Money",           "Weight": 20, "Why": "Complicit in outflow"},
        {"Flag": "High Velocity",        "Weight": 20, "Why": "Burst sending pattern"},
        {"Flag": "Shared ATM / Portal",  "Weight": 15, "Why": "Circumstantial evidence"},
    ])
    st.dataframe(weights_df, use_container_width=True, hide_index=True)

with exp2:
    st.markdown("**Risk Thresholds**")
    thresh_df = pd.DataFrame([
        {"Tier": "🔴 BLOCK",       "Range": "91+",    "Action": "Freeze account immediately"},
        {"Tier": "🟠 SUSPICIOUS",  "Range": "61–90",  "Action": "Flag for investigation"},
        {"Tier": "🟡 WATCH",       "Range": "31–60",  "Action": "Enhanced monitoring"},
        {"Tier": "🟢 CLEAN",       "Range": "0–30",   "Action": "No action required"},
    ])
    st.dataframe(thresh_df, use_container_width=True, hide_index=True)

with exp3:
    st.markdown("**Contamination Formula**")
    st.code("""
own_score = Σ (flag_weight × flag_fired)

final_score = own_score
            + (0.30 × avg_neighbour_score)

# Example — Deepa Thomas (ACC_F2):
# own_score     = 55  (WATCH range)
# contamination = 10.5 (from F1, F3)
# final_score   = 65.5 → SUSPICIOUS
    """, language="python")
    st.markdown(
        "Contamination ensures clean-looking accounts "
        "**inside dirty clusters** are not missed."
    )

# ─────────────────────────────────────────────────────────────
# TRANSACTION LOG TABLE
# ─────────────────────────────────────────────────────────────
with st.expander("📜 View Full Transaction Log"):
    txn_df = dfs["transactions"].copy()
    txn_df["sender_name"]   = txn_df["sender"].map(lambda x: scores.get(x,{}).get("holder",""))
    txn_df["receiver_name"] = txn_df["receiver"].map(lambda x: scores.get(x,{}).get("holder",""))
    txn_df["sender_tier"]   = txn_df["sender"].map(lambda x: scores.get(x,{}).get("classification",""))
    txn_df["receiver_tier"] = txn_df["receiver"].map(lambda x: scores.get(x,{}).get("classification",""))
    txn_df["amount_fmt"]    = txn_df["amount"].apply(lambda x: f"₹{x:,}")
    st.dataframe(
        txn_df[["txn_id","sender_name","receiver_name","amount_fmt","sender_tier","receiver_tier"]]\
            .rename(columns={"txn_id":"TXN ID","sender_name":"Sender","receiver_name":"Receiver",
                             "amount_fmt":"Amount","sender_tier":"Sender Tier","receiver_tier":"Receiver Tier"}),
        use_container_width=True, hide_index=True
    )

# ─────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    "<div style='text-align:center;font-family:monospace;font-size:10px;color:#1e3a5a;letter-spacing:2px'>"
    "FRAUD GRAPH ENGINE · PHASE 3 · IDENTITY-AGNOSTIC DETECTION · BUILT WITH NETWORKX + PYDECK + STREAMLIT"
    "</div>",
    unsafe_allow_html=True
)
