#!/usr/bin/env python3
"""
Turn results.csv into saturation curves — a self-contained, theme-aware HTML
report with inline SVG (no dependencies, no external assets).

  python3 runner/plot.py [results/results.csv] [-o results/report.html]

Curves (x = offered load in connections, one line per server, median over trials):
  - throughput (moves/s processed; ideal = conns x rate)
  - p99 latency, p50 latency
  - peak RSS under load
  - CPU cores used

Charts render client-side from embedded data: click a server in the legend to
hide it. Hiding rescales every y-axis to the remaining servers, so one blown-out
result can't flatten the curves you actually care about.

Identity is never color-alone: every line is direct-labelled and a full data
table is included (the palette's low-contrast light slots require this relief).
"""

import argparse
import csv
import json
import math
import os
import statistics
from collections import defaultdict

# Fixed entity->slot mapping (color follows the server, never its rank).
SLOT = {"go": 1, "rust": 2, "ocaml": 3, "elixir": 4, "python": 5}
ORDER = ["go", "rust", "ocaml", "elixir", "python"]

# Charts: (csv_field, title, y-axis label, python formatter, js format spec).
# The js spec {"d": decimals, "c": thousands-comma} mirrors the python formatter
# so client-side axis ticks / tooltips render identically.
CHARTS = [
    ("moves_per_s", "Throughput (processed)", "moves / s", lambda v: f"{v:,.0f}", {"d": 0, "c": True}),
    ("p99_ms", "Latency p99", "ms", lambda v: f"{v:.1f}", {"d": 1, "c": False}),
    ("p50_ms", "Latency p50", "ms", lambda v: f"{v:.1f}", {"d": 1, "c": False}),
    ("p99_worst_1s_ms", "Worst 1 s p99", "ms", lambda v: f"{v:.1f}", {"d": 1, "c": False}),
    ("p99_cv", "Latency stability (p99 CV)", "coeff. of variation", lambda v: f"{v:.2f}", {"d": 2, "c": False}),
    ("rss_peak_mb", "Peak memory under load", "MB (RSS)", lambda v: f"{v:.1f}", {"d": 1, "c": False}),
    ("cpu_cores", "CPU cores used", "cores", lambda v: f"{v:.2f}", {"d": 2, "c": False}),
    ("client_cpu_cores", "Client CPU (loadgen validity)", "cores", lambda v: f"{v:.2f}", {"d": 2, "c": False}),
    ("send_rate_pct", "Send-rate fidelity (loadgen validity)", "% of target", lambda v: f"{v:.1f}", {"d": 1, "c": False}),
]

# Plot geometry (shared with the client-side renderer via GEOM).
W, H = 900, 360
ML, MR, MT, MB = 72, 128, 40, 52
GEOM = {"W": W, "H": H, "ML": ML, "MR": MR, "MT": MT, "MB": MB}


def load(path):
    # rows[field][server] -> {conns: [values across trials]}
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    servers, conns = set(), set()
    with open(path) as f:
        for row in csv.DictReader(f):
            s, c = row["server"], int(row["conns"])
            servers.add(s)
            conns.add(c)
            for field, *_ in CHARTS:
                try:
                    data[field][s][c].append(float(row[field]))
                except (KeyError, ValueError):
                    pass
    present = [s for s in ORDER if s in servers] + sorted(servers - set(ORDER))
    return data, present, sorted(conns)


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def series_json(data, servers, all_conns):
    """field -> server -> [[conns, median], ...] (medians precomputed)."""
    out = {}
    for field, *_ in CHARTS:
        by_server = {}
        for s in servers:
            pts = []
            for c in all_conns:
                vals = data[field].get(s, {}).get(c)
                if vals:
                    pts.append([c, statistics.median(vals)])
            if pts:
                by_server[s] = pts
        out[field] = by_server
    return out


def table_html(data, servers, all_conns):
    fields = [c[0] for c in CHARTS]
    fmts = {c[0]: c[3] for c in CHARTS}
    heads = "".join(f"<th>{esc(c[1])}</th>" for c in CHARTS)
    rows = []
    for s in servers:
        for c in all_conns:
            cells = []
            any_val = False
            for f in fields:
                vals = data[f].get(s, {}).get(c)
                if vals:
                    any_val = True
                    cells.append(f"<td>{fmts[f](statistics.median(vals))}</td>")
                else:
                    cells.append("<td>–</td>")
            if any_val:
                dot = f'<span class="sw" style="background:var(--series-{SLOT.get(s,1)})"></span>'
                rows.append(f'<tr data-server="{esc(s)}"><td>{dot}{esc(s)}</td>'
                            f"<td>{c:,}</td>{''.join(cells)}</tr>")
    return (f'<table><thead><tr><th>server</th><th>conns</th>{heads}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody></table>')


def legend_html(servers):
    items = "".join(
        f'<button type="button" class="leg" data-server="{esc(s)}" aria-pressed="true">'
        f'<span class="sw" style="background:var(--series-{SLOT.get(s,1)})"></span>'
        f'<span class="leg-name">{esc(s)}</span></button>'
        for s in servers)
    return (f'<div class="legend" id="legend">{items}'
            f'<button type="button" class="leg leg-all" id="show-all" hidden>show all</button></div>')


CSS = """
:root{
  --surface-1:#fcfcfb; --page:#f9f9f7; --ink:#0b0b0b; --ink2:#52514e;
  --muted:#898781; --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,.10);
  --series-1:#2a78d6; --series-2:#1baf7a; --series-3:#eda100; --series-4:#008300; --series-5:#4a3aa7;
}
@media (prefers-color-scheme:dark){:root{
  --surface-1:#1a1a19; --page:#0d0d0d; --ink:#fff; --ink2:#c3c2b7;
  --muted:#898781; --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,.10);
  --series-1:#3987e5; --series-2:#199e70; --series-3:#c98500; --series-4:#008300; --series-5:#9085e9;
}}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);
  font-family:system-ui,-apple-system,"Segoe UI",sans-serif;line-height:1.5}
.wrap{max-width:960px;margin:0 auto;padding:32px 20px 64px}
h1{font-size:22px;margin:0 0 4px} .sub{color:var(--ink2);margin:0 0 24px;font-size:14px}
.card{background:var(--surface-1);border:1px solid var(--border);border-radius:12px;
  padding:16px 12px;margin:0 0 20px;overflow-x:auto}
.chart{display:block;width:100%;height:auto;min-width:560px}
.c-title{fill:var(--ink);font-size:15px;font-weight:600}
.grid{stroke:var(--grid);stroke-width:1}
.baseline{stroke:var(--axis);stroke-width:1}
.tick{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.tick-y{text-anchor:end} .tick-x{text-anchor:middle}
.axis-label{fill:var(--ink2);font-size:12px;text-anchor:middle}
.direct-label{fill:var(--ink2);font-size:12px;font-weight:500}
.legend{display:flex;gap:10px;flex-wrap:wrap;margin:0 0 20px;align-items:center}
.leg{display:inline-flex;align-items:center;gap:6px;color:var(--ink2);font-size:13px;
  background:transparent;border:1px solid var(--border);border-radius:999px;
  padding:4px 12px 4px 8px;cursor:pointer;font-family:inherit;line-height:1;
  transition:opacity .12s,border-color .12s}
.leg:hover{border-color:var(--axis)}
.leg[aria-pressed="false"]{opacity:.42}
.leg[aria-pressed="false"] .leg-name{text-decoration:line-through}
.leg[aria-pressed="false"] .sw{opacity:.5}
.leg-all{color:var(--ink2);padding:4px 12px}
.sw{width:11px;height:11px;border-radius:3px;flex:none}
tr.dim{opacity:.32}
table{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
th,td{padding:6px 10px;text-align:right;border-bottom:1px solid var(--grid)}
th:first-child,td:first-child{text-align:left} th{color:var(--ink2);font-weight:600}
th:nth-child(2),td:nth-child(2){text-align:right}
td .sw{display:inline-block;margin-right:6px;vertical-align:middle}
details{margin-top:8px} summary{cursor:pointer;color:var(--ink2);font-size:14px;padding:8px 0}
.noscript{color:var(--ink2);font-size:13px;margin:0 0 16px}
.tabs{display:flex;gap:2px;border-bottom:1px solid var(--border);margin:0 0 24px}
.tab{background:transparent;border:0;border-bottom:2px solid transparent;color:var(--ink2);
  font:inherit;font-size:14px;padding:9px 16px;margin-bottom:-1px;cursor:pointer;
  transition:color .12s,border-color .12s}
.tab:hover{color:var(--ink)}
.tab[aria-selected="true"]{color:var(--ink);border-bottom-color:var(--series-1);font-weight:600}
[role="tabpanel"][hidden]{display:none}
.card-meta{color:var(--muted);font-size:12px;margin:0 4px 2px;text-align:right}
.plegend{display:flex;gap:14px;flex-wrap:wrap;margin:0 0 20px;align-items:center}
.pleg{display:inline-flex;align-items:center;gap:6px;color:var(--ink2);font-size:13px}
"""

# Client-side renderer. Mirrors the former server-side SVG builder, but driven by
# a `hidden` set so hiding a server rescales every axis to what remains.
JS = r"""
(function(){
  var CFG = __CFG__;
  var G = CFG.geom;
  var X0 = G.ML, X1 = G.W - G.MR, Y0 = G.H - G.MB, Y1 = G.MT;
  var conns = CFG.conns;
  var xi = {}; conns.forEach(function(c,i){ xi[c] = i; });
  var hidden = Object.create(null);

  function esc(s){ return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function niceMax(v){
    if(v<=0) return 1;
    var exp = Math.floor(Math.log10(v));
    var f = v / Math.pow(10,exp);
    var nf = f<=1?1 : f<=2?2 : f<=5?5 : 10;
    return nf * Math.pow(10,exp);
  }
  function fmt(v, spec){
    var s = Number(v).toFixed(spec.d);
    if(spec.c){
      var p = s.split(".");
      p[0] = p[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      s = p.join(".");
    }
    return s;
  }
  function xpos(i,n){ return n<=1 ? X0 : X0 + (X1-X0)*i/(n-1); }

  function render(chart){
    var svg = [];
    var n = conns.length;
    var byServer = CFG.series[chart.field] || {};
    var spec = chart.spec, ylabel = chart.ylabel;

    // visible series + shared max
    var vmax = 0, visible = [];
    CFG.servers.forEach(function(s){
      if(hidden[s]) return;
      var pts = byServer[s];
      if(!pts || !pts.length) return;
      visible.push(s);
      pts.forEach(function(p){ if(p[1] > vmax) vmax = p[1]; });
    });
    var ymax = vmax > 0 ? niceMax(vmax) : 1;

    function X(c){ return xpos(xi[c], n); }
    function Y(v){ return Y0 - (Y0 - Y1) * (v / ymax); }

    svg.push('<svg viewBox="0 0 '+G.W+' '+G.H+'" class="chart" role="img" aria-label="'
      + esc(chart.title) + '" preserveAspectRatio="xMidYMid meet">');
    svg.push('<text x="'+G.ML+'" y="24" class="c-title">'+esc(chart.title)+'</text>');

    for(var k=0;k<=5;k++){
      var v = ymax*k/5, y = Y(v);
      svg.push('<line x1="'+X0+'" y1="'+y.toFixed(1)+'" x2="'+X1+'" y2="'+y.toFixed(1)+'" class="grid"/>');
      svg.push('<text x="'+(X0-10)+'" y="'+(y+4).toFixed(1)+'" class="tick tick-y">'+fmt(v,spec)+'</text>');
    }
    svg.push('<text class="axis-label" transform="translate(18,'+((Y0+Y1)/2)+') rotate(-90)">'+esc(ylabel)+'</text>');

    conns.forEach(function(c){
      svg.push('<text x="'+X(c).toFixed(1)+'" y="'+(Y0+22)+'" class="tick tick-x">'+fmt(c,{d:0,c:true})+'</text>');
    });
    svg.push('<text x="'+((X0+X1)/2)+'" y="'+(G.H-8)+'" class="axis-label">connections (offered load)</text>');
    svg.push('<line x1="'+X0+'" y1="'+Y0+'" x2="'+X1+'" y2="'+Y0+'" class="baseline"/>');

    visible.forEach(function(s){
      var col = "var(--series-" + (CFG.slot[s]||1) + ")";
      var pts = byServer[s];
      var d = pts.map(function(p,i){ return (i===0?"M":"L") + X(p[0]).toFixed(1) + "," + Y(p[1]).toFixed(1); }).join(" ");
      svg.push('<path d="'+d+'" fill="none" stroke="'+col+'" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>');
      pts.forEach(function(p){
        svg.push('<circle cx="'+X(p[0]).toFixed(1)+'" cy="'+Y(p[1]).toFixed(1)+'" r="4" fill="'+col
          +'" stroke="var(--surface-1)" stroke-width="1.5"><title>'+esc(s)+' @ '+fmt(p[0],{d:0,c:true})
          +' conns: '+fmt(p[1],spec)+' '+esc(ylabel)+'</title></circle>');
      });
      var last = pts[pts.length-1], ly = Y(last[1]);
      svg.push('<circle cx="'+(X1+14)+'" cy="'+ly.toFixed(1)+'" r="4" fill="'+col+'"/>');
      svg.push('<text x="'+(X1+24)+'" y="'+(ly+4).toFixed(1)+'" class="direct-label">'+esc(s)+'</text>');
    });

    svg.push('</svg>');
    return svg.join("");
  }

  function renderAll(){
    CFG.charts.forEach(function(chart, i){
      var el = document.getElementById("chart-"+i);
      if(el) el.innerHTML = render(chart);
    });
  }

  function syncTable(){
    var rows = document.querySelectorAll("tbody tr[data-server]");
    for(var i=0;i<rows.length;i++){
      rows[i].classList.toggle("dim", !!hidden[rows[i].getAttribute("data-server")]);
    }
  }

  function refresh(){
    var btns = document.querySelectorAll(".leg[data-server]");
    for(var i=0;i<btns.length;i++){
      var srv = btns[i].getAttribute("data-server");
      btns[i].setAttribute("aria-pressed", hidden[srv] ? "false" : "true");
    }
    var showAll = document.getElementById("show-all");
    if(showAll) showAll.hidden = Object.keys(hidden).length === 0;
    renderAll();
    syncTable();
  }

  var legend = document.getElementById("legend");
  if(legend){
    legend.addEventListener("click", function(e){
      var btn = e.target.closest ? e.target.closest(".leg") : null;
      if(!btn) return;
      if(btn.id === "show-all"){ hidden = Object.create(null); refresh(); return; }
      var s = btn.getAttribute("data-server");
      if(!s) return;
      if(hidden[s]) delete hidden[s]; else hidden[s] = true;
      refresh();
    });
  }

  renderAll();
})();
"""


# ---- latency-over-time (timeline.csv) --------------------------------------
# One card per (server, load level): the full latency spread as separate lines
# over time, so you see typical AND tail latency and whether either drifts or
# spikes across the run. Static inline SVG (no interactivity needed here).

# Percentile -> label -> colour, ramped green(typical) -> red(worst tail). Fixed
# colours (not the server palette): each card is a single server, so these
# encode the percentile, and they read on both light and dark backgrounds.
TL_SERIES = [
    ("p50_ms", "p50", "#2a9d5c"),
    ("p90_ms", "p90", "#8aa32e"),
    ("p99_ms", "p99", "#eda100"),
    ("p999_ms", "p99.9", "#e8663d"),
    ("max_ms", "max", "#d6453f"),
]


def nice_max(v):
    if v <= 0:
        return 1
    exp = math.floor(math.log10(v))
    f = v / 10 ** exp
    nf = 1 if f <= 1 else 2 if f <= 2 else 5 if f <= 5 else 10
    return nf * 10 ** exp


def load_timeline(path):
    """(server, conns) -> trial -> [rows sorted by t_s]."""
    data = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        for row in csv.DictReader(f):
            data[(row["server"], int(row["conns"]))][int(row["trial"])].append(row)
    for key in data:
        for tr in data[key]:
            data[key][tr].sort(key=lambda r: int(r["t_s"]))
    return data


def timeline_svg(server, conns, rows):
    X0, X1, Y0, Y1 = ML, W - MR, H - MB, MT
    maxT = max((int(r["t_s"]) for r in rows), default=0)
    vmax = 0.0
    for r in rows:
        for field, _, _ in TL_SERIES:
            try:
                vmax = max(vmax, float(r[field]))
            except (KeyError, ValueError):
                pass
    ymax = nice_max(vmax) if vmax > 0 else 1

    def X(t):
        return X0 if maxT <= 0 else X0 + (X1 - X0) * t / maxT

    def Y(v):
        return Y0 - (Y0 - Y1) * (v / ymax)

    svg = [f'<svg viewBox="0 0 {W} {H}" class="chart" role="img" '
           f'aria-label="{esc(server)} latency over time" preserveAspectRatio="xMidYMid meet">']
    svg.append(f'<text x="{ML}" y="24" class="c-title">{esc(server)} @ {conns:,} conns</text>')
    for k in range(6):
        v = ymax * k / 5
        y = Y(v)
        svg.append(f'<line x1="{X0}" y1="{y:.1f}" x2="{X1}" y2="{y:.1f}" class="grid"/>')
        svg.append(f'<text x="{X0 - 10}" y="{y + 4:.1f}" class="tick tick-y">{v:.1f}</text>')
    svg.append(f'<text class="axis-label" transform="translate(18,{(Y0 + Y1) / 2}) rotate(-90)">ms</text>')
    step = max(1, -(-maxT // 6))
    t = 0
    while t <= maxT:
        svg.append(f'<text x="{X(t):.1f}" y="{Y0 + 22}" class="tick tick-x">{t}</text>')
        t += step
    svg.append(f'<text x="{(X0 + X1) / 2}" y="{H - 8}" class="axis-label">time (s, measured phase)</text>')
    svg.append(f'<line x1="{X0}" y1="{Y0}" x2="{X1}" y2="{Y0}" class="baseline"/>')
    for field, label, color in TL_SERIES:
        pts = []
        for r in rows:
            try:
                pts.append((int(r["t_s"]), float(r[field])))
            except (KeyError, ValueError):
                pass
        if not pts:
            continue
        d = " ".join(("M" if i == 0 else "L") + f"{X(t):.1f},{Y(v):.1f}"
                     for i, (t, v) in enumerate(pts))
        svg.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2" '
                   f'stroke-linejoin="round" stroke-linecap="round"/>')
        ly = Y(pts[-1][1])
        svg.append(f'<text x="{X1 + 8}" y="{ly + 4:.1f}" class="direct-label" '
                   f'fill="{color}">{esc(label)}</text>')
    svg.append("</svg>")
    return "".join(svg)


def timeline_body(tl_data, src):
    """Latency-over-time tab content (no <html> wrapper — embedded in the page)."""
    if not tl_data:
        return ('<p class="sub">No timeline data yet. Run <code>runner/run.py</code> '
                'to generate <code>timeline.csv</code> next to the results CSV.</p>')
    order = ORDER + sorted({k[0] for k in tl_data} - set(ORDER))
    keys = sorted(tl_data, key=lambda k: (order.index(k[0]) if k[0] in order else 999, k[1]))
    cards = []
    for server, conns in keys:
        trials = tl_data[(server, conns)]
        tr = min(trials)  # lowest trial number, deterministic
        rows = trials[tr]
        if not rows:
            continue
        cards.append(f'<div class="card"><div class="card-meta">trial {tr}</div>'
                     f'{timeline_svg(server, conns, rows)}</div>')
    legend = "".join(f'<span class="pleg"><span class="sw" style="background:{c}"></span>'
                     f'{esc(l)}</span>' for _, l, c in TL_SERIES)
    return (f'<p class="sub">one card per server × load level · x = seconds into the '
            f'measured phase · 1 s windows · source: {esc(os.path.basename(src))}</p>'
            f'<div class="plegend">{legend}</div>{"".join(cards)}')


# Tab switcher: show the selected panel, hide the rest. Progressive enhancement —
# without JS the saturation panel stays visible (its data table is complete).
TAB_JS = r"""
(function(){
  var tabs = [].slice.call(document.querySelectorAll('.tab'));
  function sel(id){
    tabs.forEach(function(t){
      var on = t.id === id;
      t.setAttribute('aria-selected', on ? 'true' : 'false');
      var p = document.getElementById(t.getAttribute('aria-controls'));
      if(p) p.hidden = !on;
    });
  }
  tabs.forEach(function(t){ t.addEventListener('click', function(){ sel(t.id); }); });
})();
"""


def saturation_body(data, servers, all_conns, src):
    cards = "".join(f'<div class="card"><div id="chart-{i}"></div></div>'
                    for i in range(len(CHARTS)))
    return (f'<p class="sub">median over trials · x = offered load (connections) · '
            f'source: {esc(os.path.basename(src))}</p>'
            f'<p class="sub" style="margin-top:-16px">Click a server to hide it — '
            f"the axes rescale to what's left.</p>"
            f'<noscript><p class="noscript">This tab needs JavaScript to draw the charts. '
            f'The data table below is complete.</p></noscript>'
            f'{legend_html(servers)}{cards}'
            f'<details open><summary>Data table</summary>'
            f'<div class="card">{table_html(data,servers,all_conns)}</div></details>')


def build_html(data, servers, all_conns, src, tl_data=None, tl_src=None):
    cfg = {
        "geom": GEOM,
        "conns": all_conns,
        "servers": servers,
        "slot": {s: SLOT.get(s, 1) for s in servers},
        "series": series_json(data, servers, all_conns),
        "charts": [{"field": f, "title": t, "ylabel": y, "spec": spec}
                   for f, t, y, _fmt, spec in CHARTS],
    }
    js = JS.replace("__CFG__", json.dumps(cfg))
    sat = saturation_body(data, servers, all_conns, src)
    tl = timeline_body(tl_data, tl_src or src)
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>game-bench — report</title><style>{CSS}</style></head>
<body><div class="wrap">
<h1>game-bench</h1>
<div class="tabs" role="tablist" aria-label="Views">
<button class="tab" role="tab" id="tab-sat" aria-controls="panel-sat" aria-selected="true">Saturation curves</button>
<button class="tab" role="tab" id="tab-tl" aria-controls="panel-tl" aria-selected="false">Latency over time</button>
</div>
<section id="panel-sat" role="tabpanel" aria-labelledby="tab-sat">{sat}</section>
<section id="panel-tl" role="tabpanel" aria-labelledby="tab-tl" hidden>{tl}</section>
</div>
<script>{js}</script>
<script>{TAB_JS}</script>
</body></html>"""


def main():
    ap = argparse.ArgumentParser()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("csv", nargs="?", default=os.path.join(root, "results", "results.csv"))
    ap.add_argument("-o", "--out", default=os.path.join(root, "results", "report.html"))
    args = ap.parse_args()

    data, servers, all_conns = load(args.csv)
    if not servers or not all_conns:
        raise SystemExit(f"no data in {args.csv}")

    # A timeline.csv next to the summary CSV becomes the "Latency over time" tab.
    tl_csv = os.path.join(os.path.dirname(os.path.abspath(args.csv)), "timeline.csv")
    tl_data = load_timeline(tl_csv) if os.path.exists(tl_csv) else None

    html = build_html(data, servers, all_conns, args.csv, tl_data, tl_csv)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        f.write(html)
    n_tl = len(tl_data) if tl_data else 0
    print(f"wrote {args.out}  ({len(servers)} servers, {len(all_conns)} load levels, "
          f"{n_tl} run timelines)")


if __name__ == "__main__":
    main()
