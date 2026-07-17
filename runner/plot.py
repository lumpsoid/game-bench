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

Identity is never color-alone: every line is direct-labelled and a full data
table is included (the palette's low-contrast light slots require this relief).
"""

import argparse
import csv
import math
import os
import statistics
from collections import defaultdict

# Fixed entity->slot mapping (color follows the server, never its rank).
SLOT = {"go": 1, "rust": 2, "ocaml": 3, "elixir": 4, "python": 5}
ORDER = ["go", "rust", "ocaml", "elixir", "python"]

# Charts: (csv_field, title, y-axis label, value formatter)
CHARTS = [
    ("moves_per_s", "Throughput (processed)", "moves / s", lambda v: f"{v:,.0f}"),
    ("p99_ms", "Latency p99", "ms", lambda v: f"{v:.1f}"),
    ("p50_ms", "Latency p50", "ms", lambda v: f"{v:.1f}"),
    ("rss_peak_mb", "Peak memory under load", "MB (RSS)", lambda v: f"{v:.1f}"),
    ("cpu_cores", "CPU cores used", "cores", lambda v: f"{v:.2f}"),
]

# Plot geometry
W, H = 900, 360
ML, MR, MT, MB = 72, 128, 40, 52
X0, X1 = ML, W - MR
Y0, Y1 = H - MB, MT


def nice_max(v):
    if v <= 0:
        return 1.0
    exp = math.floor(math.log10(v))
    f = v / 10 ** exp
    nf = 1 if f <= 1 else 2 if f <= 2 else 5 if f <= 5 else 10
    return nf * 10 ** exp


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


def xpos(i, n):
    return X0 if n <= 1 else X0 + (X1 - X0) * i / (n - 1)


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def chart_svg(field, title, ylabel, fmt, data, servers, all_conns):
    n = len(all_conns)
    xi = {c: i for i, c in enumerate(all_conns)}

    series = {}
    vmax = 0.0
    for s in servers:
        pts = []
        for c in all_conns:
            vals = data[field].get(s, {}).get(c)
            if vals:
                m = statistics.median(vals)
                pts.append((c, m))
                vmax = max(vmax, m)
        if pts:
            series[s] = pts
    ymax = nice_max(vmax) if vmax > 0 else 1.0

    def X(c):
        return xpos(xi[c], n)

    def Y(v):
        return Y0 - (Y0 - Y1) * (v / ymax)

    parts = [f'<svg viewBox="0 0 {W} {H}" class="chart" role="img" '
             f'aria-label="{esc(title)}" preserveAspectRatio="xMidYMid meet">']
    parts.append(f'<text x="{ML}" y="24" class="c-title">{esc(title)}</text>')

    # y gridlines + ticks (5 steps)
    for k in range(6):
        v = ymax * k / 5
        y = Y(v)
        parts.append(f'<line x1="{X0}" y1="{y:.1f}" x2="{X1}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{X0-10}" y="{y+4:.1f}" class="tick tick-y">{fmt(v)}</text>')
    parts.append(f'<text class="axis-label" transform="translate(18,{(Y0+Y1)/2}) rotate(-90)">{esc(ylabel)}</text>')

    # x ticks
    for c in all_conns:
        x = X(c)
        parts.append(f'<text x="{x:.1f}" y="{Y0+22}" class="tick tick-x">{c:,}</text>')
    parts.append(f'<text x="{(X0+X1)/2}" y="{H-8}" class="axis-label">connections (offered load)</text>')

    # baseline
    parts.append(f'<line x1="{X0}" y1="{Y0}" x2="{X1}" y2="{Y0}" class="baseline"/>')

    # series lines + markers + direct labels
    for s in servers:
        if s not in series:
            continue
        slot = SLOT.get(s, 1)
        col = f"var(--series-{slot})"
        pts = series[s]
        d = " ".join(f"{'M' if i==0 else 'L'}{X(c):.1f},{Y(v):.1f}" for i, (c, v) in enumerate(pts))
        parts.append(f'<path d="{d}" fill="none" stroke="{col}" stroke-width="2" '
                     f'stroke-linejoin="round" stroke-linecap="round"/>')
        for c, v in pts:
            parts.append(f'<circle cx="{X(c):.1f}" cy="{Y(v):.1f}" r="4" fill="{col}" '
                         f'stroke="var(--surface-1)" stroke-width="1.5">'
                         f'<title>{esc(s)} @ {c:,} conns: {fmt(v)} {esc(ylabel)}</title></circle>')
        # direct label: colored dot + server name in ink, at the last point
        lc, lv = pts[-1]
        ly = Y(lv)
        parts.append(f'<circle cx="{X1+14}" cy="{ly:.1f}" r="4" fill="{col}"/>')
        parts.append(f'<text x="{X1+24}" y="{ly+4:.1f}" class="direct-label">{esc(s)}</text>')

    parts.append("</svg>")
    return "".join(parts)


def table_html(data, servers, all_conns):
    fields = [f for f, *_ in CHARTS]
    fmts = {f: fmt for f, _, _, fmt in CHARTS}
    heads = "".join(f"<th>{esc(t)}</th>" for _, t, _, _ in CHARTS)
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
                rows.append(f"<tr><td>{dot}{esc(s)}</td><td>{c:,}</td>{''.join(cells)}</tr>")
    return (f'<table><thead><tr><th>server</th><th>conns</th>{heads}</tr></thead>'
            f'<tbody>{"".join(rows)}</tbody></table>')


def legend_html(servers):
    items = "".join(
        f'<span class="leg"><span class="sw" style="background:var(--series-{SLOT.get(s,1)})"></span>{esc(s)}</span>'
        for s in servers)
    return f'<div class="legend">{items}</div>'


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
.legend{display:flex;gap:16px;flex-wrap:wrap;margin:0 0 20px}
.leg,.sw{display:inline-flex;align-items:center}
.leg{gap:6px;color:var(--ink2);font-size:13px}
.sw{width:11px;height:11px;border-radius:3px;margin-right:6px;vertical-align:middle}
table{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
th,td{padding:6px 10px;text-align:right;border-bottom:1px solid var(--grid)}
th:first-child,td:first-child{text-align:left} th{color:var(--ink2);font-weight:600}
th:nth-child(2),td:nth-child(2){text-align:right}
details{margin-top:8px} summary{cursor:pointer;color:var(--ink2);font-size:14px;padding:8px 0}
"""


def build_html(data, servers, all_conns, src):
    charts = "".join(f'<div class="card">{chart_svg(f,t,y,fmt,data,servers,all_conns)}</div>'
                     for f, t, y, fmt in CHARTS)
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>game-bench — saturation curves</title><style>{CSS}</style></head>
<body><div class="wrap">
<h1>Saturation curves</h1>
<p class="sub">median over trials · x = offered load (connections) · source: {esc(os.path.basename(src))}</p>
{legend_html(servers)}
{charts}
<details open><summary>Data table</summary><div class="card">{table_html(data,servers,all_conns)}</div></details>
</div></body></html>"""


def main():
    ap = argparse.ArgumentParser()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("csv", nargs="?", default=os.path.join(root, "results", "results.csv"))
    ap.add_argument("-o", "--out", default=os.path.join(root, "results", "report.html"))
    args = ap.parse_args()

    data, servers, all_conns = load(args.csv)
    if not servers or not all_conns:
        raise SystemExit(f"no data in {args.csv}")
    html = build_html(data, servers, all_conns, args.csv)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        f.write(html)
    print(f"wrote {args.out}  ({len(servers)} servers, {len(all_conns)} load levels)")


if __name__ == "__main__":
    main()
