# -*- coding: utf-8 -*-
"""리포트 공용 SVG 라인차트 — 테마 토큰(var(--...))을 그대로 쓰므로 다크/라이트 자동 대응."""


def line_chart(series, ylab="Calmar", xlab="", xticks=None,
               W=760, H=330, pad=(52, 16, 40, 56), mark_max=True):
    """series = [(label, [(x, y), ...], color)]

    xticks : 표시할 x 눈금 값 리스트 (None 이면 0~100 을 20 간격)
    """
    t, r, b, l = pad
    iw, ih = W - l - r, H - t - b
    xs = [x for _, pts, _ in series for x, _ in pts]
    ys = [y for _, pts, _ in series for _, y in pts]
    x0, x1 = min(xs), max(xs)
    if x1 == x0:
        x1 = x0 + 1
    lo, hi = min(ys), max(ys)
    m = (hi - lo) * 0.12 or 0.1
    lo, hi = lo - m, hi + m
    X = lambda v: l + iw * (v - x0) / (x1 - x0)
    Y = lambda v: t + ih * (1 - (v - lo) / (hi - lo))
    if xticks is None:
        xticks = list(range(0, 101, 20))

    g = [f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" '
         f'style="display:block;overflow:visible">']
    for i in range(5):
        v = lo + (hi - lo) * i / 4
        y = Y(v)
        g.append(f'<line x1="{l}" y1="{y:.1f}" x2="{l+iw}" y2="{y:.1f}" '
                 f'stroke="var(--rule)" stroke-width="1"/>')
        g.append(f'<text x="{l-10}" y="{y+4:.1f}" text-anchor="end" font-size="11" '
                 f'fill="var(--faint)" font-family="IBM Plex Mono,monospace">'
                 f'{v:.2f}</text>')
    for v in xticks:
        if not (x0 <= v <= x1):
            continue
        g.append(f'<text x="{X(v):.1f}" y="{t+ih+22}" text-anchor="middle" '
                 f'font-size="11" fill="var(--faint)" '
                 f'font-family="IBM Plex Mono,monospace">{v:g}%</text>')
    if xlab:
        g.append(f'<text x="{l+iw/2:.0f}" y="{H-4}" text-anchor="middle" '
                 f'font-size="11" fill="var(--muted)">{xlab}</text>')
    g.append(f'<text x="14" y="{t-6}" font-size="11" fill="var(--muted)">{ylab}</text>')

    for lab, pts, col in series:
        pts = sorted(pts)
        d = " ".join(("M" if i == 0 else "L") + f"{X(x):.1f},{Y(y):.1f}"
                     for i, (x, y) in enumerate(pts))
        g.append(f'<path d="{d}" fill="none" stroke="{col}" stroke-width="2" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
        if mark_max:
            bx, by = max(pts, key=lambda p: p[1])
            g.append(f'<circle cx="{X(bx):.1f}" cy="{Y(by):.1f}" r="4" fill="{col}" '
                     f'stroke="var(--surface)" stroke-width="1.5"/>')
    g.append("</svg>")

    key = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:6px;font-size:12px;'
        f'color:var(--muted)"><i style="width:14px;height:2px;background:{c};'
        f'display:inline-block"></i>{lab}</span>' for lab, _, c in series)
    return ("".join(g) + f'<div style="display:flex;flex-wrap:wrap;gap:8px 18px;'
            f'margin-top:14px;justify-content:center">{key}</div>')
