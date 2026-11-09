# -*- coding: utf-8 -*-
"""프리셋 성과 수치 자동 갱신 — 읽기 측 (Streamlit 비의존).

흐름
----
scripts/update_preset_stats.py  (GitHub Actions, 평일 10:00 KST)
    → 각 전략 엔진으로 프리셋 전체를 최신 종가까지 재계산
    → preset_stats.json (저장소 루트) 커밋 → Streamlit 재배포
strategies/*.py
    → 모듈 로드 시 apply()/apply_db() 로 프리셋 help 의 수치 줄을 최신값으로 교체

안전장치
--------
- 항목은 '전략/티커/라벨' 로 찾되, 저장된 파라미터 서명(sig)이 현재 프리셋과
  다르면 무시한다 → 프리셋 값을 바꿨는데 옛 수치가 붙는 일이 없다.
- 파일이 없거나 항목이 없으면 help 를 건드리지 않는다 (코드에 적힌 문구 그대로).
"""
from __future__ import annotations

import json
import os

STATS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "preset_stats.json")

_SKIP_KEYS = {"label", "help", "ticker"}
_cache = {"mtime": None, "data": {}}


def param_sig(preset: dict) -> str:
    """프리셋 파라미터 서명 — label/help 를 뺀 값들의 정규화 문자열."""
    parts = []
    for k in sorted(preset):
        if k in _SKIP_KEYS:
            continue
        v = preset[k]
        if isinstance(v, bool):
            v = int(v)
        if isinstance(v, (int, float)):
            v = repr(round(float(v), 6))
        elif isinstance(v, (list, tuple, dict)):
            v = json.dumps(v, ensure_ascii=False, sort_keys=True)
        else:
            v = str(v).replace(" ", "")
        parts.append(f"{k}={v}")
    return "|".join(parts)


def item_key(strategy: str, ticker: str, label: str) -> str:
    return f"{strategy}/{ticker}/{label}"


def load() -> dict:
    """preset_stats.json 로드 (mtime 캐시). 실패 시 빈 dict."""
    try:
        mt = os.path.getmtime(STATS_PATH)
    except OSError:
        return {}
    if _cache["mtime"] != mt:
        try:
            with open(STATS_PATH, encoding="utf-8") as f:
                _cache["data"] = json.load(f)
            _cache["mtime"] = mt
        except Exception:
            return {}
    return _cache["data"]


def get(strategy: str, ticker: str, preset: dict):
    """프리셋의 최신 성과 dict (없거나 파라미터가 달라졌으면 None)."""
    it = (load().get("items") or {}).get(item_key(strategy, ticker, preset.get("label", "")))
    if not it or it.get("sig") != param_sig(preset):
        return None
    return it


def stat_line(it: dict) -> str:
    return (f"CAGR {it['cagr'] * 100:.2f}%  |  MDD {it['mdd'] * 100:.2f}%  |  "
            f"Calmar {it['calmar']:.2f}")


def basis_line(it: dict) -> str:
    s = f"{str(it.get('start', ''))[:7]} ~ {it.get('end', '')} 기준"
    if it.get("win") is not None:
        s += f" · 승률 {it['win'] * 100:.0f}%"
    if it.get("note"):
        s += f" · {it['note']}"
    return s + " · 평일 자동 갱신"


def apply(strategy: str, ticker: str, presets: list) -> None:
    """프리셋 리스트의 help 에서 수치 줄(CAGR·MDD 포함 줄)을 최신값으로 교체 (제자리 수정)."""
    for p in presets or []:
        try:
            it = get(strategy, ticker, p)
            # 최신값이 없어도 코드에 적힌 옛 수치는 보여주지 않는다 (낡은 숫자 표시 금지).
            fresh = ([stat_line(it), basis_line(it)] if it
                     else ["성과 수치: 다음 자동 갱신 때 표시됩니다"])
            out, done = [], False
            for ln in str(p.get("help", "")).split("\n"):
                if "CAGR" in ln and "MDD" in ln:
                    if not done:
                        out += fresh
                        done = True
                    continue                      # 옛 수치 줄은 버림
                if "자동 갱신" in ln:
                    continue                      # 재적용 시 중복 방지
                out.append(ln)
            if not done:
                out = fresh + out
            p["help"] = "\n".join(x for x in out if x != "")
        except Exception:
            continue


def apply_db(strategy: str, db: dict) -> None:
    """{티커: [프리셋,...]} 형태의 DB 전체에 apply."""
    for tk, presets in (db or {}).items():
        apply(strategy, tk, presets)
