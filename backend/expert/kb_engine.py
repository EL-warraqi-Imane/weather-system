from __future__ import annotations

"""
kb_engine.py

KB validator for climate hazards stored as Turtle (rdflib).

This version supports *proxy / derived* featureKeys that may not exist directly in your dataframe,
but are computable from your base variables + (optional) history.

Supported derived featureKeys (auto-computed if missing):
  - dewpoint_depression (°C): from temperature_2m (°C) + relative_humidity_2m (%)
  - heat_index_f (°F): from temperature_2m (°C) + relative_humidity_2m (%), NWS Rothfusz regression
  - wind_chill_f (°F): from temperature_2m (°C) + wind_speed_10m (m/s), NWS formula
  - precip_1h_mm (mm): from precipitation (mm/hourly step)
  - precip_6h_mm (mm): rolling sum of precipitation over last 6 hours (needs history_rows)
  - soil_moisture_pctile_30d (percentile 0..100): percentile rank of soil_moisture_0_to_7cm over history_rows (ideally 30d)

You can still also define aggregations in the KB itself using:
  ex:agg "SUM" ; ex:windowHours N
and the engine will enforce it.

"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

from rdflib import Graph, Namespace, URIRef
import math


EX = Namespace("http://example.org/climate#")


# -----------------------------
# Helpers: unit conversions
# -----------------------------
def ms_to_mph(x: float) -> float:
    return x * 2.2369362920544

def ms_to_kn(x: float) -> float:
    return x * 1.9438444924406

def cm_to_in(x: float) -> float:
    return x / 2.54

def c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


# -----------------------------
# Reading literals safely
# -----------------------------
def _first(g: Graph, s: URIRef, p: URIRef):
    for o in g.objects(s, p):
        return o
    return None

def _lit_str(g: Graph, s: URIRef, p: URIRef, default: Optional[str] = None) -> Optional[str]:
    o = _first(g, s, p)
    if o is None:
        return default
    return str(o)

def _lit_float(g: Graph, s: URIRef, p: URIRef, default: Optional[float] = None) -> Optional[float]:
    o = _first(g, s, p)
    if o is None:
        return default
    try:
        return float(o.toPython() if hasattr(o, "toPython") else o)
    except Exception:
        return default

def _lit_int(g: Graph, s: URIRef, p: URIRef, default: Optional[int] = None) -> Optional[int]:
    o = _first(g, s, p)
    if o is None:
        return default
    try:
        return int(o.toPython() if hasattr(o, "toPython") else o)
    except Exception:
        return default


# -----------------------------
# Data structures
# -----------------------------
@dataclass
class ConditionEval:
    kb_event: str
    condition: str
    variable: str
    feature_key: Optional[str]
    op: str
    value: Optional[float]
    min: Optional[float]
    max: Optional[float]
    unit: Optional[str]
    windowHours: int
    agg: Optional[str]
    durationHours: int
    duration_ok: Optional[bool]
    fuzzy_score: float
    hard_ok: Optional[bool]
    source: Optional[str]
    note: Optional[str]


@dataclass
class CandidateEval:
    event: str
    probability: float
    threshold: float
    above_threshold: bool
    kb_status: str
    kb_score: float
    why: str
    evidence: Dict[str, Any]


# -----------------------------
# KB load
# -----------------------------
def load_kb(path: str) -> Graph:
    g = Graph()
    g.parse(path, format="turtle")
    return g


# -----------------------------
# Numeric extraction
# -----------------------------
def _get_raw(row: Dict[str, Any], key: Optional[str]) -> Optional[float]:
    if not key:
        return None
    if key not in row:
        return None
    v = row.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


# -----------------------------
# Proxy / derived features
# -----------------------------
def _dew_point_c(temp_c: float, rh_percent: float) -> float:
    """
    Magnus formula dew point approximation.
    temp_c in °C, RH in % (0..100)
    """
    # clamp RH to avoid log(0)
    rh = min(max(float(rh_percent), 1e-6), 100.0)
    t = float(temp_c)
    a, b = 17.27, 237.7
    alpha = (a * t) / (b + t) + math.log(rh / 100.0)
    return (b * alpha) / (a - alpha)

def _compute_dewpoint_depression_c(row: Dict[str, Any]) -> Optional[float]:
    t = _get_raw(row, "temperature_2m")
    rh = _get_raw(row, "relative_humidity_2m")
    if t is None or rh is None:
        return None
    td = _dew_point_c(t, rh)
    return float(t - td)

def _compute_heat_index_f(row: Dict[str, Any]) -> Optional[float]:
    """
    NWS Rothfusz regression. Returns ambient temp if outside main validity range.
    """
    t_c = _get_raw(row, "temperature_2m")
    rh = _get_raw(row, "relative_humidity_2m")
    if t_c is None or rh is None:
        return None

    T = c_to_f(float(t_c))
    RH = float(rh)

    # Outside the typical range, use ambient temperature (common operational handling)
    if not (T >= 80.0 and RH >= 40.0):
        return float(T)

    hi = (-42.379
          + 2.04901523 * T
          + 10.14333127 * RH
          - 0.22475541 * T * RH
          - 0.00683783 * T * T
          - 0.05481717 * RH * RH
          + 0.00122874 * T * T * RH
          + 0.00085282 * T * RH * RH
          - 0.00000199 * T * T * RH * RH)

    # Optional NWS adjustments (kept minimal to avoid surprises)
    return float(hi)

def _compute_wind_chill_f(row: Dict[str, Any]) -> Optional[float]:
    """
    NWS wind chill formula.
    Uses T in °F and wind speed in mph.
    Outside recommended range (T>50F or V<3mph), returns ambient temperature.
    """
    t_c = _get_raw(row, "temperature_2m")
    v_mps = _get_raw(row, "wind_speed_10m")
    if t_c is None or v_mps is None:
        return None

    T = c_to_f(float(t_c))
    V = ms_to_mph(float(v_mps))

    if T > 50.0 or V < 3.0:
        return float(T)

    wc = 35.74 + 0.6215 * T - 35.75 * (V ** 0.16) + 0.4275 * T * (V ** 0.16)
    return float(wc)

def _compute_precip_1h_mm(row: Dict[str, Any]) -> Optional[float]:
    # hourly precipitation step (mm)
    v = _get_raw(row, "precipitation")
    return float(v) if v is not None else None

def _compute_precip_6h_mm(row: Dict[str, Any], history_rows: Optional[List[Dict[str, Any]]]) -> Optional[float]:
    if not history_rows or len(history_rows) < 6:
        return None
    vals = []
    for hrow in history_rows[-6:]:
        v = _get_raw(hrow, "precipitation")
        if v is None:
            return None
        vals.append(float(v))
    return float(sum(vals))

def _compute_soil_moisture_pctile_30d(row: Dict[str, Any], history_rows: Optional[List[Dict[str, Any]]]) -> Optional[float]:
    """
    Percentile rank of the current soil moisture within the provided history_rows.
    If you pass 30 days of hourly data (720 rows), this matches the KB intent.
    """
    cur = _get_raw(row, "soil_moisture_0_to_7cm")
    if cur is None:
        return None
    if not history_rows or len(history_rows) < 10:
        return None

    vals = []
    for hrow in history_rows:
        v = _get_raw(hrow, "soil_moisture_0_to_7cm")
        if v is not None and math.isfinite(float(v)):
            vals.append(float(v))

    if len(vals) < 10:
        return None

    # percentile rank: percentage of historical values <= current
    le = sum(1 for v in vals if v <= float(cur))
    return float(100.0 * le / len(vals))


def _get_feature_value(
    row: Dict[str, Any],
    feature_key: Optional[str],
    history_rows: Optional[List[Dict[str, Any]]],
) -> Optional[float]:
    """
    Tries to get the feature directly from row; if missing, tries to compute a proxy feature.
    """
    if not feature_key:
        return None

    raw = _get_raw(row, feature_key)
    if raw is not None:
        return float(raw)

    fk = feature_key.strip()

    if fk == "dewpoint_depression":
        return _compute_dewpoint_depression_c(row)

    if fk == "heat_index_f":
        return _compute_heat_index_f(row)

    if fk == "wind_chill_f":
        return _compute_wind_chill_f(row)

    if fk == "precip_1h_mm":
        return _compute_precip_1h_mm(row)

    if fk == "precip_6h_mm":
        return _compute_precip_6h_mm(row, history_rows=history_rows)

    if fk == "soil_moisture_pctile_30d":
        return _compute_soil_moisture_pctile_30d(row, history_rows=history_rows)

    return None


# -----------------------------
# Unit conversion (after feature extraction)
# -----------------------------
def _convert_value(feature_key: str, raw: float, target_unit: Optional[str]) -> float:
    """
    Converts from your dataset's assumed units to target_unit.

    Assumptions:
      - wind_speed_10m, wind_gusts_10m are in m/s
      - precipitation is mm (per hour step)
      - snowfall is cm (Open-Meteo commonly uses cm)
      - temperature_2m is °C
      - derived proxies return their own units:
          dewpoint_depression -> °C
          heat_index_f -> °F
          wind_chill_f -> °F
          precip_* -> mm
          soil_moisture_pctile_30d -> percentile 0..100
    """
    if target_unit is None:
        return raw

    u = target_unit.strip().lower()

    # wind in m/s -> mph/kn
    if feature_key in ("wind_speed_10m", "wind_gusts_10m"):
        if u == "mph":
            return ms_to_mph(raw)
        if u == "kn":
            return ms_to_kn(raw)

    # snowfall in cm -> inches
    if feature_key == "snowfall":
        if u in ("in", "inch", "inches"):
            return cm_to_in(raw)

    # otherwise: no conversion
    return raw


# -----------------------------
# Condition evaluation
# -----------------------------
def _fuzzy_ge(x: float, thr: float) -> float:
    if thr <= 0:
        return 1.0
    if x >= thr:
        return 1.0
    return max(0.0, x / thr)

def _fuzzy_le(x: float, thr: float) -> float:
    if thr <= 0:
        return 1.0
    if x <= thr:
        return 1.0
    return max(0.0, thr / x)

def _eval_numeric(op: str, x: float, vmin: Optional[float], vmax: Optional[float]) -> Tuple[Optional[bool], float]:
    op = op.upper()

    if op == "GE":
        if vmin is None:
            return None, 0.0
        ok = x >= vmin
        return ok, _fuzzy_ge(x, vmin)

    if op == "GT":
        if vmin is None:
            return None, 0.0
        ok = x > vmin
        return ok, _fuzzy_ge(x, vmin)

    if op == "LE":
        if vmax is None:
            return None, 0.0
        ok = x <= vmax
        return ok, _fuzzy_le(x, vmax)

    if op == "LT":
        if vmax is None:
            return None, 0.0
        ok = x < vmax
        return ok, _fuzzy_le(x, vmax)

    if op == "BETWEEN":
        if vmin is None or vmax is None:
            return None, 0.0
        ok = (x >= vmin) and (x <= vmax)
        if ok:
            return True, 1.0
        # fuzzy: distance to interval
        if x < vmin:
            return False, _fuzzy_ge(x, vmin)
        return False, _fuzzy_le(x, vmax)

    return None, 0.0


def _eval_condition(
    g: Graph,
    kb_event_name: str,
    cond_uri: URIRef,
    row: Dict[str, Any],
    history_rows: Optional[List[Dict[str, Any]]] = None
) -> Tuple[Optional[ConditionEval], Optional[str]]:
    """
    Returns (ConditionEval, missing_reason)
    missing_reason != None means condition couldn't be evaluated fully.
    """

    var_uri = _first(g, cond_uri, EX.var)
    feature_key = _lit_str(g, cond_uri, EX.featureKey, None)
    op = _lit_str(g, cond_uri, EX.op, "GE")
    vmin = _lit_float(g, cond_uri, EX.minValue, None)
    vmax = _lit_float(g, cond_uri, EX.maxValue, None)
    unit = _lit_str(g, cond_uri, EX.unit, None)
    window_hours = _lit_int(g, cond_uri, EX.windowHours, 1) or 1
    agg = _lit_str(g, cond_uri, EX.agg, None)
    duration_hours = _lit_int(g, cond_uri, EX.durationHours, 0) or 0
    source = _lit_str(g, cond_uri, EX.source, None)
    note = _lit_str(g, cond_uri, EX.note, None)

    # PRESENT op => only requires the feature exists (direct or proxy)
    if op.upper() == "PRESENT":
        val = _get_feature_value(row, feature_key, history_rows=history_rows)
        missing = None if val is not None else (feature_key or str(var_uri))
        ev = ConditionEval(
            kb_event=kb_event_name,
            condition=str(cond_uri),
            variable=str(var_uri) if var_uri else "",
            feature_key=feature_key,
            op="PRESENT",
            value=val,
            min=None,
            max=None,
            unit=unit,
            windowHours=window_hours,
            agg=agg,
            durationHours=duration_hours,
            duration_ok=None,
            fuzzy_score=1.0 if val is not None else 0.0,
            hard_ok=True if val is not None else False,
            source=source,
            note=note,
        )
        return ev, missing

    # Aggregate SUM over last N hours (uses the *base feature_key* from KB)
    if agg and agg.upper() == "SUM" and window_hours > 1:
        if not history_rows or len(history_rows) < window_hours:
            ev = ConditionEval(
                kb_event=kb_event_name,
                condition=str(cond_uri),
                variable=str(var_uri) if var_uri else "",
                feature_key=feature_key,
                op=op,
                value=None,
                min=vmin,
                max=vmax,
                unit=unit,
                windowHours=window_hours,
                agg=agg,
                durationHours=duration_hours,
                duration_ok=None,
                fuzzy_score=0.0,
                hard_ok=None,
                source=source,
                note=note,
            )
            return ev, f"history:{feature_key}:{window_hours}h"

        vals = []
        for hrow in history_rows[-window_hours:]:
            raw = _get_feature_value(hrow, feature_key, history_rows=None)
            if raw is None:
                return None, feature_key
            vals.append(_convert_value(feature_key, float(raw), unit))

        x = float(sum(vals))
        hard_ok, fuzzy = _eval_numeric(op, x, vmin, vmax)

        ev = ConditionEval(
            kb_event=kb_event_name,
            condition=str(cond_uri),
            variable=str(var_uri) if var_uri else "",
            feature_key=feature_key,
            op=op,
            value=x,
            min=vmin,
            max=vmax,
            unit=unit,
            windowHours=window_hours,
            agg=agg,
            durationHours=duration_hours,
            duration_ok=None,
            fuzzy_score=float(fuzzy),
            hard_ok=hard_ok,
            source=source,
            note=note,
        )
        return ev, None

    # Duration requirement: must hold for N hours
    if duration_hours and duration_hours > 1:
        if not history_rows or len(history_rows) < duration_hours:
            ev = ConditionEval(
                kb_event=kb_event_name,
                condition=str(cond_uri),
                variable=str(var_uri) if var_uri else "",
                feature_key=feature_key,
                op=op,
                value=None,
                min=vmin,
                max=vmax,
                unit=unit,
                windowHours=window_hours,
                agg=agg,
                durationHours=duration_hours,
                duration_ok=None,
                fuzzy_score=0.0,
                hard_ok=None,
                source=source,
                note=note,
            )
            return ev, f"history:{feature_key}:{duration_hours}h"

        fuzzies = []
        oks = []
        for hrow in history_rows[-duration_hours:]:
            raw = _get_feature_value(hrow, feature_key, history_rows=None)
            if raw is None:
                return None, feature_key
            x = _convert_value(feature_key, float(raw), unit)
            ok, fuzzy = _eval_numeric(op, x, vmin, vmax)
            oks.append(bool(ok) if ok is not None else False)
            fuzzies.append(float(fuzzy))

        duration_ok = all(oks) if oks else None
        fuzzy_score = float(min(fuzzies)) if fuzzies else 0.0
        hard_ok = True if duration_ok else False

        ev = ConditionEval(
            kb_event=kb_event_name,
            condition=str(cond_uri),
            variable=str(var_uri) if var_uri else "",
            feature_key=feature_key,
            op=op,
            value=None,
            min=vmin,
            max=vmax,
            unit=unit,
            windowHours=window_hours,
            agg=agg,
            durationHours=duration_hours,
            duration_ok=duration_ok,
            fuzzy_score=fuzzy_score,
            hard_ok=hard_ok,
            source=source,
            note=note,
        )
        return ev, None

    # Simple (single-hour) condition
    raw = _get_feature_value(row, feature_key, history_rows=history_rows)
    if raw is None:
        ev = ConditionEval(
            kb_event=kb_event_name,
            condition=str(cond_uri),
            variable=str(var_uri) if var_uri else "",
            feature_key=feature_key,
            op=op,
            value=None,
            min=vmin,
            max=vmax,
            unit=unit,
            windowHours=window_hours,
            agg=agg,
            durationHours=duration_hours,
            duration_ok=None,
            fuzzy_score=0.0,
            hard_ok=None,
            source=source,
            note=note,
        )
        return ev, feature_key or str(var_uri)

    x = _convert_value(feature_key, float(raw), unit)
    hard_ok, fuzzy = _eval_numeric(op, x, vmin, vmax)

    ev = ConditionEval(
        kb_event=kb_event_name,
        condition=str(cond_uri),
        variable=str(var_uri) if var_uri else "",
        feature_key=feature_key,
        op=op,
        value=float(x),
        min=vmin,
        max=vmax,
        unit=unit,
        windowHours=window_hours,
        agg=agg,
        durationHours=duration_hours,
        duration_ok=None,
        fuzzy_score=float(fuzzy),
        hard_ok=hard_ok,
        source=source,
        note=note,
    )
    return ev, None


# -----------------------------
# Event evaluation (rules OR)
# -----------------------------
def _event_uri_candidates(name: str) -> List[URIRef]:
    """
    Try a few normalizations so your XGB label can still match a KB fragment.
    """
    if not name:
        return []
    n = str(name)

    cands = []
    cands.append(EX[n])  # exact
    cands.append(EX[n.replace(" ", "_")])
    cands.append(EX[n.replace(" ", "_").replace("-", "_")])
    cands.append(EX[n.replace("-", "_")])
    return cands

def _resolve_event_uri(g: Graph, event_name: str) -> Optional[URIRef]:
    for u in _event_uri_candidates(event_name):
        if (u, None, None) in g:
            return u
    return None

def _get_recommendations(g: Graph, event_uri: URIRef) -> List[str]:
    recs = []
    for rec in g.objects(event_uri, EX.hasRecommendation):
        txt = _lit_str(g, rec, EX.text, None)
        if txt:
            recs.append(txt)
    # de-dup
    out = []
    seen = set()
    for r in recs:
        if r not in seen:
            out.append(r)
            seen.add(r)
    return out


def _evaluate_event_with_kb(
    g: Graph,
    event_name: str,
    row: Dict[str, Any],
    history_rows: Optional[List[Dict[str, Any]]] = None
) -> Tuple[str, float, str, Dict[str, Any], Optional[URIRef]]:
    """
    Returns: (kb_status, kb_score, why, evidence, resolved_event_uri)
    kb_status: VALID | REJECT | NOT_IN_KB
    """

    euri = _resolve_event_uri(g, event_name)
    if euri is None:
        return "NOT_IN_KB", 0.0, f"Event '{event_name}' not found in KB.", {"mapped_to": event_name}, None

    best_valid_score = -1.0
    best_valid_conditions = None

    any_insufficient = False
    insufficient_missing: List[str] = []
    partial_conditions: List[Dict[str, Any]] = []

    rules = list(g.objects(euri, EX.hasRule))

    # If event has no rules, treat as "no constraints" => VALID
    if not rules:
        return "VALID", 1.0, "No constraints defined in KB for this event.", {"conditions": []}, euri

    for rule in rules:
        conds = list(g.objects(rule, EX.hasCondition))
        if not conds:
            continue

        rule_missing = []
        rule_evals = []
        rule_failed = False
        rule_scores = []

        for cu in conds:
            ce, miss = _eval_condition(g, event_name, cu, row, history_rows=history_rows)
            if ce is not None:
                rule_evals.append(asdict(ce))
                if ce.hard_ok is False:
                    rule_failed = True
                rule_scores.append(float(ce.fuzzy_score))
            if miss:
                rule_missing.append(miss)

        if rule_failed:
            partial_conditions.extend(rule_evals)
            continue

        if rule_missing:
            any_insufficient = True
            insufficient_missing.extend(rule_missing)
            partial_conditions.extend(rule_evals)
            continue

        # all conditions evaluable and none failed => rule passes
        rule_score = float(min(rule_scores)) if rule_scores else 1.0
        if rule_score > best_valid_score:
            best_valid_score = rule_score
            best_valid_conditions = rule_evals

    if best_valid_score >= 0:
        return "VALID", best_valid_score, "KB constraints satisfied.", {
            "conditions": best_valid_conditions or [],
            "missing": [],
            "mapped_to": str(euri).split("#")[-1],
        }, euri

    if any_insufficient:
        missing = sorted(set(insufficient_missing))
        return "INSUFFICIENT_DATA", 0.0, "Insufficient data to validate with KB (missing required variables/history).", {
            "conditions": partial_conditions[:],
            "missing": missing,
            "mapped_to": str(euri).split("#")[-1],
        }, euri

    return "REJECT", 0.0, "KB constraints violated.", {
        "conditions": partial_conditions[:],
        "missing": [],
        "mapped_to": str(euri).split("#")[-1],
    }, euri


# -----------------------------
# Public API: candidate evaluation + final decision
# -----------------------------
def evaluate_top5_with_kb(
    g: Graph,
    top5: List[Dict[str, Any]],
    row_features: Dict[str, Any],
    history_rows: Optional[List[Dict[str, Any]]] = None,
    proba_unverified_cutoff: float = 0.85,
) -> Dict[str, Any]:
    """
    Accepts any length list; name kept for backward compatibility.
    top5: list like [{"event": "...", "probability": p, "threshold": t}, ...]
    row_features: dict of forecast variables (temperature_2m, wind_speed_10m, precipitation, etc.)
    history_rows: list of dicts for previous hours (including current) for duration/aggregate/proxy checks
    """

    checked: List[Dict[str, Any]] = []

    # 1) evaluate each candidate
    for cand in top5:
        ev = cand["event"]
        p = float(cand["probability"])
        thr = float(cand.get("threshold", 0.5))
        above = p >= thr

        if ev == "No-Event":
            checked.append(asdict(CandidateEval(
                event=ev, probability=p, threshold=thr, above_threshold=above,
                kb_status="VALID", kb_score=1.0,
                why="No-Event fallback.",
                evidence={}
            )))
            continue

        kb_status, kb_score, why, evidence, euri = _evaluate_event_with_kb(
            g=g, event_name=ev, row=row_features, history_rows=history_rows
        )

        checked.append(asdict(CandidateEval(
            event=ev, probability=p, threshold=thr, above_threshold=above,
            kb_status=kb_status, kb_score=float(kb_score),
            why=why, evidence=evidence
        )))


    # 2) choose final event
    # Priority:
    #   A) among above_threshold candidates, first VALID with highest probability
    #   B) else No-Event

    above_list = [c for c in checked if c["above_threshold"] and c["event"] != "No-Event"]

    # A) VALID
    valid = [c for c in above_list if c["kb_status"] == "VALID"]
    if valid:
        best = sorted(valid, key=lambda x: x["probability"], reverse=True)[0]
        euri = _resolve_event_uri(g, best["event"])
        recs = _get_recommendations(g, euri) if euri is not None else []
        return {
            "final_event": best["event"],
            "final_proba": float(best["probability"]),
            "kb_status": "VALID",
            "kb_why": best["why"],
            "top5_checked": checked,
            "recommendations": recs
        }


    # B) No-Event
    euri = _resolve_event_uri(g, "No-Event") 
    recs = _get_recommendations(g, euri) if euri is not None else []
    if not recs:
        recs = ["Aucun événement majeur détecté."]

    return {
        "final_event": "No-Event",
        "final_proba": 1.0,
        "kb_status": "VALID",
        "kb_why": "No candidate passed KB validation and none was confidently unverified => No-Event.",
        "top5_checked": checked,
        "recommendations": recs
    }
