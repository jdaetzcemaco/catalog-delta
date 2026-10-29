"""
Did the catalog teams' work on a SKU change its organic presence in Google?

Event per SKU = the day it left the STEP workflow (work log), cohort = ISO week × team.
Before/after windows aligned on the event (the event day itself is skipped), a matched
control of SKUs that were not worked (same calendar window, same bucket of previous
impressions), and three separate readings: indexing (0 → >0 impressions), coverage
(queries, impressions, clicks) and ranking (impression-weighted position).

Pure pandas; the job calls `build_seo_impact` and stores the frames under perf/seo_*.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

SEO_VERSION = 1
WINDOW = 14
MIN_IMPR = 10          # a change smaller than this many impressions is "estable"
REL = 0.20             # ±20 % to call it "creció" / "bajó"
SMALL_SAMPLE = 30      # SKUs with presence below which the cohort is flagged
BUCKET_EDGES = [-1, 0, 9, 99, 999, np.inf]
BUCKET_LABELS = ["0", "1–9", "10–99", "100–999", "1000+"]
STATUS = {
    "pendiente": "Pendiente (ventana incompleta)",
    "no_visible": "No visible tras el trabajo",
    "sin_presencia": "Sin presencia en Google",
    "nuevo": "Nuevo en Google",
    "crecio": "Creció",
    "bajo": "Bajó",
    "estable": "Estable",
}
ALL_TEAMS = "Todos"
QUERY_STATUS = {"nueva": "Nueva", "perdida": "Perdida", "subio": "Subió", "bajo": "Bajó", "estable": "Estable"}


def _dt(s) -> pd.Series:
    """Datetimes from strings of any format (reports mix dates and timestamps) or datetimes."""
    if s is None:
        return pd.Series(dtype="datetime64[ns]")
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s)
    return pd.to_datetime(s, errors="coerce", format="mixed")


def _pct(after, before) -> float:
    if before is None or pd.isna(before) or before <= 0:
        return np.nan
    return float((after / before - 1) * 100)


def bucket(impressions: pd.Series) -> pd.Series:
    return pd.cut(impressions.fillna(0), BUCKET_EDGES, labels=BUCKET_LABELS).astype(str)


def week_monday(days: pd.Series) -> pd.Series:
    d = pd.to_datetime(days)
    return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()


# ── 1. events ───────────────────────────────────────────────────────────────
def events_from_work_log(work_log: pd.DataFrame | None, visible_history: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    One row per worked SKU: event_day (latest `salida_flujo`, else latest `fin_dat_log`),
    cohorte (Monday of that week), equipo (Diseño / Edición / Diseño+Edición when both
    teams have an event ≤ 7 days apart), usuarios, plus first_seen / last_visible.
    """
    cols = ["SKU", "event_day", "cohorte", "equipo", "usuarios", "creacion_step", "first_seen", "last_visible"]
    if work_log is None or work_log.empty:
        return pd.DataFrame(columns=cols)
    wl = work_log.copy()
    wl["SKU"] = wl["SKU"].astype(str).str.strip()
    salida = _dt(wl.get("salida_flujo")).dt.normalize()
    datlog = _dt(wl.get("fin_dat_log")).dt.normalize()
    wl["event_day"] = salida.fillna(datlog)
    wl = wl.dropna(subset=["event_day"])
    if wl.empty:
        return pd.DataFrame(columns=cols)
    per_team = wl.groupby(["SKU", "equipo"])["event_day"].max().reset_index()
    latest = per_team.sort_values("event_day").groupby("SKU").tail(1).set_index("SKU")
    span = per_team.groupby("SKU")["event_day"].agg(["min", "max", "count"])
    both = span[(span["count"] > 1) & ((span["max"] - span["min"]).dt.days <= 7)].index
    out = pd.DataFrame({"SKU": latest.index, "event_day": latest["event_day"].values, "equipo": latest["equipo"].values})
    out.loc[out["SKU"].isin(both), "equipo"] = "Diseño+Edición"
    out["cohorte"] = week_monday(out["event_day"])
    users = wl.dropna(subset=["usuario"]).groupby("SKU")["usuario"].agg(lambda s: ", ".join(sorted(set(map(str, s)))))
    out["usuarios"] = out["SKU"].map(users).fillna("")
    if "creacion_step" in wl.columns:
        out["creacion_step"] = out["SKU"].map(_dt(wl["creacion_step"]).groupby(wl["SKU"]).min())
    else:
        out["creacion_step"] = pd.NaT
    out["first_seen"], out["last_visible"] = pd.NaT, pd.NaT
    if visible_history is not None and not visible_history.empty:
        vh = visible_history.assign(SKU=visible_history["SKU"].astype(str)).drop_duplicates("SKU").set_index("SKU")
        out["first_seen"] = out["SKU"].map(_dt(vh["first_seen"]))
        out["last_visible"] = out["SKU"].map(_dt(vh["last_visible"]))
    return out[cols].reset_index(drop=True)


# ── 2. windows ──────────────────────────────────────────────────────────────
def _norm_daily(daily_gsc: pd.DataFrame | None) -> pd.DataFrame:
    cols = ["day", "SKU", "clicks", "impressions", "position", "impressions_ml", "impressions_ps", "consultas"]
    if daily_gsc is None or daily_gsc.empty:
        return pd.DataFrame(columns=cols)
    d = daily_gsc.copy()
    d["day"] = pd.to_datetime(d["day"]).dt.normalize()
    d["SKU"] = d["SKU"].astype(str)
    for c in ("impressions_ml", "impressions_ps", "consultas"):
        if c not in d.columns:
            d[c] = 0
    for c in ("clicks", "impressions", "impressions_ml", "impressions_ps", "consultas"):
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
    d["position"] = pd.to_numeric(d["position"], errors="coerce")
    return d[cols]


def window_metrics(daily_gsc: pd.DataFrame | None, events: pd.DataFrame, as_of: str, window: int = WINDOW) -> pd.DataFrame:
    """
    Per SKU of `events`: sums before (−window..−1) and after (+1..+window) the event day,
    impression-weighted positions, days with impressions, first impression ever, and
    whether each window is complete given the data range. After-window values are NaN
    while incomplete, so a half-pulled window never reads as a drop.
    """
    d = _norm_daily(daily_gsc)
    ev = events[["SKU", "event_day"]].copy()
    ev["SKU"] = ev["SKU"].astype(str)
    ev["event_day"] = pd.to_datetime(ev["event_day"]).dt.normalize()
    out = ev.copy()
    data_min = d["day"].min() if not d.empty else pd.NaT
    end = pd.Timestamp(as_of).normalize()
    m = d.merge(ev, on="SKU", how="inner")
    m["offset"] = (m["day"] - m["event_day"]).dt.days
    before = m[(m["offset"] >= -window) & (m["offset"] <= -1)]
    after = m[(m["offset"] >= 1) & (m["offset"] <= window)]

    def agg(part: pd.DataFrame, suffix: str) -> pd.DataFrame:
        g = part.assign(w=part["position"] * part["impressions"]).groupby("SKU").agg(
            impr=("impressions", "sum"), clics=("clicks", "sum"), w=("w", "sum"), consultas=("consultas", "sum"),
            impr_ml=("impressions_ml", "sum"), impr_ps=("impressions_ps", "sum"),
            dias=("impressions", lambda s: int((s > 0).sum())))
        g["pos"] = (g["w"] / g["impr"].where(g["impr"] > 0)).round(1)
        g = g.drop(columns="w")
        g.columns = [f"{c}_{suffix}" for c in g.columns]
        return g

    out = out.merge(agg(before, "antes"), left_on="SKU", right_index=True, how="left")
    out = out.merge(agg(after, "despues"), left_on="SKU", right_index=True, how="left")
    sums = [c for c in out.columns if c.endswith(("_antes", "_despues")) and not c.startswith("pos_")]
    out[sums] = out[sums].fillna(0)
    first = d[d["impressions"] > 0].groupby("SKU")["day"].min()
    out["primera_impresion"] = out["SKU"].map(first)
    out["antes_completo"] = (out["event_day"] - pd.Timedelta(days=window)) >= data_min if pd.notna(data_min) else False
    out["despues_completo"] = (out["event_day"] + pd.Timedelta(days=window)) <= end
    out["completo"] = out["antes_completo"] & out["despues_completo"]
    after_cols = [c for c in out.columns if c.endswith("_despues")]
    out.loc[~out["despues_completo"], after_cols] = np.nan
    out["delta_impr_pct"] = [_pct(a, b) for a, b in zip(out["impr_despues"], out["impr_antes"])]
    out["delta_clics_pct"] = [_pct(a, b) for a, b in zip(out["clics_despues"], out["clics_antes"])]
    return out


# ── 3. status ───────────────────────────────────────────────────────────────
def sku_status(m: pd.DataFrame, min_impr: int = MIN_IMPR, rel: float = REL) -> pd.Series:
    """Status per row of `window_metrics` output (joined with last_visible when available)."""
    # Visibility is only judged when the history is there (a NaT everywhere means "unknown")
    has_vis = "last_visible" in m.columns and m["last_visible"].notna().any()

    def one(r) -> str:
        if not bool(r.get("completo", False)):
            return "pendiente"
        if has_vis:
            lv = r.get("last_visible")
            if lv is None or pd.isna(lv) or pd.Timestamp(lv) < r["event_day"]:
                return "no_visible"
        b, a = float(r["impr_antes"]), float(r["impr_despues"])
        if b == 0 and a == 0:
            return "sin_presencia"
        first = r.get("primera_impresion")
        if b == 0 and a > 0:
            return "nuevo" if (first is None or pd.isna(first) or pd.Timestamp(first) > r["event_day"]) else "crecio"
        if a == 0 or (a - b <= -min_impr and a / b - 1 <= -rel):
            return "bajo"
        if a - b >= min_impr and a / b - 1 >= rel:
            return "crecio"
        return "estable"
    return m.apply(one, axis=1) if not m.empty else pd.Series(dtype=object)


# ── 4. control ──────────────────────────────────────────────────────────────
def control_pool(events: pd.DataFrame, work_log: pd.DataFrame | None, visible_history: pd.DataFrame | None,
                 ctx: pd.DataFrame | None, pivot: pd.Timestamp, window: int = WINDOW) -> pd.DataFrame:
    """
    SKUs that can stand in for the worked ones around `pivot`: visible before and after
    the window (visible history), not worked within ±(window + 7) days of `pivot`.
    Returns SKU + NIVEL 1 (when `ctx` has it).
    """
    if visible_history is None or visible_history.empty:
        return pd.DataFrame(columns=["SKU", "NIVEL 1"])
    vh = visible_history.assign(SKU=visible_history["SKU"].astype(str)).drop_duplicates("SKU")
    first = _dt(vh["first_seen"])
    last_vis = _dt(vh["last_visible"])
    ok = (first <= pivot - pd.Timedelta(days=window)) & (last_vis >= pivot + pd.Timedelta(days=window))
    pool = vh.loc[ok, ["SKU"]].copy()
    worked = set(events["SKU"].astype(str))
    if work_log is not None and not work_log.empty:
        wl = work_log.copy()
        ev = _dt(wl.get("salida_flujo")).fillna(_dt(wl.get("fin_dat_log"))).dt.normalize()
        near = (ev - pivot).abs().dt.days <= window + 7
        worked |= set(wl.loc[near.fillna(False), "SKU"].astype(str))
    pool = pool[~pool["SKU"].isin(worked)]
    if ctx is not None and "NIVEL 1" in ctx.columns:
        pool = pool.merge(ctx.assign(SKU=ctx["SKU"].astype(str))[["SKU", "NIVEL 1"]].drop_duplicates("SKU"),
                          on="SKU", how="left")
    else:
        pool["NIVEL 1"] = np.nan
    return pool.reset_index(drop=True)


CTRL_SUMS = ["impr_antes", "impr_despues", "clics_antes", "clics_despues"]


def aggregate_controls(rows: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse per-SKU control metrics into strata (cohorte, event_day, bucket, NIVEL 1):
    count, sums and the number that went 0 → >0. Everything downstream only needs these
    sums, and keeping one row per pool SKU per event day was the job's memory peak.
    """
    cols = ["cohorte", "event_day", "bucket", "NIVEL 1", "ctrl_n", *CTRL_SUMS, "nuevos"]
    if rows is None or rows.empty:
        return pd.DataFrame(columns=cols)
    if "ctrl_n" in rows.columns:
        return rows
    c = rows.copy()
    c["bucket"] = bucket(c["impr_antes"])
    c["nuevos"] = ((c["impr_antes"] == 0) & (c["impr_despues"] > 0)).astype(int)
    if "cohorte" not in c.columns:
        c["cohorte"] = pd.NaT
    if "NIVEL 1" not in c.columns:
        c["NIVEL 1"] = np.nan
    g = c.groupby(["cohorte", "event_day", "bucket", "NIVEL 1"], dropna=False).agg(
        ctrl_n=("SKU", "size"), **{k: (k, "sum") for k in CTRL_SUMS}, nuevos=("nuevos", "sum")).reset_index()
    return g[cols]


def _ctrl_summary(agg: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    g = agg.groupby(keys, dropna=False).agg(ctrl_n=("ctrl_n", "sum"), **{f"ctrl_{k}": (k, "sum") for k in CTRL_SUMS},
                                            nuevos=("nuevos", "sum")).reset_index()
    g["ctrl_tasa_nuevos"] = g["nuevos"] / g["ctrl_n"].where(g["ctrl_n"] > 0)
    return g.drop(columns="nuevos")


def match_controls(treated: pd.DataFrame, controls: pd.DataFrame, min_stratum: int = 20) -> pd.DataFrame:
    """
    Stratified control deltas for each treated row. Stratum = (event_day, bucket of previous
    impressions[, NIVEL 1 when that stratum holds ≥ min_stratum controls]). Deterministic:
    the control delta is the ratio of sums over the stratum (no sampling). `controls` is
    either per-SKU metrics or the output of `aggregate_controls`.
    """
    t = treated.copy()
    t["bucket"] = bucket(t["impr_antes"])
    out_cols = ["ctrl_n", "ctrl_impr_antes", "ctrl_impr_despues", "ctrl_clics_antes", "ctrl_clics_despues",
                "ctrl_delta_impr_pct", "ctrl_delta_clics_pct", "ctrl_tasa_nuevos", "uplift_impr_pct", "uplift_clics_pct"]
    agg = aggregate_controls(controls)
    if agg.empty or t.empty:
        for c in out_cols:
            t[c] = np.nan
        return t.drop(columns="bucket")
    keys_fine = ["event_day", "bucket", "NIVEL 1"] if "NIVEL 1" in t.columns and agg["NIVEL 1"].notna().any() else None
    keys_coarse = ["event_day", "bucket"]
    merged = t.merge(_ctrl_summary(agg, keys_coarse), on=keys_coarse, how="left")
    if keys_fine:
        fine = _ctrl_summary(agg, keys_fine)
        fine = fine[fine["ctrl_n"] >= min_stratum]
        f = t[["SKU", *keys_fine]].merge(fine, on=keys_fine, how="left").set_index("SKU")
        merged = merged.set_index("SKU")
        for col in [c_ for c_ in fine.columns if c_.startswith("ctrl_")]:
            merged[col] = f[col].where(f[col].notna(), merged[col])
        merged = merged.reset_index()
    merged["ctrl_delta_impr_pct"] = [_pct(a, b) for a, b in zip(merged["ctrl_impr_despues"], merged["ctrl_impr_antes"])]
    merged["ctrl_delta_clics_pct"] = [_pct(a, b) for a, b in zip(merged["ctrl_clics_despues"], merged["ctrl_clics_antes"])]
    merged["uplift_impr_pct"] = merged["delta_impr_pct"] - merged["ctrl_delta_impr_pct"]
    merged["uplift_clics_pct"] = merged["delta_clics_pct"] - merged["ctrl_delta_clics_pct"]
    return merged.drop(columns="bucket")


# ── 5. cohort summary, buckets and aligned daily index ──────────────────────
def _group_summary(x: pd.DataFrame, ctrl: pd.DataFrame | None, n_boot: int, seed: int) -> dict:
    complete = x[x["completo"].fillna(False)]
    st = x["estado"].value_counts()
    row = {
        "skus": int(len(x)), "completo": bool(len(x) and complete.shape[0] == len(x)),
        "con_presencia": int(((complete["impr_antes"] > 0) | (complete["impr_despues"] > 0)).sum()),
        **{k: int(st.get(k, 0)) for k in ("nuevo", "crecio", "bajo", "estable", "sin_presencia", "no_visible", "pendiente")},
        "impr_antes": float(complete["impr_antes"].sum()), "impr_despues": float(complete["impr_despues"].sum()),
        "clics_antes": float(complete["clics_antes"].sum()), "clics_despues": float(complete["clics_despues"].sum()),
        "consultas_antes": float(complete["consultas_antes"].sum()), "consultas_despues": float(complete["consultas_despues"].sum()),
    }
    row["delta_impr_pct"] = _pct(row["impr_despues"], row["impr_antes"])
    row["delta_clics_pct"] = _pct(row["clics_despues"], row["clics_antes"])
    both = complete[(complete["impr_antes"] > 0) & (complete["impr_despues"] > 0)]
    row["pos_antes"] = float(np.average(both["pos_antes"].fillna(0), weights=both["impr_antes"])) if len(both) else np.nan
    row["pos_despues"] = float(np.average(both["pos_despues"].fillna(0), weights=both["impr_despues"])) if len(both) else np.nan
    eligible = complete[complete["estado"].isin(["sin_presencia", "nuevo"])]
    row["tasa_nuevos"] = float((eligible["estado"] == "nuevo").mean()) if len(eligible) else np.nan
    nuevos = complete[complete["estado"] == "nuevo"]
    dias = (nuevos["primera_impresion"] - nuevos["event_day"]).dt.days
    row["dias_a_primera_impresion_mediana"] = float(dias.median()) if len(dias) else np.nan
    # Control: ratio of sums of the matched strata, weighted by the treated SKUs' own control cells
    with_ctrl = complete[complete["ctrl_impr_antes"].notna() & (complete["ctrl_impr_antes"] > 0)] if "ctrl_impr_antes" in complete else complete.iloc[0:0]
    if len(with_ctrl):
        w_before = with_ctrl["impr_antes"].clip(lower=1)
        ctrl_delta_impr = float(np.average(with_ctrl["ctrl_delta_impr_pct"].fillna(0), weights=w_before))
        ctrl_delta_clics = float(np.average(with_ctrl["ctrl_delta_clics_pct"].fillna(0), weights=w_before))
        row["ctrl_delta_impr_pct"], row["ctrl_delta_clics_pct"] = ctrl_delta_impr, ctrl_delta_clics
        row["ctrl_tasa_nuevos"] = float(with_ctrl["ctrl_tasa_nuevos"].mean()) if "ctrl_tasa_nuevos" in with_ctrl else np.nan
    else:
        row["ctrl_delta_impr_pct"] = row["ctrl_delta_clics_pct"] = row["ctrl_tasa_nuevos"] = np.nan
    row["uplift_impr_pct"] = row["delta_impr_pct"] - row["ctrl_delta_impr_pct"] if pd.notna(row["ctrl_delta_impr_pct"]) else np.nan
    row["uplift_clics_pct"] = row["delta_clics_pct"] - row["ctrl_delta_clics_pct"] if pd.notna(row["ctrl_delta_clics_pct"]) else np.nan
    # Bootstrap CI of the uplift (resample treated SKUs with presence before the event)
    pres = complete[complete["impr_antes"] > 0]
    if len(pres) >= 5 and pd.notna(row["ctrl_delta_impr_pct"]) and n_boot > 0:
        rng = np.random.default_rng(seed)
        b, a = pres["impr_antes"].to_numpy(float), pres["impr_despues"].to_numpy(float)
        cd = pres["ctrl_delta_impr_pct"].fillna(0).to_numpy(float)
        idx = rng.integers(0, len(pres), size=(n_boot, len(pres)))
        sb, sa = b[idx].sum(axis=1), a[idx].sum(axis=1)
        ctrl = (cd[idx] * b[idx]).sum(axis=1) / np.maximum(sb, 1)
        up = (sa / np.maximum(sb, 1) - 1) * 100 - ctrl
        row["uplift_impr_ci_low"], row["uplift_impr_ci_high"] = float(np.percentile(up, 2.5)), float(np.percentile(up, 97.5))
    else:
        row["uplift_impr_ci_low"] = row["uplift_impr_ci_high"] = np.nan
    row["muestra_pequena"] = bool(row["con_presencia"] < SMALL_SAMPLE)
    return row


def cohort_summary(skus: pd.DataFrame, as_of: str, n_boot: int = 200, seed: int = 7) -> pd.DataFrame:
    """One row per (cohorte, equipo) plus a 'Todos' row per cohort (each SKU counted once)."""
    rows = []
    if skus.empty:
        return pd.DataFrame(columns=["cohorte", "equipo", "skus"])
    for cohorte, part in skus.groupby("cohorte"):
        rows.append({"cohorte": cohorte, "equipo": ALL_TEAMS, **_group_summary(part, None, n_boot, seed)})
        for equipo, sub in part.groupby("equipo"):
            rows.append({"cohorte": cohorte, "equipo": equipo, **_group_summary(sub, None, n_boot, seed)})
    out = pd.DataFrame(rows)
    out["as_of"] = pd.Timestamp(as_of)
    return out.sort_values(["cohorte", "equipo"]).reset_index(drop=True)


def bucket_summary(skus: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    """Treated vs control Δ impressions / clicks per bucket of previous impressions, per cohort."""
    rows = []
    t = skus[skus["completo"].fillna(False)].copy()
    if t.empty:
        return pd.DataFrame(columns=["cohorte", "equipo", "bucket", "n", "impr_antes", "impr_despues", "delta_impr_pct",
                                     "ctrl_n", "ctrl_impr_antes", "ctrl_impr_despues", "ctrl_delta_impr_pct"])
    t["bucket"] = bucket(t["impr_antes"])
    c = aggregate_controls(controls)
    for (cohorte, equipo), part in pd.concat([t.assign(equipo=ALL_TEAMS), t]).groupby(["cohorte", "equipo"]):
        cc = c[c["cohorte"] == cohorte] if not c.empty else c
        for b in BUCKET_LABELS:
            x = part[part["bucket"] == b]
            if x.empty:
                continue
            y = cc[cc["bucket"] == b] if not cc.empty else cc
            n_c = int(y["ctrl_n"].sum()) if len(y) else 0
            rows.append({"cohorte": cohorte, "equipo": equipo, "bucket": b, "n": int(len(x)),
                         "impr_antes": float(x["impr_antes"].sum()), "impr_despues": float(x["impr_despues"].sum()),
                         "delta_impr_pct": _pct(x["impr_despues"].sum(), x["impr_antes"].sum()),
                         "clics_antes": float(x["clics_antes"].sum()), "clics_despues": float(x["clics_despues"].sum()),
                         "delta_clics_pct": _pct(x["clics_despues"].sum(), x["clics_antes"].sum()),
                         "ctrl_n": n_c, "ctrl_impr_antes": float(y["impr_antes"].sum()) if n_c else np.nan,
                         "ctrl_impr_despues": float(y["impr_despues"].sum()) if n_c else np.nan,
                         "ctrl_delta_impr_pct": _pct(y["impr_despues"].sum(), y["impr_antes"].sum()) if n_c else np.nan,
                         "ctrl_delta_clics_pct": _pct(y["clics_despues"].sum(), y["clics_antes"].sum()) if n_c else np.nan})
    return pd.DataFrame(rows)


def daily_index(skus: pd.DataFrame, daily_gsc: pd.DataFrame | None, controls_daily: pd.DataFrame | None,
                window: int = WINDOW) -> pd.DataFrame:
    """
    Impressions per day relative to the event (offset −window..+window) for the worked
    SKUs of each (cohorte, equipo) and for their control, indexed to the before-window
    mean (= 100). `controls_daily`: day, impressions, clicks, skus, cohorte, event_day (one
    row per cohort and day, already summed over the control pool).
    """
    cols = ["cohorte", "equipo", "grupo", "offset", "impressions", "clicks", "skus", "indice"]
    d = _norm_daily(daily_gsc)
    if d.empty or skus.empty:
        return pd.DataFrame(columns=cols)
    frames = []
    t = skus[skus["completo"].fillna(False)][["SKU", "event_day", "cohorte", "equipo"]]
    t = pd.concat([t.assign(equipo=ALL_TEAMS), t])

    def aligned(part: pd.DataFrame, grupo: str) -> pd.DataFrame:
        m = d.merge(part, on="SKU", how="inner")
        m["offset"] = (m["day"] - m["event_day"]).dt.days
        m = m[(m["offset"] >= -window) & (m["offset"] <= window)]
        g = m.groupby(["cohorte", "equipo", "offset"]).agg(impressions=("impressions", "sum"), clicks=("clicks", "sum")).reset_index()
        n = part.groupby(["cohorte", "equipo"])["SKU"].nunique().rename("skus").reset_index()
        g = g.merge(n, on=["cohorte", "equipo"], how="left")
        g["grupo"] = grupo
        return g

    frames.append(aligned(t, "trabajados"))
    if controls_daily is not None and not controls_daily.empty:
        cd = controls_daily.copy()
        cd["day"] = pd.to_datetime(cd["day"]).dt.normalize()
        cd["event_day"] = pd.to_datetime(cd["event_day"]).dt.normalize()
        cd["offset"] = (cd["day"] - cd["event_day"]).dt.days
        cd = cd[(cd["offset"] >= -window) & (cd["offset"] <= window)]
        for equipo in t["equipo"].unique():
            g = cd.groupby(["cohorte", "offset"]).agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"),
                                                      skus=("skus", "max")).reset_index()
            g["equipo"], g["grupo"] = equipo, "control"
            frames.append(g)
    out = pd.concat(frames, ignore_index=True)
    if out.empty:
        return pd.DataFrame(columns=cols)
    # Fill missing offsets with zero so the lines are continuous
    full = []
    for (cohorte, equipo, grupo), part in out.groupby(["cohorte", "equipo", "grupo"]):
        idx = pd.DataFrame({"offset": range(-window, window + 1)})
        p = idx.merge(part, on="offset", how="left")
        p["cohorte"], p["equipo"], p["grupo"] = cohorte, equipo, grupo
        p[["impressions", "clicks"]] = p[["impressions", "clicks"]].fillna(0)
        p["skus"] = part["skus"].iloc[0]
        base = p.loc[p["offset"] < 0, "impressions"].mean()
        p["indice"] = (p["impressions"] / base * 100).round(1) if base and base > 0 else np.nan
        full.append(p)
    return pd.concat(full, ignore_index=True)[cols]


# ── 6. query-level deltas ───────────────────────────────────────────────────
def query_deltas(events: pd.DataFrame, queries: pd.DataFrame | None, window: int = WINDOW, min_impr: int = MIN_IMPR,
                 pos_change: float = 3.0) -> pd.DataFrame:
    """
    Per (SKU, query): impressions / clicks / position before and after the event, status
    nueva / perdida / subio / bajo / estable. `queries` = concatenated gsc/<day>/queries.parquet
    rows (day, SKU, query, impressions, clicks, position, anonima, ml, ps).
    """
    cols = ["SKU", "query", "impr_antes", "impr_despues", "clics_antes", "clics_despues", "pos_antes", "pos_despues",
            "estado", "anonima", "ml", "ps", "cohorte", "event_day"]
    if queries is None or queries.empty or events.empty:
        return pd.DataFrame(columns=cols)
    q = queries.copy()
    q["day"] = pd.to_datetime(q["day"]).dt.normalize()
    q["SKU"] = q["SKU"].astype(str)
    ev = events[["SKU", "event_day", "cohorte"]].copy()
    ev["SKU"] = ev["SKU"].astype(str)
    ev["event_day"] = pd.to_datetime(ev["event_day"]).dt.normalize()
    m = q.merge(ev, on="SKU", how="inner")
    m["offset"] = (m["day"] - m["event_day"]).dt.days
    m = m[(m["offset"] >= -window) & (m["offset"] <= window) & (m["offset"] != 0)]
    if m.empty:
        return pd.DataFrame(columns=cols)
    m["when"] = np.where(m["offset"] < 0, "antes", "despues")
    m["w"] = m["position"] * m["impressions"]
    g = m.groupby(["SKU", "query", "when"]).agg(impr=("impressions", "sum"), clics=("clicks", "sum"), w=("w", "sum")).unstack("when")
    g.columns = [f"{a}_{b}" for a, b in g.columns]
    for c in ("impr_antes", "impr_despues", "clics_antes", "clics_despues", "w_antes", "w_despues"):
        if c not in g.columns:
            g[c] = 0.0
    g = g.fillna(0.0)
    g["pos_antes"] = (g["w_antes"] / g["impr_antes"].where(g["impr_antes"] > 0)).round(1)
    g["pos_despues"] = (g["w_despues"] / g["impr_despues"].where(g["impr_despues"] > 0)).round(1)
    g = g.drop(columns=["w_antes", "w_despues"]).reset_index()
    flags = m.groupby(["SKU", "query"]).agg(anonima=("anonima", "max"), ml=("ml", "max"), ps=("ps", "max")).reset_index()
    g = g.merge(flags, on=["SKU", "query"], how="left").merge(ev, on="SKU", how="left")

    def status(r) -> str:
        if r["impr_antes"] == 0 and r["impr_despues"] > 0:
            return "nueva"
        if r["impr_antes"] > 0 and r["impr_despues"] == 0:
            return "perdida"
        if max(r["impr_antes"], r["impr_despues"]) >= min_impr and pd.notna(r["pos_antes"]) and pd.notna(r["pos_despues"]):
            if r["pos_antes"] - r["pos_despues"] >= pos_change:
                return "subio"
            if r["pos_despues"] - r["pos_antes"] >= pos_change:
                return "bajo"
        return "estable"

    g["estado"] = g.apply(status, axis=1)
    for c in ("anonima", "ml", "ps"):
        g[c] = g[c].fillna(False).astype(bool)
    return g[cols].sort_values(["SKU", "impr_despues"], ascending=[True, False]).reset_index(drop=True)


def query_rollup(deltas: pd.DataFrame) -> pd.DataFrame:
    """Per SKU: consultas_nuevas, consultas_perdidas, top_consulta_despues."""
    cols = ["SKU", "consultas_nuevas", "consultas_perdidas", "consultas_subieron", "top_consulta_despues"]
    if deltas is None or deltas.empty:
        return pd.DataFrame(columns=cols)
    real = deltas[~deltas["anonima"]]
    g = real.groupby("SKU").agg(consultas_nuevas=("estado", lambda s: int((s == "nueva").sum())),
                                consultas_perdidas=("estado", lambda s: int((s == "perdida").sum())),
                                consultas_subieron=("estado", lambda s: int((s == "subio").sum())))
    top = real[real["impr_despues"] > 0].sort_values("impr_despues", ascending=False).drop_duplicates("SKU").set_index("SKU")["query"]
    g["top_consulta_despues"] = top
    return g.reset_index()[cols]


# ── 7. what changed on the listing ──────────────────────────────────────────
LISTING_FLAGS = {"has_desc": "descripción", "has_image": "imagen", "has_name": "nombre", "has_brand": "marca",
                 "has_price": "precio", "is_visible": "visible"}


def listing_changes(flags_before: pd.DataFrame | None, flags_after: pd.DataFrame | None, skus: pd.Series) -> pd.DataFrame:
    """
    Compare `build_flags` output of the snapshot before and after the event for `skus`:
    score before/after and a short text of what was gained or lost (e.g. "+descripción, +imagen").
    """
    cols = ["SKU", "score_antes", "score_despues", "cambios_ficha"]
    if flags_before is None or flags_after is None or flags_before.empty or flags_after.empty:
        return pd.DataFrame(columns=cols)
    keep = ["SKU", "content_score", *LISTING_FLAGS, "taxonomy_depth"]
    b = flags_before[[c for c in keep if c in flags_before.columns]].assign(SKU=flags_before["SKU"].astype(str)).drop_duplicates("SKU").set_index("SKU")
    a = flags_after[[c for c in keep if c in flags_after.columns]].assign(SKU=flags_after["SKU"].astype(str)).drop_duplicates("SKU").set_index("SKU")
    rows = []
    for sku in skus.astype(str).unique():
        rb, ra = (b.loc[sku] if sku in b.index else None), (a.loc[sku] if sku in a.index else None)
        if ra is None:
            continue
        changes = []
        if rb is None:
            changes.append("nuevo en el catálogo")
        else:
            for flag, label in LISTING_FLAGS.items():
                if flag in ra.index and flag in rb.index and int(ra[flag]) != int(rb[flag]):
                    changes.append(("+" if int(ra[flag]) > int(rb[flag]) else "−") + label)
            if "taxonomy_depth" in ra.index and int(ra["taxonomy_depth"]) > int(rb["taxonomy_depth"]):
                changes.append("+categoría")
        rows.append({"SKU": sku, "score_antes": None if rb is None else int(rb["content_score"]),
                     "score_despues": int(ra["content_score"]), "cambios_ficha": ", ".join(changes)})
    return pd.DataFrame(rows, columns=cols)


# ── 8. orchestration ────────────────────────────────────────────────────────
@dataclass
class SEOImpact:
    cohorts: pd.DataFrame
    skus: pd.DataFrame
    daily: pd.DataFrame
    buckets: pd.DataFrame


def build_seo_impact(work_log: pd.DataFrame | None, daily_gsc: pd.DataFrame | None,
                     visible_history: pd.DataFrame | None, ctx: pd.DataFrame | None, as_of: str,
                     window: int = WINDOW, n_boot: int = 200) -> SEOImpact:
    """Everything the page needs except the query deltas (those come from per-day files)."""
    events = events_from_work_log(work_log, visible_history)
    empty = SEOImpact(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    if events.empty:
        return empty
    d = _norm_daily(daily_gsc)
    # Keep only the SKUs whose windows can exist in the data we hold
    if not d.empty:
        events = events[events["event_day"] >= d["day"].min()]
    metrics = window_metrics(d, events, as_of, window)
    skus = events.merge(metrics.drop(columns=["event_day"]), on="SKU", how="left")
    skus["estado"] = sku_status(skus)
    if ctx is not None and "NIVEL 1" in ctx.columns:
        skus = skus.merge(ctx.assign(SKU=ctx["SKU"].astype(str))[["SKU", "NIVEL 1"]].drop_duplicates("SKU"), on="SKU", how="left")
    # Controls: one pool per cohort (pivot = the cohort's Monday + 3), metrics per distinct event day
    control_rows, control_daily = [], []
    for cohorte, part in skus[skus["completo"].fillna(False)].groupby("cohorte"):
        pivot = pd.Timestamp(cohorte) + pd.Timedelta(days=3)
        pool = control_pool(events, work_log, visible_history, ctx, pivot, window)
        if pool.empty:
            continue
        lo = part["event_day"].min() - pd.Timedelta(days=window)
        hi = part["event_day"].max() + pd.Timedelta(days=window)
        d_c = d[(d["day"] >= lo) & (d["day"] <= hi) & d["SKU"].isin(pool["SKU"])]
        for ev_day in sorted(part["event_day"].unique()):
            pev = pd.DataFrame({"SKU": pool["SKU"], "event_day": pd.Timestamp(ev_day)})
            cm = window_metrics(d_c, pev, as_of, window)
            cm["antes_completo"] = (pd.Timestamp(ev_day) - pd.Timedelta(days=window)) >= d["day"].min()
            cm["completo"] = cm["antes_completo"] & cm["despues_completo"]
            cm = cm[cm["completo"]]
            if cm.empty:
                continue
            cm = cm.merge(pool, on="SKU", how="left")
            cm["cohorte"] = cohorte
            control_rows.append(aggregate_controls(cm))
            del cm
        # Aligned daily series for the control: the pool summed per day around the cohort's median event day
        med = pd.Timestamp(part["event_day"].median()).normalize()
        sub = d_c[(d_c["day"] >= med - pd.Timedelta(days=window)) & (d_c["day"] <= med + pd.Timedelta(days=window))]
        if not sub.empty:
            g = sub.groupby("day").agg(impressions=("impressions", "sum"), clicks=("clicks", "sum")).reset_index()
            control_daily.append(g.assign(skus=int(sub["SKU"].nunique()), cohorte=cohorte, event_day=med))
    controls = pd.concat(control_rows, ignore_index=True) if control_rows else pd.DataFrame()
    if not controls.empty:
        matched = []
        for cohorte, part in skus.groupby("cohorte"):
            matched.append(match_controls(part, controls[controls["cohorte"] == cohorte]))
        skus = pd.concat(matched, ignore_index=True)
    else:
        skus = match_controls(skus, None)
    cohorts = cohort_summary(skus, as_of, n_boot)
    buckets = bucket_summary(skus, controls)
    daily = daily_index(skus, d, pd.concat(control_daily, ignore_index=True) if control_daily else None, window)
    skus["dias_a_primera_impresion"] = (skus["primera_impresion"] - skus["event_day"]).dt.days.where(skus["estado"] == "nuevo")
    return SEOImpact(cohorts, skus, daily, buckets)
