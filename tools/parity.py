"""
Compare the rebuilt core against a legacy capture (tools/legacy_capture.py).

Every on-screen metric and every Excel sheet of the v1 app is recomputed with
catalog_core from the same files; differences are listed so each one can be traced
to an intended FIX or treated as a regression.

Usage:
    python tools/parity.py --capture data/legacy_full.pkl \
        --today data/today.xlsx --yesterday data/yesterday.xlsx \
        --diseno data/diseno.xlsx --edicion data/edicion.xlsx
"""

from __future__ import annotations

import argparse
import io
import os
import pickle
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog_core import (  # noqa: E402
    build_productivity, excel_sheets, load_catalog, load_productivity, run_catalog,
)

KEY_COLS = ["SKU", "<ID>", "Usuario", "Categoría"]


def _num(label: str, value) -> float:
    return float(str(value).replace(",", "").replace("%", "").replace("+", ""))


def new_metrics(run, prod) -> dict[tuple[str, str], float]:
    """Recompute every v1 on-screen metric, keyed by (heading or tab, label)."""
    s = run.summary.iloc[0]
    ch, inv = run.changes, run.inventory.kpis
    sku = run.sku_changes
    m = {
        ("🆕 SKU Changes (Today vs Yesterday)", "New SKUs"): len(sku["new"]),
        ("🆕 SKU Changes (Today vs Yesterday)", "Removed SKUs"): len(sku["removed"]),
        ("🆕 SKU Changes (Today vs Yesterday)", "Net SKU Change"): sku["net"],
        ("📈 Catalog Health Summary", "Total SKUs"): s["Total SKUs"],
        ("📈 Catalog Health Summary", "Visible"): s["Visible"],
        ("📈 Catalog Health Summary", "Visible %"): s["Visible %"],
        ("📈 Catalog Health Summary", "With Image %"): s["With Image %"],
        ("📈 Catalog Health Summary", "With Price %"): s["With Price %"],
        ("📈 Catalog Health Summary", "With Stock %"): s["With Stock %"],
        ("📈 Catalog Health Summary", "Avg Content Score"): s["Avg Content Score"],
        ("📈 Catalog Health Summary", "Perfect Score (100)"): s["Perfect Score"],
        ("🔄 Changes Detected", "Score Changes (±10+)"): len(ch["Score Changes"]),
        ("📦 Inventario Omnicanal", "🚫 Stock sin Visibilidad"): inv["stock_no_visible"],
        ("📦 Inventario Omnicanal", "📉 Stock+Visible, Score<80"): inv["stock_visible_score_bajo"],
        ("📦 Inventario Omnicanal", "⛔ Deshabilitados con Stock"): inv["deshabilitados_stock"],
        ("📦 Inventario Omnicanal", "🔴 Tipo C con Inventario"): inv["tipo_c_stock"],
        ("📦 Inventario Omnicanal", "🚚 Long Tail sin Modal"): inv["long_tail_sin_modal"],
        ("🔴 2. Tipo C con Inventario", "📋 Pendientes (Tipo C con Stock)"): inv["tipo_c_stock"],
        ("🔴 2. Tipo C con Inventario", "🎓 Graduados hoy"): inv.get("tipo_c_graduados", 0),
        ("🔴 2. Tipo C con Inventario", "✅ Graduados + Ya Visibles"): inv.get("tipo_c_graduados_visibles", 0),
    }
    for name in ["New SKUs", "Removed SKUs", "Newly Visible", "No Longer Visible", "Image Changes",
                 "Price Changes", "Stock Flips", "Stock Not Visible", "Top Priorities"]:
        m[("🔄 Changes Detected", name)] = len(ch[name])

    if prod is not None and prod.teams():
        u = prod.unique_counts()
        m[("A. SKUs por Usuario", "Total SKUs únicos trabajados")] = prod.user_assignments()["<ID>"].nunique()
        m[("B. SKUs Repetidos entre Equipos", "SKUs en ambos equipos")] = u["ambos"]
        m[("D. SKUs con Inventario Omnicanal", "SKUs únicos con inventario omnicanal")] = len(prod.with_omni_stock())
        m[("E. SKUs Únicos del Día", "Total SKUs únicos trabajados hoy")] = u["total"]
        m[("E. SKUs Únicos del Día", "Solo Diseño")] = u["solo_diseno"]
        m[("E. SKUs Únicos del Día", "Ambos Equipos")] = u["ambos"]
        m[("E. SKUs Únicos del Día", "Solo Edición")] = u["solo_edicion"]
        left = prod.left_workflow(prod.latest_flujo_date(), run.today_raw)
        k = prod.left_workflow_kpis(left)
        f = "F. SKUs que Salieron del Flujo"
        m[(f, "SKUs únicos salieron del flujo")] = k["total"]
        m[(f, "Con inventario omnicanal")] = k["con_inventario"]
        m[(f, "✅ Con inv. + Visibles")] = k["con_inventario_visibles"]
        m[(f, "⚠️ Con inv. + No Visibles")] = k["con_inventario_no_visibles"]
    return m


def _norm(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
        return ""
    s = str(v).strip()
    if s.lower() == "nan":
        return ""
    try:
        f = float(s)
        return repr(int(f)) if f.is_integer() else repr(round(f, 6))
    except ValueError:
        return s


def compare_sheet(name: str, old: pd.DataFrame, new: pd.DataFrame) -> list[str]:
    notes = []
    if len(old) != len(new):
        notes.append(f"rows {len(old)} -> {len(new)}")
    missing = [c for c in old.columns if c not in new.columns]
    added = [c for c in new.columns if c not in old.columns]
    if missing:
        notes.append(f"columns dropped {missing}")
    if added:
        notes.append(f"columns added {added}")
    key = next((k for k in KEY_COLS if k in old.columns and k in new.columns), None)
    if key is None:
        return notes
    ok, nk = old[key].map(_norm), new[key].map(_norm)
    only_old, only_new = set(ok) - set(nk), set(nk) - set(ok)
    if only_old:
        notes.append(f"{len(only_old)} {key}s only in v1 e.g. {sorted(only_old)[:5]}")
    if only_new:
        notes.append(f"{len(only_new)} {key}s only in rebuild e.g. {sorted(only_new)[:5]}")
    if not only_old and not only_new and ok.is_unique and nk.is_unique:
        common = [c for c in old.columns if c in new.columns and c != key]
        a = old.assign(_k=ok).set_index("_k")[common].map(_norm)
        b = new.assign(_k=nk).set_index("_k")[common].map(_norm).reindex(a.index)
        diff = (a != b)
        for c in common:
            n = int(diff[c].sum())
            if n:
                ex = diff.index[diff[c]][0]
                notes.append(f"{n} cells differ in '{c}' e.g. {key}={ex}: {a.at[ex, c]!r} -> {b.at[ex, c]!r}")
    return notes


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--capture", required=True)
    p.add_argument("--today", required=True)
    p.add_argument("--yesterday", required=True)
    p.add_argument("--diseno")
    p.add_argument("--edicion")
    args = p.parse_args()

    with open(args.capture, "rb") as fh:
        cap = pickle.load(fh)

    t0 = time.time()
    run = run_catalog(load_catalog(args.today), load_catalog(args.yesterday))
    prod = build_productivity(
        load_productivity(args.diseno) if args.diseno else None,
        load_productivity(args.edicion) if args.edicion else None,
    )
    new_sheets = excel_sheets(run, prod)
    print(f"Rebuild computed in {time.time() - t0:.0f}s (v1 took {cap['elapsed_s']:.0f}s)\n")

    print("== On-screen metrics ==")
    ours = new_metrics(run, prod)
    same = 0
    for m in cap["metrics"]:
        key = (m["heading"] or m["tab"], m["label"])
        if key not in ours:
            print(f"  ?  {key} not mapped")
            continue
        old, new = _num(m["label"], m["value"]), float(ours[key])
        if abs(old - new) > 1e-9:
            print(f"  ≠  {key[0]} / {key[1]}: {m['value']} -> {ours[key]}")
        else:
            same += 1
    print(f"  {same}/{len(cap['metrics'])} metrics identical\n")

    print("== Excel sheets ==")
    old_sheets = pd.read_excel(io.BytesIO(cap["download"]), sheet_name=None)
    for name in list(old_sheets) + [n for n in new_sheets if n not in old_sheets]:
        if name not in new_sheets:
            print(f"  ≠  {name}: missing in rebuild")
            continue
        if name not in old_sheets:
            print(f"  +  {name}: new sheet ({len(new_sheets[name])} rows)")
            continue
        new = pd.read_excel(io.BytesIO(_roundtrip(new_sheets[name])))
        notes = compare_sheet(name, old_sheets[name], new)
        mark = "≠" if notes else "="
        print(f"  {mark}  {name} ({len(old_sheets[name])} rows)" + ("".join(f"\n       - {n}" for n in notes)))
    return 0


def _roundtrip(df: pd.DataFrame) -> bytes:
    """Write through Excel like v1 did, so both sides are compared as Excel values."""
    out = io.BytesIO()
    df.to_excel(out, index=False)
    return out.getvalue()


if __name__ == "__main__":
    sys.exit(main())
