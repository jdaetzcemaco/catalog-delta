"""Team productivity: what Diseño and Edición worked on and what left the workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .text import yesno

ID, NAME, CATEGORY, USER, OMNI = "<ID>", "<Name>", "Categoría", "Usuario", "Total Omnicanal"
FLUJO_COL = "Fecha de Salida del Flujo de trabajo"
PROMOTER_COL = {"Diseño": "Usuario Promueve desde Catalogo", "Edición": "Usuario Promueve desde Compras"}


@dataclass
class ProductivityReport:
    diseno: pd.DataFrame | None
    edicion: pd.DataFrame | None

    @property
    def both(self) -> bool:
        return self.diseno is not None and self.edicion is not None

    def teams(self) -> list[tuple[str, pd.DataFrame]]:
        return [(t, df) for t, df in (("Diseño", self.diseno), ("Edición", self.edicion)) if df is not None]

    @property
    def diseno_ids(self) -> set:
        return set(self.diseno[ID]) if self.diseno is not None else set()

    @property
    def edicion_ids(self) -> set:
        return set(self.edicion[ID]) if self.edicion is not None else set()

    # ── A. SKUs por Usuario ─────────────────────────────────────────────────
    def user_assignments(self) -> pd.DataFrame:
        """(Usuario, <ID>) pairs: the worker plus whoever promoted the SKU."""
        frames = []
        for team, df in self.teams():
            frames.append(df[[USER, ID]])
            promoter = PROMOTER_COL[team]
            if promoter in df.columns:
                frames.append(df[[promoter, ID]].rename(columns={promoter: USER}).dropna(subset=[USER]))
        if not frames:
            return pd.DataFrame(columns=[USER, ID])
        return pd.concat(frames).drop_duplicates([USER, ID])

    def skus_by_user(self) -> pd.DataFrame:
        return (
            self.user_assignments().groupby(USER)[ID].count()
            .reset_index(name="SKUs Únicos")
            .sort_values("SKUs Únicos", ascending=False)
            .reset_index(drop=True)
        )

    # ── B. SKUs Repetidos entre Equipos ────────────────────────────────────
    def repeated(self) -> pd.DataFrame:
        if not self.both:
            return pd.DataFrame(columns=[ID, NAME, CATEGORY])
        ids = self.diseno_ids & self.edicion_ids
        return self.diseno[self.diseno[ID].isin(ids)][[ID, NAME, CATEGORY]].drop_duplicates(ID).reset_index(drop=True)

    # ── C. SKUs que NO pasaron por ambos equipos ──────────────────────────
    def only_in(self, team: str) -> pd.DataFrame:
        """Rows of `team` whose SKU the other team never touched (one row per SKU)."""
        if not self.both:
            return pd.DataFrame()
        df, other = (self.diseno, self.edicion_ids) if team == "Diseño" else (self.edicion, self.diseno_ids)
        # FIX F4: one row per SKU (v1 Excel could repeat a SKU)
        return df[~df[ID].isin(other)].drop_duplicates(ID).reset_index(drop=True)

    def only_in_by_category(self, team: str) -> pd.DataFrame:
        only = self.only_in(team)
        if only.empty:
            return pd.DataFrame(columns=[CATEGORY, "Cantidad"])
        return (
            only.groupby(CATEGORY)[ID].count()
            .reset_index(name="Cantidad")
            .sort_values("Cantidad", ascending=False)
            .reset_index(drop=True)
        )

    # ── D. SKUs con Inventario Omnicanal ────────────────────────────────────
    def with_omni_stock(self) -> pd.DataFrame:
        frames = [
            df[pd.to_numeric(df[OMNI], errors="coerce") > 0].assign(Team=team)[[ID, NAME, "Team", OMNI]]
            for team, df in self.teams()
        ]
        if not frames:
            return pd.DataFrame(columns=[ID, NAME, "Team", OMNI])
        return pd.concat(frames).drop_duplicates(ID).reset_index(drop=True)

    # ── E. SKUs Únicos del Día ───────────────────────────────────────────────
    def unique_counts(self) -> dict[str, int]:
        d, e = self.diseno_ids, self.edicion_ids
        return {"total": len(d | e), "solo_diseno": len(d - e), "ambos": len(d & e), "solo_edicion": len(e - d)}

    # ── F. SKUs que Salieron del Flujo ──────────────────────────────────────
    def latest_flujo_date(self) -> date | None:
        dates = [
            d for _, df in self.teams() if FLUJO_COL in df.columns
            for d in pd.to_datetime(df[FLUJO_COL], errors="coerce").dropna().dt.date
        ]
        return max(dates) if dates else None

    def left_workflow(self, on: date, catalog: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        SKUs that left the workflow on `on`, one row per SKU, with catalog VISIBLE
        joined when a catalog (SKU, VISIBLE) is given.
        """
        cols = [ID, NAME, CATEGORY, USER, FLUJO_COL, OMNI, "Team"]
        frames = []
        for team, df in self.teams():
            left = df[pd.to_datetime(df[FLUJO_COL], errors="coerce").dt.date == on].assign(Team=team)
            frames.append(left[[c for c in cols if c in left.columns]])
        if not frames:
            return pd.DataFrame(columns=cols)
        out = (
            pd.concat(frames).drop_duplicates(ID)
            .sort_values(FLUJO_COL, ascending=False)
            .reset_index(drop=True)
        )
        if catalog is not None and "VISIBLE" in catalog.columns:
            vis = catalog[["SKU", "VISIBLE"]].drop_duplicates("SKU")
            out = out.merge(vis, left_on=ID, right_on="SKU", how="left").drop(columns=["SKU"])
        return out

    @staticmethod
    def left_workflow_kpis(left: pd.DataFrame) -> dict[str, int]:
        with_stock = pd.to_numeric(left.get(OMNI, pd.Series(dtype=float)), errors="coerce") > 0
        if "VISIBLE" in left.columns:
            # FIX F5: same "yes" values as everywhere else (v1 only accepted "si")
            visible = with_stock & (yesno(left["VISIBLE"]) == 1)
        else:
            visible = pd.Series(False, index=left.index)
        return {
            "total": len(left),
            "con_inventario": int(with_stock.sum()),
            "con_inventario_visibles": int(visible.sum()),
            "con_inventario_no_visibles": int(with_stock.sum() - visible.sum()),
        }


def build_productivity(diseno: pd.DataFrame | None, edicion: pd.DataFrame | None) -> ProductivityReport:
    return ProductivityReport(diseno=diseno, edicion=edicion)
