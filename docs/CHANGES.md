# Rebuild: rule changes vs v1

All rules live in `catalog_core/`. The content score, deltas and every section keep the
v1 definitions except for the fixes below, each tagged `FIX Fn` in the code.

## Fixes

| # | What was wrong in v1 | Now |
|---|---|---|
| F1 | History rows sent by `api.py` used an approximate score (estimated average, "Score = 100" always 0, taxonomy points for merely having the column, `TIENE IMAGEN = "No"` counted as having an image). | History is always written from `build_summary`, the same numbers the app shows. |
| F2 | "Stock sin visibilidad" had three definitions (KPI card, Excel sheet, stock definition). | Inventory §1 definition everywhere in Inventario (excludes Tipo C, disabled and non-physical). The "Stock Not Visible" change table is unchanged. |
| F3 | Inventory used only `TIENE STOCK`; the score uses `STOCK > 0` or `TIENE STOCK`. | One definition. They agree on all 203,595 SKUs of 2026-03-31, so no count changes. |
| F4 | Excel sheets did not match the screen: inventory sheets skipped the non-physical exclusion, "Salieron del Flujo" listed every date with repeats, "Solo Diseño/Edición" could repeat a SKU. | Excel writes the same rows as the screen (with every catalog column). New sheets: Deshabilitados con Stock, Tipo C Graduados, Con Inventario Omnicanal. |
| F5 | Section F counted only `VISIBLE == "si"` as visible. | Same yes-values as everywhere (si, sí, yes, true, 1). |
| F6 | History delta compared with the sheet's last row, so a second save in a day compared the day with itself. | Delta vs the latest earlier date; re-running a day updates its row. |
| F7 | Non-physical filter matched "certificado de regalo", but the export says "Certificados De Regalo" (217 SKUs), so gift certificates were treated as physical products. | Matches singular and plural. |
| F8 | "Score = 100" counted SKUs scoring 100, but the weights add up to 95, so it was always 0 (also in the history sheet). | "Perfect Score" counts SKUs at the maximum (95). Scores themselves are unchanged. History rows before the switch show 0. |

## Parity on real files (today 2026-03-31, yesterday 2025-12-04, productivity 2026-04-23)

`tools/legacy_capture.py` ran the unchanged v1 app; `tools/parity.py` recomputed everything
with `catalog_core`. 37 of 40 on-screen metrics and 16 of 22 Excel sheets are identical,
cell by cell. Every difference traces to a fix:

| Metric / sheet | v1 | Rebuild | Cause |
|---|---|---|---|
| Stock sin Visibilidad | 255 | 249 | F7: 6 gift certificates |
| Stock+Visible, Score<80 | 88 | 26 | F7: 62 gift certificates |
| Deshabilitados con Stock | 688 | 673 | F7: 15 gift certificates |
| Excel: Stock No Visible | 6,155 | 249 | F2/F4: now the §1 definition |
| Excel: No Fisicos | 3,802 | 4,019 | F7: +217 gift certificates |
| Excel: Long Tail Sin Modal | 9,803 | 9,797 | F4: matches screen (9,797) |
| Excel: Sin Imagen Deshabilitado | 1,070 | 1,032 | F7: 38 gift certificates |
| Excel: Salieron del Flujo | 259 | 60 | F4: matches screen (60, one date) |

Speed: v1 needed 100 s and 1.1 GB RAM for the two catalogs; the rebuild reads and
computes both in 15 s.
