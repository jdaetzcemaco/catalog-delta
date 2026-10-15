# Listas de campaña (página Campañas)

Una lista es un conjunto de SKUs que van a salir en una promoción o temporada. Como la
fecha de arranque es confidencial, no hay cuenta regresiva: el job evalúa cada lista
activa **todos los días** con el catálogo más reciente, hasta que alguien la archiva.

## Flujo

1. En **Campañas → Nueva lista** se sube un Excel/CSV con los SKUs (cualquier hoja con una
   columna `SKU`, `Código`, `ID`… o, si no hay encabezado reconocible, la columna con más
   valores numéricos). El app muestra cuántos SKUs no existen en el catálogo del día.
2. El app escribe `incoming/lists/<id>.json` (`id` = nombre en minúsculas con guiones).
   **El app nunca escribe en `processed/`**: eso lo hace solo el job, así no hay carreras.
3. El job (`Job.run_campaigns`) copia la lista a `processed/lists/<id>/list.json` y guarda la
   evaluación del día en `processed/lists/<id>/status/<day>.parquet`. En `index.json`,
   `lists[<id>]` lleva estado, `status_days` y `last_kpis`.
4. Archivar/Reactivar vuelve a escribir el JSON en `incoming/lists/` con `status`; el job lo
   re-ingiere y deja de evaluarla (o retoma).

## Checks por SKU (`catalog_core/campaigns.py::readiness`)

| Columna | Regla |
|---|---|
| `en_catalogo` | el SKU existe en el snapshot del día |
| `visible` | `VISIBLE` = sí |
| `habilitado` | `HABILITADO/DESHABILITADO` empieza con "habil" |
| `contenido` | score ≥ 80 **y** imagen **y** precio **y** nombre **y** descripción |
| `categoria_real` | `NIVEL 1` ≠ "Catalogo Completo" (Tipo C) |
| `stock` | `has_stock` del score (STOCK > 0 o TIENE STOCK) |
| `stock_confiable` | STOCK < 1,000,000 (placeholder del export desde el 22 sep 2026) |
| `listo` | todos los anteriores; si `stock_confiable` es falso, el stock **no** bloquea |

Columnas de contexto: `ultima_vez_visible`, `primera_vez_visto`, `dias_visible_seguidos`
(de `sku_history/visible.parquet`), `salida_flujo`, `fin_dat_log`, `equipo` (de
`sku_history/workflow.parquet`), `score`, `falta_imagen/precio/descripcion`.

**Regresiones**: SKUs listos en la evaluación anterior y no en la actual, con la lista de
checks que perdieron (`regressions`).

## Historial por SKU (`catalog_core/sku_history.py`)

- `visible.parquet`: por SKU `first_seen`, `last_seen`, `last_visible`, `visible_streak`,
  `days_visible`. Se actualiza incrementalmente con cada día nuevo; si llega un día fuera de
  orden se reconstruye desde todos los snapshots (solo columnas SKU y VISIBLE).
- `workflow.parquet`: última `Fecha de Salida del Flujo de trabajo`, `Fecha_Fin_Dat_Log` y
  `Fecha_Fin_Edi_Mer` por SKU y el equipo de la salida más reciente, acumulado de los
  reportes de productividad de Diseño y Edición.
