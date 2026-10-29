# Impacto SEO del trabajo de catálogo

¿Cambia la presencia en Google de un SKU cuando Edición o Diseño trabajan su ficha? La página
**Impacto SEO** responde con una medición por cohortes, no con la vista por URL de Search Console
(que no separa el efecto del trabajo del ruido de temporada ni dice qué cambió).

## Fuentes

| Dato | De dónde | Archivo |
|---|---|---|
| Evento por SKU | work log de productividad (`sku_history/work_log.parquet`): última **salida del flujo** de STEP, si no, fin Dat_Log | `events_from_work_log` |
| Presencia en Google | Search Console diario por SKU (`perf/daily_gsc.parquet`), búsqueda WEB, página de producto | `window_metrics` |
| Consultas por SKU | export de Search Console a BigQuery (`gsc/<day>/queries.parquet`, solo días con fuente `bq`) | `query_deltas` |
| Cambios en la ficha | snapshot del catálogo anterior y posterior a la ventana (`build_flags`) | `listing_changes` |
| Visibilidad | `sku_history/visible.parquet` (`first_seen`, `last_visible`) | estado `no_visible`, control |

### Search Console desde BigQuery

Desde el 27-ago-2026 el export en bloque de Search Console vive en BigQuery (proyecto
`cemaco-content-system`, dataset `searchconsole`, tabla `searchdata_url_impression`). El job lo
usa para todo día ≥ `GSC_BQ_START` cuando tiene credenciales con acceso (`pipeline/gsc_bq.py`,
REST `jobs.query`, una partición por consulta ≈ 35–50 MB, tope `maximumBytesBilled` 2 GB). Los días
anteriores, o sin BigQuery, siguen viniendo de la API (`pipeline/gsc.py`). `index.json` guarda la
fuente por día en `gsc.sources`.

Por qué: para el 10-oct-2026 la API (dimensión `page`) devolvió 2,325 SKUs / 63k impresiones /
6,250 clics; el export, 19,014 SKUs / 175k impresiones / 8,830 clics solo WEB, más la consulta, el
dispositivo y los flags de *product snippets* / *merchant listings*. **Ruptura de serie**: al pasar a
BigQuery los clics e impresiones de Rendimiento y Campañas suben ~1.4× / ~2.8× por cobertura, no por
tráfico. `position = sum_position / impressions + 1` (el export es base cero).

Columnas nuevas en `perf/daily_gsc.parquet`: `impressions_ml`, `impressions_ps`, `impressions_mobile`,
`consultas` (consultas no anónimas distintas); cero en días de API.

Acceso: la cuenta `catalog-delta@g4-cemaco` necesita *BigQuery Data Viewer* en el dataset y
*BigQuery Job User* en el proyecto (o se pasa otra llave en `GSC_BQ_SERVICE_ACCOUNT_JSON`).
`python3 tools/check_gsc_bq.py 2026-10-10` comprueba el acceso y la cobertura.

Primer despliegue con BigQuery: `python3 tools/reset_gsc_days.py --apply` (con las mismas variables de
almacenamiento que el job) borra de `index.json` los días ≥ `GSC_BQ_START` que vinieron de la API; el job los
vuelve a bajar desde BigQuery a razón de 30 días por pasada (cada 30 min) y reemplaza las filas de
`perf/daily_gsc.parquet`. Clics e impresiones de Rendimiento y Campañas suben ~1.4× / ~2.8× por cobertura.

## Metodología (`catalog_core/seo_impact.py`)

1. **Evento**: último `salida_flujo` por SKU (fallback `fin_dat_log`). Cada SKU cuenta una vez.
   `equipo` = Diseño / Edición / **Diseño+Edición** (ambos reportes con ≤ 7 días de diferencia).
   `cohorte` = lunes de la semana ISO del evento.
2. **Ventanas**: 14 días antes (−14…−1) y 14 después (+1…+14); el día del evento no cuenta. Días sin
   fila = 0 impresiones. La ventana «después» incompleta queda en NaN (estado `pendiente`), nunca una
   suma parcial que parezca caída. Posición ponderada por impresiones.
3. **Estado por SKU**: `no_visible` (no visible en el sitio tras el evento) → `sin_presencia` (0 y 0) →
   `nuevo` (0 antes, > 0 después y primera impresión posterior al evento) → `bajo` (≤ −20 % y ≥ 10
   impresiones menos, o perdió toda presencia) → `crecio` (≥ +20 % y ≥ 10 más) → `estable`.
4. **Control**: SKUs visibles antes y después de la ventana, sin evento en ±21 días, mismas fechas;
   media estratificada determinista por (día del evento, tramo de impresiones previas `0 / 1–9 /
   10–99 / 100–999 / 1000+`[, Nivel 1 si el estrato tiene ≥ 20 controles]).
   **Δ vs control** = Δ % trabajados − Δ % control (difference-in-differences). Intervalo de confianza
   95 % por bootstrap sobre SKUs (200 remuestreos, semilla fija). `muestra_pequena` si < 30 SKUs con
   presencia.
5. **Consultas**: por (SKU, consulta) antes/después: `nueva`, `perdida`, `subio` / `bajo` (≥ 3
   posiciones con ≥ 10 impresiones), `estable`. Solo cohortes completas y días con fuente `bq`.
6. **Cambios en la ficha**: flags del snapshot anterior al inicio de la ventana vs el primero
   posterior al final (`+descripción`, `+imagen`, `−marca`, `+categoría`, `nuevo en el catálogo`).

## Archivos (`perf/seo_*.parquet`, escritos por `Job.refresh_seo_impact`)

| Archivo | Grano | Claves |
|---|---|---|
| `seo_cohorts` | (cohorte, equipo) + fila «Todos» | conteos por estado, sumas antes/después, Δ %, control, uplift e IC, posiciones, `tasa_nuevos`, `completo`, `muestra_pequena` |
| `seo_skus` | SKU con evento | evento, equipo, usuarios, ventanas, estado, control y uplift, `dias_a_primera_impresion`, consultas nuevas/perdidas, `top_consulta_despues`, `cambios_ficha`, score antes/después |
| `seo_buckets` | (cohorte, equipo, tramo) | trabajados vs control por tramo |
| `seo_daily` | (cohorte, equipo, grupo, offset −14…14) | impresiones diarias alineadas al evento, `indice` (promedio previo = 100) |
| `seo_queries` | (SKU, consulta) | antes/después, estado, flags |
| `seo_listing` | (SKU, cohorte) | cambios en la ficha (caché) |

Se recalcula cuando hay días nuevos de Search Console, un reporte de productividad nuevo o cambia
`SEO_VERSION`; consultas y cambios de ficha solo para cohortes completas no procesadas
(`index.seo.queries_cohorts`), podadas a 120 días.

## Cómo leerlo (y qué no dice)

- **Nuevos en Google** = indexación. **Δ vs control** = cobertura (más consultas, más impresiones).
  **Posición** = ranking: con el trabajo actual casi no se mueve; la ganancia es de cobertura.
- **Sin presencia** es la lista accionable: visibles y que Google no muestra 14 días después.
- Mínimo detectable: con ~600 SKUs/semana de Edición y ~30 % con presencia, ±10 % de impresiones se
  lee por semana; Diseño (≈50 con presencia) solo acumulando varias semanas.
- `salida_flujo` es la salida de STEP, no la publicación en VTEX; `visible.parquet` no tiene serie
  diaria, así que «visible durante la ventana» del control es aproximado.
- Referencia (26-oct-2026, cohorte 22-sep…8-oct, 3,153 SKUs): impresiones +12 % y clics +19 % frente
  a −6 %…−10 % / −4 %…−8 % del control; 111 nuevos en Google; 68 % sin presencia a los 14 días.
