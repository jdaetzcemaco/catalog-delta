# Rendimiento (GA4 + Search Console)

Una sola cuenta de servicio de Google (`catalog-delta@g4-cemaco.iam.gserviceaccount.com`)
lee GA4 (Viewer en la propiedad 268360644), Search Console (usuario de `sc-domain:cemaco.com`)
y la hoja de historial. En Render va como `GOOGLE_SERVICE_ACCOUNT_JSON` solo en el job.

## Qué baja el job y cuándo

| Fuente | Reporte | Clave | Frescura |
|---|---|---|---|
| GA4 | `itemId`, `itemVariant`, `itemName` × vistas, carrito, compras, ingresos | **SKU = `itemVariant`** (verificado 100 %); `itemId` = PRODUCT ID | hasta ayer; se re-leen los últimos 3 días |
| GA4 | `landingPagePlusQueryString`, `sessionDefaultChannelGroup` × sesiones, sesiones con interacción | SKU extraído de la URL `/<slug>-<sku>/p` | ídem |
| Search Console | **export a BigQuery** (desde 27-ago-2026): URL × clics, impresiones, posición, consultas, flags de snippet/Merchant; antes de esa fecha, o sin acceso a BigQuery, la API por página | SKU extraído de la URL | hasta hace 2 días (3 con la API); se re-leen 4 días. Ver docs/SEO.md |

Máximo 30 días por corrida (`PERF_MAX_DAYS_PER_PASS`), así el histórico de 90 días
(`GA4_BACKFILL_DAYS`) se completa en unas 3 corridas. Un error de cuota o permisos detiene la
fuente en esa corrida y se reintenta en la siguiente.

## Archivos

- `ga4/<day>/items.parquet`, `ga4/<day>/landing.parquet`, `gsc/<day>/pages.parquet` (+ `queries.parquet`
  en días de BigQuery): crudos por día.
- `perf/seo_*.parquet`: impacto SEO del trabajo de catálogo (docs/SEO.md).
- `perf/daily_item.parquet`, `perf/daily_landing.parquet`, `perf/daily_gsc.parquet`: una fila por
  (día, SKU); el job los actualiza y recorta a 120 días.
- `perf/sku_perf.parquet`: **lo único que lee el app**. Por SKU del catálogo más reciente: sumas
  7/30/90 días de cada métrica, semana anterior (`*_7d_prev`), `ultimo_dia_vistas`, tasas de
  carrito y compra a 30 días, y `posicion_30d` (ponderada por impresiones).

## En el app

- **Rendimiento**: KPIs por ventana (7/30/90), Top productos, *Tráfico sin stock* (visibles con
  vistas y sin stock confiable), *Stock sin tráfico* (visibles con stock y 0 vistas), *SEO*
  (oportunidades: posición 4–20 con ≥ 100 impresiones) y ficha por SKU.
- **Campañas**: vistas, carrito, orgánicas, clics y posición por SKU de la lista, más KPIs de
  tráfico de la lista.
- **Alertas**: `views_no_stock` (alta) y `organic_drop` por lista (media). Umbrales `ALERT_*`.

GA4 recorta filas de muy bajo volumen y cierra el día 24–48 h después: un cero en vistas no es un
dato duro, por eso ninguna regla alerta por "cero" de un solo día.

## Canales y CRO

- **Canal por SKU**: el job baja también `itemVariant × sessionDefaultChannelGroup` por día y lo
  agrupa en *orgánico*, *pagado Google* (Search, Shopping, PMax, Display, Video), *pagado social*,
  *directo* y *otros*. `perf/sku_perf.parquet` lleva `vistas/carrito/compras/ingresos_30d_<canal>`.
- **Tasas por persona**: `itemsAddedToCart` / `itemsPurchased` cuentan **unidades** (50 cajas de piso
  = 50), así que el CRO usa las tasas de GA4 `cartToViewRate` y `purchaseToViewRate` (personas que
  agregaron / compraron ÷ personas que vieron) sobre una ventana de 30 días, una vez al día
  (`perf/window_rates.parquet`, `perf/window_channel_rates.parquet`), topadas en 100 %.
- **Clasificación** (`catalog_core/cro.py`, resultado en `perf/cro.parquet`): cada SKU físico se
  compara con su Nivel 1; tasas ajustadas por volumen (bayes empírico, 100 vistas a la tasa de la
  categoría). Grupos: fuga de conversión, carrito sin compra, pagado sin retorno, joya oculta,
  orgánico en riesgo, tráfico sin stock; umbrales en `CROThresholds`.
- **Página CRO**: grupos con motivo, impacto estimado y acción sugerida; fijar SKUs en la lista
  «En la mira» (se evalúa a diario en Campañas); contenido vs conversión; embudo por canal.
