# Rendimiento (GA4 + Search Console)

Una sola cuenta de servicio de Google (`catalog-delta@g4-cemaco.iam.gserviceaccount.com`)
lee GA4 (Viewer en la propiedad 268360644), Search Console (usuario de `sc-domain:cemaco.com`)
y la hoja de historial. En Render va como `GOOGLE_SERVICE_ACCOUNT_JSON` solo en el job.

## Qué baja el job y cuándo

| Fuente | Reporte | Clave | Frescura |
|---|---|---|---|
| GA4 | `itemId`, `itemVariant`, `itemName` × vistas, carrito, compras, ingresos | **SKU = `itemVariant`** (verificado 100 %); `itemId` = PRODUCT ID | hasta ayer; se re-leen los últimos 3 días |
| GA4 | `landingPagePlusQueryString`, `sessionDefaultChannelGroup` × sesiones, sesiones con interacción | SKU extraído de la URL `/<slug>-<sku>/p` | ídem |
| Search Console | página × clics, impresiones, CTR, posición | SKU extraído de la URL | hasta hace 3 días; se re-leen 4 días |

Máximo 30 días por corrida (`PERF_MAX_DAYS_PER_PASS`), así el histórico de 90 días
(`GA4_BACKFILL_DAYS`) se completa en unas 3 corridas. Un error de cuota o permisos detiene la
fuente en esa corrida y se reintenta en la siguiente.

## Archivos

- `ga4/<day>/items.parquet`, `ga4/<day>/landing.parquet`, `gsc/<day>/pages.parquet`: crudos por día.
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
