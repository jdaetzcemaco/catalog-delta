# Preguntar (agente con Claude)

Página **Comercial → Preguntar**. Un chat que responde preguntas sobre los resultados ya
procesados por el job. El agente **no calcula ni escribe datos**: llama herramientas de solo
lectura (`app/agent.py`) sobre los mismos loaders cacheados de `app/data.py` y explica lo
que devuelven. Cada respuesta muestra «Datos consultados» con la entrada y el resultado de
cada herramienta, para auditar las cifras.

## Herramientas

| Herramienta | Qué devuelve |
|---|---|
| `resumen_dia` | KPIs del día vs el anterior, Inventario Omnicanal, calidad del export, filas por tabla |
| `cambios` | Filas de una tabla de Cambios o de Inventario (máx. 200) |
| `alertas` | Alertas del día, con muestra de SKUs y si ya están revisadas |
| `sku` | Ficha: catálogo, historial de visibilidad, GA4/GSC, CRO, listas donde está, usuarios que lo trabajaron |
| `trabajo` | Quién trabajó y quién promovió un SKU (o qué SKUs trabajó una persona), de los reportes de productividad de Diseño y Edición |
| `buscar_sku` | Búsqueda por código, nombre o marca (sin tildes) |
| `lista` | Listas de campaña; detalle con fallas por check y SKUs no listos |
| `ranking_skus` | Ranking por cualquier métrica de rendimiento, con filtros |
| `historial` | Serie diaria de KPIs |
| `cro` | Conteo por grupo CRO o los SKUs de un grupo por impacto |

Cada resultado se limita a 200 filas y ~40k caracteres. Los textos del catálogo se tratan
como datos no confiables (el system prompt prohíbe seguir instrucciones dentro de ellos).

## Costo y tope

- Modelo `claude-opus-5-5` (US$4 entrada / US$20 salida por millón de tokens; lectura de
  caché US$0.20). Prompt caching automático sobre system prompt, herramientas y conversación.
- **`AGENT_DAILY_BUDGET_USD`** (por defecto 5) es un **máximo diario**, no un gasto fijo.
  El costo de cada llamada se calcula de `usage` y se suma en
  `processed/agent/usage-<día>.json` (hora de Guatemala), compartido por todos los usuarios.
  Antes de cada llamada se verifica que el gasto del día más el peor caso de una llamada
  quepa en el tope; si no, el chat se pausa hasta el día siguiente.
- Límites por pregunta: 8 llamadas al modelo y 8,000 tokens de salida por llamada.
  Límite por sesión: 30 preguntas.
- **Fallback del lado del servidor activado** (`fallbacks: "default"`): si un clasificador
  de seguridad rechaza una pregunta por error, la API la reintenta con el modelo de respaldo
  recomendado. Se cobra a la tarifa de ese modelo, y el cálculo de costo lo contempla.

## Configuración

Solo en el servicio web: `ANTHROPIC_API_KEY` (secreto) y `AGENT_DAILY_BUDGET_USD`.
Opcional `AGENT_MODEL` para cambiar el modelo.

## Historial de trabajo por SKU

El job guarda `processed/sku_history/work_log.parquet`: una fila por SKU y por reporte diario de
productividad (Diseño y Edición) con el usuario que lo trabajó, quien lo promovió y las fechas del
flujo. Se arma con todos los reportes guardados y se actualiza cuando llega uno nuevo
(`index["sku_history"]["work_log_files"]`). Cubre solo los días con reporte: desde el 23 de
septiembre de 2026, cuando empezó la lectura de los correos de STEP, más un reporte suelto del 20
de marzo.
