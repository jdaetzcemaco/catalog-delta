# Alertas

Reglas deterministas calculadas por el job en cada corrida sobre resultados ya guardados
(`catalog_core/alerts.py`). Nunca leen el export crudo, así que son baratas y reproducibles.
Se guardan en `processed/alerts/<day>.json` para el último día de catálogo; `first_seen`
se conserva de un día al siguiente mientras la alerta siga activa.

| Regla | Severidad | Dispara cuando | Umbral (`ALERT_*`) |
|---|---|---|---|
| `list_regression` | alta | SKUs de una lista activa estaban listos ayer y hoy no | — |
| `list_low_ready` | media | lista activa con % listos por debajo del mínimo | `LIST_MIN_READY_PCT` = 80 |
| `quality_stock_placeholder` | alta | % de SKUs con STOCK ≥ 1,000,000 | `STOCK_PLACEHOLDER_PCT` = 1 |
| `visible_drop` | alta | SKUs visibles cayeron vs día anterior | `VISIBLE_DROP_PCT` = 5 |
| `score_drop` | media | score promedio bajó vs día anterior | `SCORE_DROP_POINTS` = 1 |
| `stock_no_visible_jump` | media | "stock sin visibilidad" creció (y ≥ 10 SKUs) | `STOCK_NO_VISIBLE_JUMP_PCT` = 25 |
| `new_visible_weak` | media | SKUs nuevos visibles con score < 80 | `NEW_VISIBLE_WEAK_MIN` = 20 |
| `tipo_c_stuck` | media | Tipo C con stock con ≥ N días en el catálogo | `TIPO_C_MIN_DAYS` = 14 |
| `export_late` | media | el último catálogo tiene más de N días | `EXPORT_LATE_DAYS` = 1 |

Los umbrales se cambian con variables de entorno en Render (p. ej. `ALERT_VISIBLE_DROP_PCT=3`).

## En el app

- **Resumen** muestra arriba las alertas pendientes del día con enlace a **Alertas**.
- **Alertas**: por severidad, cada una con detalle, enlace a la página que la explica, los SKUs
  afectados (hasta 200; la tabla completa está en la página origen) y **Marcar revisada**.
  Las revisiones se guardan en `processed/alerts/ack.json`, el único archivo de `processed/`
  que escribe el app (el job nunca lo toca).
- Cuando haya datos de GA4 (Fase 2) se agregan reglas de tráfico (visible con vistas y sin
  stock, caída de sesiones orgánicas) con ventanas de varios días.

## Pendiente

- Entrega fuera del tablero: correo diario o Teams (Incoming Webhook) con las alertas nuevas.
