# Cemaco GA4 Landing Report — Workflow n8n

Workflow automatizado que extrae datos de **Google Analytics 4** para responder
tres preguntas de negocio clave para el ecommerce de Cemaco (Guatemala). VTEX
se usa únicamente como fuente de validación del total diario, sin paginación.

---

## 1. Qué hace este workflow

### Los tres objetivos de negocio

| # | Pregunta | Dónde se responde |
|---|----------|-------------------|
| 1 | ¿Qué landing pages generan más órdenes y revenue? | Hoja `daily_detail` + Top 3 en mensaje de Teams |
| 2 | ¿Las campañas (verano, día de la madre, etc.) están convirtiendo? | Columna `campaign` en `daily_detail`, filtrable por UTM |
| 3 | ¿Qué canal trae tráfico que realmente compra? | Columna `channel` + resumen de canales en Teams y hoja `daily_summary` |

### Reporte diario vs reporte semanal

- **Reporte diario (todos los días a las 7 AM hora Guatemala):**
  Llega a Teams con el resumen del día anterior. Incluye revenue GA4,
  top 3 landings, canal líder y el estado de validación VTEX.

- **Reporte semanal (solo lunes):**
  Se envía *antes* del diario. Agrega los últimos 7 días del Excel y muestra
  tendencias de revenue, top landings acumulados y participación por canal.
  Útil para reuniones de inicio de semana.

### Cómo leer el estado de validación VTEX vs GA4

La columna `statusValidacion` compara las órdenes registradas en VTEX con
las compras reportadas por GA4 para el mismo día:

| Estado | Diferencia | Qué significa |
|--------|-----------|---------------|
| `✅ OK` | < 10 % | GA4 y VTEX están alineados. No requiere acción. |
| `⚠️ REVISAR` | 10 – 20 % | Diferencia moderada. Puede ser timing de facturación o sesiones cross-device. Revisar semanalmente. |
| `🚨 ALERTA` | > 20 % | Diferencia significativa. Ver sección Troubleshooting. |

> **Nota:** Una diferencia pequeña (< 5 %) es completamente normal porque GA4
> usa sesiones de navegador mientras VTEX registra órdenes de sistema.
> No busques el 0 %; busca estabilidad en el tiempo.

---

## 2. Pre-requisitos

### a) Credencial OAuth2 de Google en n8n

1. En n8n, ve a **Settings → Credentials → New Credential**.
2. Elige **Google Analytics OAuth2 API**.
3. Scope requerido: `https://www.googleapis.com/auth/analytics.readonly`
4. Completa Client ID y Client Secret desde
   [Google Cloud Console](https://console.cloud.google.com/) (APIs & Services → Credentials).
5. Autoriza la cuenta de servicio o usuario que tiene acceso a la propiedad GA4.

### b) AppKey y AppToken VTEX

1. En el admin de VTEX, ve a **Account Settings → Account → Application Keys**.
2. Crea una nueva clave con scope **Orders** (lectura).
3. Guarda `appKey` y `appToken` — se usan solo en el nodo VTEX Validación.

### c) Incoming Webhook de Microsoft Teams

1. En el canal de Teams destino, haz click en **···** → **Connectors**.
2. Busca y configura **Incoming Webhook**.
3. Copia la URL generada — es el valor de `TEAMS_WEBHOOK_URL`.

### d) Archivo Excel en SharePoint

1. Genera el archivo con el script incluido:
   ```bash
   pip install openpyxl
   python generate_excel_template.py
   ```
2. Sube `cemaco_ga4_landing_report.xlsx` a la biblioteca de SharePoint deseada.
3. En SharePoint, abre el archivo → **···** → **Details** → copia el **Item ID**
   (es el valor de `SHAREPOINT_FILE_ID`).

---

## 3. Variables a reemplazar

Antes de activar el workflow, reemplaza estas variables. Puedes definirlas
como **variables de entorno n8n** (recomendado) o directamente en cada nodo.

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `GA4_PROPERTY_ID` | Solo el número numérico de la propiedad GA4, sin el prefijo `properties/` | `123456789` |
| `VTEX_ACCOUNT_NAME` | Account name de VTEX (aparece en la URL del admin) | `cemaco` |
| `VTEX_APP_KEY` | AppKey generada en VTEX | `vtexappkey-cemaco-XXXXXX` |
| `VTEX_APP_TOKEN` | AppToken correspondiente a la AppKey | `XXXXXXXXXXXXXXXXXXXXXXXX` |
| `TEAMS_WEBHOOK_URL` | URL completa del Incoming Webhook de Teams | `https://outlook.office.com/webhook/...` |
| `SHAREPOINT_FILE_ID` | Item ID del archivo Excel en SharePoint | `01ABCDE...` |

Para definirlas en n8n: **Settings → Variables → Add Variable**.

---

## 4. Cómo importar y configurar

### Paso 1 — Importar el JSON

1. En n8n, haz click en **+** (nuevo workflow) o abre uno existente.
2. Menú superior → **Import from file**.
3. Selecciona `cemaco_ga4_landing_workflow.json`.
4. El workflow aparecerá con todos los nodos conectados.

### Paso 2 — Conectar la credencial OAuth2 al nodo GA4

1. Haz doble click en el nodo **GA4 Landing Pages + Conversión**.
2. En el campo **Credential**, selecciona la credencial Google Analytics
   OAuth2 que creaste en el paso 2a de Pre-requisitos.
3. Repite para el nodo **GA4 Channel Summary**.

### Paso 3 — Conectar credencial Microsoft Excel

1. Abre los nodos **Excel — daily_detail** y **Excel — daily_summary**.
2. Selecciona o crea la credencial **Microsoft Excel OAuth2**.
3. Asegúrate de que el `SHAREPOINT_FILE_ID` esté definido como variable de entorno.

### Paso 4 — Primer run manual

1. Haz click en **Execute Workflow** (botón de play).
2. Verifica que los nodos GA4 devuelvan datos (si ayer hubo tráfico).
3. Verifica que el nodo VTEX devuelva `paging.total` en el response.
4. Verifica que el Excel se actualice en SharePoint.
5. Verifica que Teams reciba el mensaje diario.

### Paso 5 — Activar el schedule

Una vez validado el run manual, activa el workflow con el toggle
**Active** en la esquina superior derecha. Correrá automáticamente
cada día a las 07:00 AM hora Guatemala (13:00 UTC).

---

## 5. Cómo interpretar los resultados

### ¿Qué es una buena tasa de conversión por landing?

El benchmark varía por tipo de página y dispositivo, pero como referencia
para retail en Guatemala:

| Tipo de landing | Conversión esperada |
|-----------------|---------------------|
| Página de producto (PDP) | 1.5 – 4 % |
| Categoría | 0.5 – 2 % |
| Página de campaña / landing promo | 2 – 6 % |
| Homepage | 0.3 – 1 % |

Si una landing con mucho tráfico tiene menos del 0.3 %, revisar:
velocidad de carga, claridad del CTA, y relevancia del tráfico que llega.

### ¿Cuándo preocuparse por el ratio paid vs orgánico?

- **Paid > 60 % del revenue:** la dependencia de paid es alta.
  Si se pausa una campaña, el revenue cae inmediatamente. Invertir en SEO
  y email para diversificar.
- **Orgánico > 70 %:** buena señal de salud SEO, pero validar que
  las campañas pagadas estén activas y midiendo correctamente.
- **Email < 5 %:** la base de datos de clientes está subaprovechada.
  Oportunidad de activar flujos de retención.

### ¿Qué hacer si el estado de validación es ALERTA?

1. **Verificar el rango de fechas:** VTEX registra por fecha de creación
   de la orden; GA4 registra por fecha de la sesión de compra. Puede haber
   diferencias de zona horaria (Guatemala = UTC-6).
2. **Revisar el filtro de status VTEX:** el workflow filtra `invoiced`.
   Si hay órdenes en `payment-pending` o `handling`, no se cuentan.
   Considera ampliar a `f_status=invoiced,handling` si el negocio lo requiere.
3. **Verificar el ecommerce tracking de GA4:** ir a GA4 →
   Reports → Monetization → Ecommerce purchases. Si hay `0` purchases,
   el tag no está disparando el evento `purchase`.
4. **Revisar devoluciones o cancelaciones:** si VTEX tiene muchas
   órdenes canceladas post-facturación, el total de VTEX bajará
   en días posteriores pero GA4 no se modifica retroactivamente.

---

## 6. Troubleshooting

### GA4 devuelve 0 purchases

**Síntoma:** el nodo `Procesar GA4 Landing Pages` calcula `totalPurchases = 0`.

**Causas y solución:**
- El evento `purchase` no está implementado en el dataLayer. Verificar en
  Google Tag Manager que el trigger `ecommerce` esté activo en las páginas
  de confirmación de orden.
- La propiedad GA4 configurada es diferente a la del sitio. Verificar
  el Property ID en GA4 → Admin → Property Settings.
- El rango de fechas no tiene datos (¿feriado? ¿sitio caído?). Ejecutar
  el workflow con `dateYesterday` seteado a un día con tráfico conocido.

### OAuth2 de Google expirado

**Síntoma:** el nodo GA4 devuelve error `401 Unauthorized`.

**Solución:**
1. En n8n, ir a **Settings → Credentials**.
2. Abrir la credencial **Google Analytics OAuth2**.
3. Hacer click en **Reconnect** y volver a autorizar con la cuenta de Google.
4. Los tokens se renuevan automáticamente en el siguiente run.

### Excel no se actualiza en SharePoint

**Síntoma:** el nodo `Excel — daily_detail` o `daily_summary` falla con
error `403 Forbidden` o `File not found`.

**Solución:**
- Verificar que la cuenta de Microsoft usada en la credencial tiene
  **permisos de edición** en la biblioteca de SharePoint donde está el archivo.
- Verificar que el `SHAREPOINT_FILE_ID` sea el Item ID correcto del archivo
  (no el Drive ID ni el Document Library ID).
- Si el archivo fue movido o renombrado, actualizar el ID en las variables.

### Teams no recibe el mensaje

**Síntoma:** el nodo `Teams — Mensaje Diario` o `Mensaje Semanal` devuelve
`400 Bad Request` o `410 Gone`.

**Solución:**
- La URL del webhook de Teams tiene una vigencia. Si expiró o el conector
  fue eliminado, crear uno nuevo en el canal de Teams y actualizar
  `TEAMS_WEBHOOK_URL`.
- Verificar que el canal de Teams no haya sido archivado.
- Para debug, copiar el body del request desde el nodo de n8n y pegarlo
  en Postman apuntando al mismo webhook URL.

> **Buena práctica n8n:** usa siempre nodos nativos de n8n
> (como `Microsoft Teams`, `Microsoft Excel`, `Google Analytics`)
> antes de recurrir a nodos HTTP Request genéricos o nodos de desarrollo
> personalizado. Los nodos nativos gestionan la autenticación OAuth2
> y los reintentos automáticamente.

---

## Arquitectura del workflow

```
Schedule Trigger (07:00 AM Guatemala)
         │
    Set Variables
    (dateYesterday, dateFrom, dateTo, isMonday, weekStart, weekEnd)
         │
    ┌────┴────────────┬──────────────────┐
    │                 │                  │
GA4 Landing      GA4 Channel        VTEX Validación
Pages Report     Summary             (1 request, sin paginación)
    │                 │                  │
Procesar GA4     Procesar GA4       Validación
Landing Pages    Canales            VTEX vs GA4
    │                 │                  │
    └────────────────►│◄─────────────────┘
                      │
              Excel daily_detail
                      │
              Excel daily_summary
                      │
                 ¿Es lunes?
                /          \
           TRUE              FALSE
             │                  │
    Leer 7 días Excel    Preparar payload
             │            diario (Teams)
    Calcular stats              │
    semanales            Teams Mensaje
             │            Diario ⚡
    Teams Mensaje
    Semanal 📊
             │
    Preparar payload
    diario (Teams)
             │
    Teams Mensaje
    Diario ⚡
```

---

*Generado para Cemaco Guatemala — Workflow GA4 Landing Report v1.0*
*Fuente principal: Google Analytics 4 | Validación: VTEX*
