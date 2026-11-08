# Dashboard Mercado & Macro
- Stack: FastAPI en Railway + PostgreSQL + TailwindCSS en /static.
- Endpoints:
  - /bcra/variables: Datos scrapeados de indicadores.ar y datos.gob.ar (agrupado y lista).
  - /dolar: Cotizaciones en vivo vía dolarapi.com.
  - /mercado/resumen: Spreads de brechas.
- UI: Modo oscuro con tarjetas categorizadas, brechas calculadas contra Dólar Oficial minorista, tasas con TNA y auto-refresco cada 60s.
