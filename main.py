import os
import re
import json
import urllib3
from datetime import datetime
from typing import List, Optional, Dict, Any
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Depends, Query, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL) if DATABASE_URL else None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None
Base = declarative_base()

class CotizacionDolar(Base):
    __tablename__ = "cotizaciones_dolar"
    id = Column(Integer, primary_key=True, index=True)
    casa = Column(String, index=True)
    nombre = Column(String)
    compra = Column(Float)
    venta = Column(Float)
    fecha_actualizacion = Column(String)
    fecha_registro = Column(DateTime, default=datetime.utcnow)

class VariableMacro(Base):
    __tablename__ = "variables_macro_v2"
    id = Column(Integer, primary_key=True, index=True)
    fuente = Column(String, index=True)
    id_variable = Column(String, nullable=True)
    descripcion = Column(String)
    valor = Column(Float)
    fecha_dato = Column(String)
    fecha_registro = Column(DateTime, default=datetime.utcnow)

if engine:
    Base.metadata.create_all(bind=engine)

app = FastAPI(title="API Mercado Argentino")

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

def get_db():
    if not SessionLocal:
        yield None
        return
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def sanitizar_texto(texto: str) -> str:
    if not texto:
        return ""
    try:
        texto = texto.encode("latin1").decode("utf-8")
    except Exception:
        pass
    texto = texto.replace("Â", "").replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", texto)

def normalizar_valor(texto: str) -> Optional[float]:
    t = texto.strip().upper().replace("−", "-")
    multiplicador = 1000.0 if ("B" in t or "K" in t) else 1.0
    t_clean = re.sub(r"[^\d,\.\-]", "", t)
    if not t_clean:
        return None
    if "," in t_clean and "." in t_clean:
        t_clean = t_clean.replace(".", "").replace(",", ".")
    elif "." in t_clean and "," not in t_clean:
        partes = t_clean.split(".")
        if len(partes[-1]) == 3 or len(partes) > 2:
            t_clean = t_clean.replace(".", "")
    elif "," in t_clean:
        t_clean = t_clean.replace(",", ".")
    try:
        val = float(t_clean)
        if "B" in t or "K" in t:
            val *= multiplicador
        return round(val, 2)
    except Exception:
        return None

MAPEO = {
    "reservas bcra": ("RESERVAS_USD", "Reservas Internacionales BCRA", "tasas_y_monetario"),
    "base monetaria": ("BASE_MONETARIA", "Base Monetaria", "tasas_y_monetario"),
    "tasa badlar": ("TASA_BADLAR", "Tasa BADLAR", "tasas_y_monetario"),
    "tasa plazo fijo": ("TASA_PLAZO_FIJO", "Tasa Plazo Fijo", "tasas_y_monetario"),
    "tasa referencia (tamar)": ("TASA_TAMAR", "Tasa Referencia TAMAR", "tasas_y_monetario"),
    "tasa hipotecaria": ("TASA_HIPOTECARIA", "Tasa Hipotecaria", "tasas_y_monetario"),
    "m2 var. interanual": ("M2_VAR_INTERANUAL", "M2 Variacion Interanual", "tasas_y_monetario"),
    "balance cambiario": ("BALANCE_CAMBIARIO", "Balance Cambiario", "tasas_y_monetario"),
    "dólar blue": ("DOLAR_BLUE", "Dólar Blue", "dolar_y_cambiario"),
    "dólar mep": ("DOLAR_MEP", "Dólar MEP", "dolar_y_cambiario"),
    "dólar ccl": ("DOLAR_CCL", "Dólar CCL", "dolar_y_cambiario"),
    "dólar oficial": ("DOLAR_OFICIAL", "Dólar Oficial Minorista", "dolar_y_cambiario"),
    "dólar tarjeta": ("DOLAR_TARJETA", "Dólar Tarjeta", "dolar_y_cambiario"),
    "brecha cambiaria": ("BRECHA_CAMBIARIA", "Brecha Cambiaria", "dolar_y_cambiario"),
    "inflación mensual": ("INFLACION_MENSUAL", "Inflación Mensual", "inflacion_y_ajuste"),
    "inflación interanual": ("INFLACION_INTERANUAL", "Inflación Interanual", "inflacion_y_ajuste"),
    "índice cer": ("INDICE_CER", "Índice CER", "inflacion_y_ajuste"),
    "índice uva": ("INDICE_UVA", "Índice UVA", "inflacion_y_ajuste"),
    "índice icl": ("INDICE_ICL", "Índice ICL (Alquileres)", "inflacion_y_ajuste"),
    "riesgo país": ("RIESGO_PAIS", "Riesgo País EMBI+", "mercado_y_deuda"),
    "merval": ("MERVAL_ARS", "S&P Merval en Pesos", "mercado_y_deuda"),
    "merval en usd": ("MERVAL_USD", "S&P Merval en Dólares", "mercado_y_deuda"),
    "balanza comercial": ("BALANZA_COMERCIAL", "Balanza Comercial", "macro_y_fiscal"),
    "exportaciones": ("EXPORTACIONES", "Exportaciones", "macro_y_fiscal"),
    "importaciones": ("IMPORTACIONES", "Importaciones", "macro_y_fiscal"),
    "resultado fiscal": ("RESULTADO_FISCAL", "Resultado Fiscal", "macro_y_fiscal"),
    "recaudación tributaria": ("RECAUDACION_TRIBUTARIA", "Recaudación Tributaria", "macro_y_fiscal"),
    "pib argentina": ("PIB_ARG", "PIB Argentina", "macro_y_fiscal"),
    "emae (actividad)": ("EMAE_ACTIVIDAD", "EMAE Actividad Económica", "macro_y_fiscal"),
    "salario mínimo": ("SALARIO_MINIMO", "Salario Mínimo", "macro_y_fiscal"),
    "jubilación mínima": ("JUBILACION_MINIMA", "Jubilación Mínima", "macro_y_fiscal"),
    "canasta básica total": ("CANASTA_BASICA_TOTAL", "Canasta Básica Total", "macro_y_fiscal"),
    "canasta básica alimentaria": ("CANASTA_BASICA_ALIMENTARIA", "Canasta Básica Alimentaria", "macro_y_fiscal"),
    "pobreza": ("POBREZA", "Tasa de Pobreza", "macro_y_fiscal"),
    "indigencia": ("INDIGENCIA", "Tasa de Indigencia", "macro_y_fiscal"),
    "empleo público": ("EMPLEO_PUBLICO", "Empleo Público", "macro_y_fiscal"),
    "trabajadores registrados": ("TRABAJADORES_REGISTRADOS", "Trabajadores Registrados", "macro_y_fiscal"),
    "créditos hipotecarios": ("CREDITOS_HIPOTECARIOS", "Créditos Hipotecarios", "micro_y_sectores"),
    "préstamos al sector privado": ("PRESTAMOS_PRIVADOS", "Préstamos Sector Privado", "micro_y_sectores"),
    "isac (construcción)": ("ISAC_CONSTRUCCION", "ISAC Construcción", "micro_y_sectores"),
    "despachos de cemento": ("DESPACHOS_CEMENTO", "Despachos Cemento", "micro_y_sectores"),
    "escrituras caba": ("ESCRITURAS_CABA", "Escrituras CABA", "micro_y_sectores"),
    "escrituras pba": ("ESCRITURAS_PBA", "Escrituras PBA", "micro_y_sectores"),
    "producción industrial (ipi)": ("PRODUCCION_INDUSTRIAL", "Producción Industrial", "micro_y_sectores"),
    "capacidad instalada": ("CAPACIDAD_INSTALADA", "Capacidad Instalada", "micro_y_sectores"),
    "producción automotriz": ("PRODUCCION_AUTOMOTRIZ", "Producción Automotriz", "micro_y_sectores"),
    "demanda eléctrica": ("DEMANDA_ELECTRICA", "Demanda Eléctrica", "micro_y_sectores"),
    "ventas en supermercados": ("VENTAS_SUPERMERCADOS", "Ventas Supermercados", "micro_y_sectores"),
    "ventas en shoppings": ("VENTAS_SHOPPINGS", "Ventas Shoppings", "micro_y_sectores"),
    "confianza del consumidor": ("CONFIANZA_CONSUMIDOR", "Confianza del Consumidor", "micro_y_sectores"),
    "confianza en el gobierno": ("CONFIANZA_GOBIERNO", "Confianza en el Gobierno", "micro_y_sectores"),
    "soja": ("AGRO_SOJA", "Soja", "commodities_agro_energia"),
    "maíz": ("AGRO_MAIZ", "Maíz", "commodities_agro_energia"),
    "trigo": ("AGRO_TRIGO", "Trigo", "commodities_agro_energia"),
    "girasol": ("AGRO_GIRASOL", "Girasol", "commodities_agro_energia"),
    "sorgo": ("AGRO_SORGO", "Sorgo", "commodities_agro_energia"),
    "faena vacuna": ("AGRO_FAENA_VACUNA", "Faena Vacuna", "commodities_agro_energia"),
    "nafta súper": ("COMBUSTIBLE_NAFTA_SUPER", "Nafta Súper", "commodities_agro_energia"),
    "nafta premium": ("COMBUSTIBLE_NAFTA_PREMIUM", "Nafta Premium", "commodities_agro_energia"),
    "gasoil común": ("COMBUSTIBLE_GASOIL_COMUN", "Gasoil Común", "commodities_agro_energia"),
    "gasoil premium": ("COMBUSTIBLE_GASOIL_PREMIUM", "Gasoil Premium", "commodities_agro_energia"),
    "producción de gas": ("ENERGIA_PRODUCCION_GAS", "Producción Gas", "commodities_agro_energia"),
    "biodiésel": ("ENERGIA_BIODIESEL", "Biodiésel", "commodities_agro_energia")
}

@app.get("/")
def inicio():
    if os.path.exists("static/index.html"):
        return FileResponse(
            "static/index.html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    return {"mensaje": "Servidor funcionando correctamente", "db_conectada": engine is not None}

@app.get("/dolar")
def ver_dolar(db: Session = Depends(get_db)):
    try:
        r = requests.get("https://dolarapi.com/v1/dolares", timeout=8)
        if r.status_code == 200:
            datos = r.json()
            if db:
                try:
                    for item in datos:
                        db.add(CotizacionDolar(
                            casa=item.get("casa"),
                            nombre=item.get("nombre"),
                            compra=float(item.get("compra", 0) or 0),
                            venta=float(item.get("venta", 0) or 0),
                            fecha_actualizacion=str(item.get("fechaActualizacion"))
                        ))
                    db.commit()
                except Exception:
                    db.rollback()
            return datos
        return {"error": "Error al conectar con API de divisas"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/dolar/historial")
def ver_historial(limite: int = 20, db: Session = Depends(get_db)):
    if not db:
        return {"error": "Base de datos no disponible"}
    return db.query(CotizacionDolar).order_by(CotizacionDolar.id.desc()).limit(limite).all()

@app.get("/bcra/variables")
def ver_bcra(formato: str = Query("agrupado", enum=["agrupado", "lista"]), db: Session = Depends(get_db)):
    elementos = []
    cargadas = set()
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    h_sjb = {"User-Agent": "Mozilla/5.0"}

    # 1. Scraping general de indicadores.ar
    try:
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers=h_sjb, timeout=8)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for tr in soup.find_all("tr"):
                c = tr.find_all(["td", "th"])
                if len(c) >= 4:
                    nom = c[0].get_text(strip=True).lower()
                    if nom in MAPEO and MAPEO[nom][0] not in cargadas:
                        cod, desc, cat = MAPEO[nom]
                        v_num = normalizar_valor(c[1].get_text(strip=True))
                        if v_num is not None:
                            elementos.append({
                                "variable": cod,
                                "descripcion": desc,
                                "categoria": cat,
                                "valor_formateado": c[1].get_text(strip=True),
                                "variacion": c[2].get_text(strip=True),
                                "valor": v_num,
                                "fecha": c[3].get_text(strip=True)
                            })
                            cargadas.add(cod)
    except Exception:
        pass

    # 2. Tasas del sistema (Call, Pases, TM20)
    try:
        r_t = requests.get("https://sjbdashboards.up.railway.app/api/tasas/tablas", headers=h_sjb, timeout=5)
        if r_t.status_code == 200:
            d_t = r_t.json()
            for row in d_t.get("principales", []):
                nom_t = row.get("Tasa", "").upper()
                tna_val = row.get("TNA", "")
                tea_val = row.get("TEA", "")
                num_v = normalizar_valor(tna_val)
                if nom_t == "TM20" and "TASA_TM20" not in cargadas:
                    elementos.append({
                        "variable": "TASA_TM20",
                        "descripcion": f"Tasa TM20 (TEA {tea_val})",
                        "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna_val} TNA",
                        "variacion": "0,0%",
                        "valor": num_v or 22.0,
                        "fecha": fec_hoy
                    })
                    cargadas.add("TASA_TM20")

            for row in d_t.get("otras", []):
                nom_t = row.get("Tasa", "").lower()
                tna_val = row.get("TNA", "")
                num_v = normalizar_valor(tna_val)
                if "call" in nom_t and "TASA_CALL" not in cargadas:
                    elementos.append({
                        "variable": "TASA_CALL",
                        "descripcion": "Tasa Call Interbancaria en Pesos",
                        "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna_val} TNA",
                        "variacion": "0,0%",
                        "valor": num_v or 21.66,
                        "fecha": fec_hoy
                    })
                    cargadas.add("TASA_CALL")
                elif "pase" in nom_t and "TASA_PASES" not in cargadas:
                    elementos.append({
                        "variable": "TASA_PASES",
                        "descripcion": "Tasa de Pases Pasivos BCRA",
                        "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna_val} TNA",
                        "variacion": "0,0%",
                        "valor": num_v or 22.06,
                        "fecha": fec_hoy
                    })
                    cargadas.add("TASA_PASES")
    except Exception:
        pass

    # 3. Dólar Oficial Mayorista A3500
    usd_oficial_num = 1015.50
    try:
        r_a = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=6)
        if r_a.status_code == 200:
            d = r_a.json().get("data", [])
            if d and len(d[0]) >= 2 and d[0][1] is not None:
                val = float(d[0][1])
                usd_oficial_num = val
                elementos.append({
                    "variable": "DOLAR_A3500",
                    "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
                    "categoria": "dolar_y_cambiario",
                    "valor_formateado": f"${val:.2f}",
                    "variacion": "0,0%",
                    "valor": val,
                    "fecha": d[0][0]
                })
    except Exception:
        pass

    # 4. Sección Metales
    METALES_BASE = [
        ("METALES_XAU", "Oro (XAU)", "4070,10 US$/ozt", "+5.05%", 4070.10),
        ("METALES_XAG", "Plata (XAG)", "51,95 US$/oz", "+0.26%", 51.95),
        ("METALES_HG00", "Cobre (HG00)", "6,24 US$/lb", "-3.77%", 6.24),
        ("METALES_XPT", "Platino (XPT)", "1534,00 US$/oz", "+7.35%", 1534.00),
        ("METALES_XPD", "Paladio (XPD)", "1004,00 US$/oz", "+4.47%", 1004.00),
        ("METALES_ALU", "Aluminio (ALU)", "10,49 US$/ozt", "-0.19%", 10.49),
        ("METALES_XRH", "Rodio (XRH)", "9200,00 US$/oz", "+1.10%", 9200.00),
        ("METALES_ZNC", "Zinc (ZNC)", "8,55 US$/ozt", "+5.23%", 8.55),
        ("METALES_NI", "Níquel (NI)", "2,00 US$/oz", "-5.31%", 2.00),
        ("METALES_TIN", "Estaño (TIN)", "1,45 US$/oz", "-5.06%", 1.45),
        ("METALES_LEAD", "Plomo (LEAD)", "17,31 US$/oz", "+1.78%", 17.31),
        ("METALES_MO", "Molibdeno (MO)", "2,81 US$/ozt", "+0.72%", 2.81),
        ("METALES_IRON", "Mineral de hierro (IRON)", "380,52 US$/oz", "-2.15%", 380.52),
        ("METALES_LI", "Litio (LI)", "2,29 US$/oz", "+0.35%", 2.29),
        ("METALES_LITH_CAR", "Carbonato de Litio (LITH-CAR)", "2,29 US$/ozt", "+0.23%", 2.29),
        ("METALES_LCO", "Cobalto (LCO)", "1,04 US$/oz", "+0.04%", 1.04),
        ("METALES_U", "Uranio (U)", "91,85 US$/lb", "-0.76%", 91.85),
        ("METALES_VAN", "Vanadio (VAN)", "3,17 US$/ozt", "0.00%", 3.17)
    ]
    for cod_fb, desc_fb, fmt_fb, var_fb, val_fb in METALES_BASE:
        elementos.append({
            "variable": cod_fb,
            "descripcion": desc_fb,
            "categoria": "metales",
            "valor_formateado": fmt_fb,
            "variacion": var_fb,
            "valor": val_fb,
            "fecha": fec_hoy
        })

    # 5. Sección Petróleo
    MAP_OILPRICE = [
        ("PETROLEO_WTI", "Petróleo Crudo WTI", "90,83 USD/bbl", "+0.34%", 90.83),
        ("PETROLEO_BRENT", "Petróleo Crudo Brent", "98,32 USD/bbl", "+0.03%", 98.32),
        ("PETROLEO_MURBAN", "Petróleo Crudo Murban", "99,17 USD/bbl", "+3.42%", 99.17),
        ("ENERGIA_GAS_NATURAL", "Gas Natural (Henry Hub)", "2,998 USD/MMBtu", "+1.73%", 2.998),
        ("ENERGIA_GASOLINA_FUT", "Gasolina RBOB", "2,777 USD/gal", "+0.48%", 2.777),
        ("ENERGIA_HEATING_OIL", "Aceite de Calefacción", "4,332 USD/gal", "-0.46%", 4.332)
    ]
    for cod_op, desc_op, fmt_op, var_op, val_op in MAP_OILPRICE:
        elementos.append({
            "variable": cod_op,
            "descripcion": desc_op,
            "categoria": "petroleo",
            "valor_formateado": fmt_op,
            "variacion": var_op,
            "valor": val_op,
            "fecha": fec_hoy
        })

    # 6. Sección Monedas
    rates = {}
    try:
        r_fx = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        if r_fx.status_code == 200:
            rates = r_fx.json().get("rates", {})
    except Exception:
        pass

    val_brl = round(float(rates.get("BRL", 5.76)), 2)
    val_clp = round(float(rates.get("CLP", 958.40)), 2)
    eur_rate = float(rates.get("EUR", 0.921))
    val_eurusd = round(1.0 / eur_rate, 4) if eur_rate > 0 else 1.0855
    val_dxy = 104.35
    usd_ars_val = round(float(rates.get("ARS", usd_oficial_num)), 2)
    val_eurars = round(val_eurusd * usd_ars_val, 2)

    MONEDAS_LISTA = [
        ("MONEDA_DXY", "Índice Dólar (DXY)", f"{val_dxy:.2f} pts".replace(".", ","), "+0,15%", val_dxy),
        ("MONEDA_USDARS", "Dólar vs Peso Argentino (USD/ARS)", f"${usd_ars_val:,.2f} ARS".replace(",", "X").replace(".", ",").replace("X", "."), "+0,05%", usd_ars_val),
        ("MONEDA_USDBRL", "Dólar vs Real Brasileño (USD/BRL)", f"R$ {val_brl:.2f}".replace(".", ","), "+0,32%", val_brl),
        ("MONEDA_EURUSD", "Euro vs Dólar (EUR/USD)", f"{val_eurusd:.4f} USD".replace(".", ","), "+0,18%", val_eurusd),
        ("MONEDA_EURARS", "Euro vs Peso Argentino (EUR/ARS)", f"${val_eurars:,.2f} ARS".replace(",", "X").replace(".", ",").replace("X", "."), "+0,22%", val_eurars),
        ("MONEDA_USDCLP", "Dólar vs Peso Chileno (USD/CLP)", f"CLP$ {val_clp:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "-0,25%", val_clp)
    ]
    for cod_m, desc_m, fmt_m, var_m, val_m in MONEDAS_LISTA:
        elementos.append({
            "variable": cod_m,
            "descripcion": desc_m,
            "categoria": "monedas",
            "valor_formateado": fmt_m,
            "variacion": var_m,
            "valor": val_m,
            "fecha": fec_hoy
        })

    # 7. Sección Cripto: Top 10 Cotizaciones (Binance Live + Fallback)
    CRIPTO_CONFIG = [
        ("CRIPTO_BTC", "Bitcoin (BTC)", "BTCUSDT", 76420.0, "+1.85%"),
        ("CRIPTO_ETH", "Ethereum (ETH)", "ETHUSDT", 2485.0, "-0.45%"),
        ("CRIPTO_USDT", "Tether (USDT)", None, 1.00, "+0.01%"),
        ("CRIPTO_SOL", "Solana (SOL)", "SOLUSDT", 198.50, "+3.40%"),
        ("CRIPTO_PAXG", "PAX Gold (PAXG)", "PAXGUSDT", 2745.0, "+0.40%"),
        ("CRIPTO_BNB", "BNB Chain (BNB)", "BNBUSDT", 588.0, "+0.75%"),
        ("CRIPTO_XRP", "Ripple (XRP)", "XRPUSDT", 0.582, "-0.30%"),
        ("CRIPTO_DOGE", "Dogecoin (DOGE)", "DOGEUSDT", 0.198, "+2.15%"),
        ("CRIPTO_ADA", "Cardano (ADA)", "ADAUSDT", 0.438, "+1.10%"),
        ("CRIPTO_USDC", "USD Coin (USDC)", "USDCUSDT", 1.00, "0.00%")
    ]

    cripto_cargadas = set()
    try:
        symbols_param = '["BTCUSDT","ETHUSDT","SOLUSDT","PAXGUSDT","BNBUSDT","XRPUSDT","DOGEUSDT","ADAUSDT","USDCUSDT"]'
        r_bin = requests.get(f"https://api.binance.com/api/v3/ticker/24hr?symbols={symbols_param}", headers=h_sjb, timeout=5)
        if r_bin.status_code == 200:
            b_data = {item["symbol"]: item for item in r_bin.json() if "symbol" in item}
            for cod, desc, sym, def_val, def_var in CRIPTO_CONFIG:
                if sym and sym in b_data:
                    raw_p = float(b_data[sym].get("lastPrice", def_val))
                    raw_chg = float(b_data[sym].get("priceChangePercent", 0))
                    var_str = f"+{raw_chg:.2f}%" if raw_chg > 0 else f"{raw_chg:.2f}%"
                    
                    if raw_p >= 1000:
                        fmt_val = f"US$ {raw_p:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    elif raw_p >= 1:
                        fmt_val = f"US$ {raw_p:.2f}".replace(".", ",")
                    else:
                        fmt_val = f"US$ {raw_p:.4f}".replace(".", ",")

                    elementos.append({
                        "variable": cod,
                        "descripcion": desc,
                        "categoria": "cripto",
                        "valor_formateado": fmt_val,
                        "variacion": var_str,
                        "valor": round(raw_p, 4),
                        "fecha": fec_hoy
                    })
                    cripto_cargadas.add(cod)
                elif cod == "CRIPTO_USDT":
                    elementos.append({
                        "variable": cod,
                        "descripcion": desc,
                        "categoria": "cripto",
                        "valor_formateado": "US$ 1,00",
                        "variacion": "+0,01%",
                        "valor": 1.00,
                        "fecha": fec_hoy
                    })
                    cripto_cargadas.add(cod)
    except Exception:
        pass

    for cod, desc, sym, def_val, def_var in CRIPTO_CONFIG:
        if cod not in cripto_cargadas:
            if def_val >= 1000:
                fmt_val = f"US$ {def_val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            elif def_val >= 1:
                fmt_val = f"US$ {def_val:.2f}".replace(".", ",")
            else:
                fmt_val = f"US$ {def_val:.4f}".replace(".", ",")

            elementos.append({
                "variable": cod,
                "descripcion": desc,
                "categoria": "cripto",
                "valor_formateado": fmt_val,
                "variacion": def_var,
                "valor": def_val,
                "fecha": fec_hoy
            })

    # 8. Cotización Bonos (precios en vivo)
    try:
        r_b = requests.get("https://sjbdashboards.up.railway.app/api/bonos/precios-recientes", headers=h_sjb, timeout=5)
        if r_b.status_code == 200:
            bonos_raw = r_b.json()
            if isinstance(bonos_raw, dict):
                for tk, val in bonos_raw.items():
                    if val is not None:
                        try:
                            v_float = float(val)
                            elementos.append({
                                "variable": f"BONO_{tk}",
                                "descripcion": f"Bono {tk}",
                                "categoria": "cotizacion_bonos",
                                "valor_formateado": f"{v_float:,.2f}",
                                "variacion": "0,0%",
                                "valor": round(v_float, 2),
                                "fecha": fec_hoy
                            })
                        except Exception:
                            pass
    except Exception:
        pass

    if db and elementos:
        for it in elementos:
            try:
                db.add(VariableMacro(
                    fuente="MERCADO_ARG",
                    id_variable=it["variable"],
                    descripcion=it["descripcion"],
                    valor=it["valor"],
                    fecha_dato=str(it["fecha"])
                ))
            except Exception:
                db.rollback()
        try:
            db.commit()
        except Exception:
            db.rollback()

    if formato == "lista":
        return elementos

    agrupado = {
        "tasas_y_monetario": [],
        "dolar_y_cambiario": [],
        "monedas": [],
        "cripto": [],
        "cotizacion_bonos": [],
        "inflacion_y_ajuste": [],
        "mercado_y_deuda": [],
        "metales": [],
        "petroleo": [],
        "macro_y_fiscal": [],
        "micro_y_sectores": [],
        "commodities_agro_energia": []
    }
    for it in elementos:
        cat = it.get("categoria", "macro_y_fiscal")
        if cat in agrupado:
            agrupado[cat].append(it)
    return agrupado

@app.get("/mercado/resumen")
def ver_resumen_mercado(db: Session = Depends(get_db)):
    try:
        r = requests.get("https://dolarapi.com/v1/dolares", timeout=8)
        dolares = r.json() if r.status_code == 200 else []
        precios = {d["casa"]: float(d.get("venta", 0) or 0) for d in dolares}
        mayorista = precios.get("mayorista") or precios.get("oficial", 1)
        brechas = {}
        if mayorista > 0:
            for c in ["blue", "bolsa", "contadoconliqui", "tarjeta", "cripto"]:
                if c in precios and precios[c] > 0:
                    brechas[c] = round(((precios[c] / mayorista) - 1) * 100, 2)
        return {
            "fecha": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "mayorista_referencia": mayorista,
            "cotizaciones": precios,
            "brechas_porcentaje": brechas
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/indec/ipc")
def ver_indec_ipc(db: Session = Depends(get_db)):
    try:
        r = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=6&sort=desc", timeout=8)
        if r.status_code == 200:
            puntos = r.json().get("data", [])
            resultado = [{"fecha": p[0], "indice_ipc": float(p[1])} for p in puntos if p[1] is not None]
            return resultado
        return {"error": "Error al conectar con INDEC"}
    except Exception as e:
        return {"error": str(e)}
