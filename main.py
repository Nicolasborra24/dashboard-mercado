import os
import re
import json
import time
import urllib3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional, Dict, Any
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Depends, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL and DATABASE_URL.startswith("postgresql://") and not DATABASE_URL.startswith("postgresql+"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = None
SessionLocal = None
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

if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=engine)
        print("[DB] Conexión establecida con éxito.")
    except Exception as e:
        print(f"[DB Aviso] Modo en memoria activo ({e})")
        engine = None
        SessionLocal = None

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
    "riesgo país": ("RIESGO_PAIS", "Riesgo País EMBI+", "indices"),
    "riesgo pais": ("RIESGO_PAIS", "Riesgo País EMBI+", "indices"),
    "merval": ("MERVAL_ARS", "S&P Merval en Pesos", "indices"),
    "s&p merval": ("MERVAL_ARS", "S&P Merval en Pesos", "indices"),
    "merval en usd": ("MERVAL_USD", "S&P Merval en Dólares", "indices"),
    "s&p merval en usd": ("MERVAL_USD", "S&P Merval en Dólares", "indices"),
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

US_TICKERS_DEF = [
    ("^GSPC", "^spx", "INDICE_SP500", "S&P 500 (SPX)", "estados_unidos", "pts"),
    ("^NDX", "^ndx", "INDICE_NASDAQ", "Nasdaq 100 (NDX)", "estados_unidos", "pts"),
    ("^VIX", "^vix", "INDICE_VIX", "Índice de Volatilidad (VIX)", "estados_unidos", "pts"),
    ("^DJI", "^dji", "INDICE_DJI", "Dow Jones Industrial (DJI)", "estados_unidos", "pts"),
    ("^RUT", "^rut", "INDICE_RUSSELL", "Russell 2000 (RUT)", "estados_unidos", "pts"),
    ("DX-Y.NYB", "dxy", "MONEDA_DXY", "Índice Dólar (DXY)", "estados_unidos", "pts"),
    ("XLK", "xlk.us", "SPDR_XLK", "Tecnología (XLK)", "sectores", "usd"),
    ("XLF", "xlf.us", "SPDR_XLF", "Financiero (XLF)", "sectores", "usd"),
    ("XLE", "xle.us", "SPDR_XLE", "Energía (XLE)", "sectores", "usd"),
    ("XLV", "xlv.us", "SPDR_XLV", "Salud (XLV)", "sectores", "usd"),
    ("XLI", "xli.us", "SPDR_XLI", "Industrial (XLI)", "sectores", "usd"),
    ("XLY", "xly.us", "SPDR_XLY", "Consumo Discrecional (XLY)", "sectores", "usd"),
    ("XLP", "xlp.us", "SPDR_XLP", "Consumo Básico (XLP)", "sectores", "usd"),
    ("XLU", "xlu.us", "SPDR_XLU", "Servicios Públicos (XLU)", "sectores", "usd"),
    ("XLB", "xlb.us", "SPDR_XLB", "Materiales (XLB)", "sectores", "usd"),
    ("XLRE", "xlre.us", "SPDR_XLRE", "Bienes Raíces (XLRE)", "sectores", "usd"),
    ("XLC", "xlc.us", "SPDR_XLC", "Comunicaciones (XLC)", "sectores", "usd")
]

INDICES_BASE_MEMORIA = [
    {"variable": "MERVAL_ARS", "descripcion": "S&P Merval en Pesos", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "$ 2.085.420", "variacion": "+1,25%", "valor": 2085420.0, "fecha": "Cierre"},
    {"variable": "MERVAL_USD", "descripcion": "S&P Merval en Dólares (CCL)", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "US$ 1.812,50", "variacion": "+0,85%", "valor": 1812.50, "fecha": "Cierre"},
    {"variable": "RIESGO_PAIS", "descripcion": "Riesgo País EMBI+", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "939 pb", "variacion": "+1,51%", "valor": 939.0, "fecha": "Cierre"},
    {"variable": "INDICE_SP500", "descripcion": "S&P 500 (SPX)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "5.995,54 pts", "variacion": "+0,38%", "valor": 5995.54, "fecha": "Cierre"},
    {"variable": "INDICE_NASDAQ", "descripcion": "Nasdaq 100 (NDX)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "21.117,20 pts", "variacion": "+0,09%", "valor": 21117.20, "fecha": "Cierre"},
    {"variable": "INDICE_VIX", "descripcion": "Índice de Volatilidad (VIX)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "14,94 pts", "variacion": "-1,71%", "valor": 14.94, "fecha": "Cierre"},
    {"variable": "INDICE_DJI", "descripcion": "Dow Jones Industrial (DJI)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "43.988,99 pts", "variacion": "+0,59%", "valor": 43988.99, "fecha": "Cierre"},
    {"variable": "INDICE_RUSSELL", "descripcion": "Russell 2000 (RUT)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "2.399,61 pts", "variacion": "+0,71%", "valor": 2399.61, "fecha": "Cierre"},
    {"variable": "MONEDA_DXY", "descripcion": "Índice Dólar (DXY)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "104,35 pts", "variacion": "+0,15%", "valor": 104.35, "fecha": "Cierre"},
    {"variable": "SPDR_XLK", "descripcion": "Tecnología (XLK)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 228,45", "variacion": "+0,52%", "valor": 228.45, "fecha": "Cierre"},
    {"variable": "SPDR_XLF", "descripcion": "Financiero (XLF)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 49,32", "variacion": "+1,15%", "valor": 49.32, "fecha": "Cierre"},
    {"variable": "SPDR_XLE", "descripcion": "Energía (XLE)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 91,80", "variacion": "+0,84%", "valor": 91.80, "fecha": "Cierre"},
    {"variable": "SPDR_XLV", "descripcion": "Salud (XLV)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 146,22", "variacion": "-0,28%", "valor": 146.22, "fecha": "Cierre"},
    {"variable": "SPDR_XLI", "descripcion": "Industrial (XLI)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 139,15", "variacion": "+0,65%", "valor": 139.15, "fecha": "Cierre"},
    {"variable": "SPDR_XLY", "descripcion": "Consumo Discrecional (XLY)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 206,88", "variacion": "+1,40%", "valor": 206.88, "fecha": "Cierre"},
    {"variable": "SPDR_XLP", "descripcion": "Consumo Básico (XLP)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 78,90", "variacion": "-0,15%", "valor": 78.90, "fecha": "Cierre"},
    {"variable": "SPDR_XLU", "descripcion": "Servicios Públicos (XLU)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 81,42", "variacion": "-0,45%", "valor": 81.42, "fecha": "Cierre"},
    {"variable": "SPDR_XLB", "descripcion": "Materiales (XLB)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 93,65", "variacion": "+0,35%", "valor": 93.65, "fecha": "Cierre"},
    {"variable": "SPDR_XLRE", "descripcion": "Bienes Raíces (XLRE)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 43,10", "variacion": "-0.62%", "valor": 43.10, "fecha": "Cierre"},
    {"variable": "SPDR_XLC", "descripcion": "Comunicaciones (XLC)", "categoria": "indices", "subcategoria": "sectores", "valor_formateado": "US$ 92,74", "variacion": "+0,48%", "valor": 92.74, "fecha": "Cierre"}
]

SJB_CURVAS_URLS = {
    "bopreales": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=BOPREALES&moneda=ARS",
    "lecaps": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=LECAP%20y%20BONCAP&moneda=ARS",
    "cer": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=CER&moneda=ARS",
    "soberanos": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=ARS",
    "duales": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=DUALES&moneda=ARS&incluir_cer_tamar=true&incluir_dl_tamar=true",
    "tamar": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=LETAM&moneda=ARS",
    "dollar_linked": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=D%C3%B3lar%20Linked&moneda=ARS"
}

CACHE_CURVAS_SJB: Dict[str, Any] = {}
CACHE_INFLACION_IMPLICITA: Any = None
CACHE_BANDAS_CAMBIARIAS: Dict[str, Any] = {
    "banda_inferior": 703.82,
    "banda_superior": 1974.20,
    "dolar_mayorista": 1545.50
}

CACHE_VARIABLES: Dict[str, List[Dict[str, Any]]] = {
    "tasas_y_monetario": [],
    "dolar_y_cambiario": [],
    "monedas": [],
    "cripto": [],
    "cotizacion_bonos": [],
    "indices": [dict(x) for x in INDICES_BASE_MEMORIA],
    "inflacion_y_ajuste": [],
    "metales": [],
    "petroleo": [],
    "macro_y_fiscal": [],
    "micro_y_sectores": [],
    "commodities_agro_energia": []
}
CACHE_LOCK = threading.Lock()
ULTIMA_ACTUALIZACION = ""

def fetch_quote_externa(sym_y: str, sym_s: str) -> Optional[Dict[str, float]]:
    h = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_y}?interval=1d&range=1d"
        r = requests.get(url, headers=h, timeout=3.5)
        if r.status_code == 200:
            res = r.json().get("chart", {}).get("result", [])
            if res:
                meta = res[0].get("meta", {})
                price = meta.get("regularMarketPrice")
                prev = meta.get("chartPreviousClose") or meta.get("previousClose")
                if price is not None:
                    chg_pct = ((price - prev) / prev * 100) if prev else 0.0
                    return {"price": round(float(price), 2), "change_pct": round(float(chg_pct), 2)}
    except Exception:
        pass

    try:
        url_s = f"https://stooq.com/q/l/?s={sym_s}&f=sd2t2ohlcv&h&e=csv"
        r_s = requests.get(url_s, headers=h, timeout=3.0)
        if r_s.status_code == 200 and "Close" in r_s.text:
            lines = r_s.text.strip().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split(",")
                if len(parts) >= 6:
                    close_p = float(parts[5])
                    open_p = float(parts[2]) if parts[2] != "N/D" else close_p
                    chg_pct = ((close_p - open_p) / open_p * 100) if open_p else 0.0
                    return {"price": round(close_p, 2), "change_pct": round(chg_pct, 2)}
    except Exception:
        pass
    return None

def obtener_datos_mercado_usa() -> List[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    
    def worker_ticker(info):
        sym_y, sym_s, cod, desc, subcat, tipo = info
        q = fetch_quote_externa(sym_y, sym_s)
        if q:
            p = q["price"]
            chg = q["change_pct"]
            var_str = f"+{chg:.2f}%" if chg > 0 else f"{chg:.2f}%"
            if tipo == "usd":
                fmt = f"US$ {p:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if p >= 1000 else f"US$ {p:.2f}".replace(".", ",")
            else:
                fmt = f"{p:,.2f} pts".replace(",", "X").replace(".", ",").replace("X", ".")
            return {
                "variable": cod,
                "descripcion": desc,
                "categoria": "indices",
                "subcategoria": subcat,
                "valor_formateado": fmt,
                "variacion": var_str,
                "valor": p,
                "fecha": fec_hoy
            }
        return None

    with ThreadPoolExecutor(max_workers=6) as ex:
        res = list(ex.map(worker_ticker, US_TICKERS_DEF))
    return [r for r in res if r is not None]

def obtener_datos_indicadores() -> List[Dict[str, Any]]:
    items = []
    try:
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            cargadas = set()
            for tr in soup.find_all("tr"):
                c = tr.find_all(["td", "th"])
                if len(c) >= 4:
                    nom = c[0].get_text(strip=True).lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
                    for k_map, v_map in MAPEO.items():
                        k_clean = k_map.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
                        if k_clean == nom and v_map[0] not in cargadas:
                            cod, desc, cat = v_map
                            v_num = normalizar_valor(c[1].get_text(strip=True))
                            if v_num is not None:
                                item_dict = {
                                    "variable": cod,
                                    "descripcion": desc,
                                    "categoria": cat,
                                    "valor_formateado": c[1].get_text(strip=True),
                                    "variacion": c[2].get_text(strip=True),
                                    "valor": v_num,
                                    "fecha": c[3].get_text(strip=True)
                                }
                                if cat == "indices":
                                    item_dict["subcategoria"] = "argentina"
                                items.append(item_dict)
                                cargadas.add(v_map[0])
    except Exception:
        pass
    return items

def obtener_datos_dolar_futuro() -> Optional[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    meses_es = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    now = datetime.utcnow()
    nombre_mes = f"{meses_es[now.month - 1]} {now.year}"
    precio_fut = 1570.50
    var_str = "-0,19%"

    try:
        h = {"User-Agent": "Mozilla/5.0"}
        r = requests.get("https://www.dolarito.ar/", headers=h, timeout=4)
        if r.status_code == 200:
            m = re.search(r"dolar futuro[^\d]*([\d\.,]+)", r.text, re.IGNORECASE)
            if m:
                val = normalizar_valor(m.group(1))
                if val and val > 1000:
                    precio_fut = val
    except Exception:
        pass

    return {
        "variable": "DOLAR_FUTURO",
        "descripcion": f"DÓLAR FUTURO ({nombre_mes.upper()})",
        "categoria": "dolar_y_cambiario",
        "valor_formateado": f"${precio_fut:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "variacion": var_str,
        "valor": precio_fut,
        "fecha": f"Vto. {nombre_mes}"
    }

def obtener_datos_metales_panorama() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }

    MAPA_METALES = {
        "oro": ("METALES_XAU", "ORO (XAU)", "US$/ozt"),
        "gold": ("METALES_XAU", "ORO (XAU)", "US$/ozt"),
        "plata": ("METALES_XAG", "PLATA (XAG)", "US$/oz"),
        "silver": ("METALES_XAG", "PLATA (XAG)", "US$/oz"),
        "cobre": ("METALES_HG00", "COBRE (HG00)", "US$/lb"),
        "copper": ("METALES_HG00", "COBRE (HG00)", "US$/lb"),
        "platino": ("METALES_XPT", "PLATINO (XPT)", "US$/oz"),
        "platinum": ("METALES_XPT", "PLATINO (XPT)", "US$/oz"),
        "paladio": ("METALES_XPD", "PALADIO (XPD)", "US$/oz"),
        "palladium": ("METALES_XPD", "PALADIO (XPD)", "US$/oz"),
        "aluminio": ("METALES_ALU", "ALUMINIO (ALU)", "US$/ozt"),
        "aluminum": ("METALES_ALU", "ALUMINIO (ALU)", "US$/ozt"),
        "rodio": ("METALES_XRH", "RODIO (XRH)", "US$/oz"),
        "rhodium": ("METALES_XRH", "RODIO (XRH)", "US$/oz"),
        "zinc": ("METALES_ZNC", "ZINC (ZNC)", "US$/ozt"),
        "niquel": ("METALES_NI", "NÍQUEL (NI)", "US$/oz"),
        "nickel": ("METALES_NI", "NÍQUEL (NI)", "US$/oz"),
        "estaño": ("METALES_TIN", "ESTAÑO (TIN)", "US$/oz"),
        "tin": ("METALES_TIN", "ESTAÑO (TIN)", "US$/oz"),
        "plomo": ("METALES_LEAD", "PLOMO (LEAD)", "US$/oz"),
        "lead": ("METALES_LEAD", "PLOMO (LEAD)", "US$/oz"),
        "molibdeno": ("METALES_MO", "MOLIBDENO (MO)", "US$/ozt"),
        "molybdenum": ("METALES_MO", "MOLIBDENO (MO)", "US$/ozt"),
        "hierro": ("METALES_IRON", "MINERAL DE HIERRO (IRON)", "US$/oz"),
        "iron": ("METALES_IRON", "MINERAL DE HIERRO (IRON)", "US$/oz"),
        "litio": ("METALES_LI", "LITIO (LI)", "US$/oz"),
        "lithium": ("METALES_LI", "LITIO (LI)", "US$/oz"),
        "carbonato de litio": ("METALES_LITH_CAR", "CARBONATO DE LITIO (LITH-CAR)", "US$/ozt"),
        "cobalto": ("METALES_LCO", "COBALTO (LCO)", "US$/oz"),
        "cobalt": ("METALES_LCO", "COBALTO (LCO)", "US$/oz"),
        "uranio": ("METALES_U", "URANIO (U)", "US$/lb"),
        "uranium": ("METALES_U", "URANIO (U)", "US$/lb"),
        "vanadio": ("METALES_VAN", "VANADIO (VAN)", "US$/ozt"),
        "vanadium": ("METALES_VAN", "VANADIO (VAN)", "US$/ozt")
    }

    try:
        url = "https://www.panorama-minero.com/es/markets/metals"
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            cargados = set()

            for tr in soup.find_all("tr"):
                tds = tr.find_all(["td", "th"])
                if len(tds) >= 2:
                    nombre_raw = tds[0].get_text(strip=True).lower()
                    texto_limpio = nombre_raw.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")

                    for clave, (cod, desc, unidad) in MAPA_METALES.items():
                        if clave in texto_limpio and cod not in cargados:
                            val_str = tds[1].get_text(strip=True)
                            val_num = normalizar_valor(val_str)
                            if val_num is not None:
                                var_str = "0.00%"
                                if len(tds) >= 3:
                                    raw_var = tds[2].get_text(strip=True)
                                    if "%" in raw_var or "+" in raw_var or "-" in raw_var:
                                        var_str = raw_var

                                fmt = f"{val_num:,.2f} {unidad}".replace(",", "X").replace(".", ",").replace("X", ".") if val_num >= 1000 else f"{val_num:.2f} {unidad}".replace(".", ",")
                                items.append({
                                    "variable": cod,
                                    "descripcion": desc,
                                    "categoria": "metales",
                                    "valor_formateado": fmt,
                                    "variacion": var_str,
                                    "valor": val_num,
                                    "fecha": fec_hoy
                                })
                                cargados.add(cod)
    except Exception as e:
        print(f"[Panorama Minero Scraping Error] {e}")

    return items

def obtener_datos_tasas_sjb() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = requests.get("https://sjbdashboards.up.railway.app/api/tasas/tablas", headers={"User-Agent": "Mozilla/5.0"}, timeout=4)
        if r.status_code == 200:
            d = r.json()
            for row in d.get("principales", []):
                if row.get("Tasa", "").upper() == "TM20":
                    tna_val = row.get("TNA", "")
                    items.append({
                        "variable": "TASA_TM20",
                        "descripcion": f"Tasa TM20 (TEA {row.get('TEA', '')})",
                        "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna_val} TNA",
                        "variacion": "0,0%",
                        "valor": normalizar_valor(tna_val) or 22.0,
                        "fecha": fec_hoy
                    })
            for row in d.get("otras", []):
                nom_t = row.get("Tasa", "").lower()
                tna_val = row.get("TNA", "")
                if "call" in nom_t:
                    items.append({
                        "variable": "TASA_CALL",
                        "descripcion": "Tasa Call Interbancaria en Pesos",
                        "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna_val} TNA",
                        "variacion": "0,0%",
                        "valor": normalizar_valor(tna_val) or 21.66,
                        "fecha": fec_hoy
                    })
                elif "pase" in nom_t:
                    items.append({
                        "variable": "TASA_PASES",
                        "descripcion": "Tasa de Pases Pasivos BCRA",
                        "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna_val} TNA",
                        "variacion": "0,0%",
                        "valor": normalizar_valor(tna_val) or 22.06,
                        "fecha": fec_hoy
                    })
    except Exception:
        pass
    return items

def obtener_datos_monedas_y_dolar() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    usd_oficial = 1015.50
    try:
        r_a = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=4)
        if r_a.status_code == 200:
            d = r_a.json().get("data", [])
            if d and len(d[0]) >= 2 and d[0][1]:
                usd_oficial = float(d[0][1])
                items.append({
                    "variable": "DOLAR_A3500",
                    "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
                    "categoria": "dolar_y_cambiario",
                    "valor_formateado": f"${usd_oficial:.2f}",
                    "variacion": "0,0%",
                    "valor": usd_oficial,
                    "fecha": d[0][0]
                })
    except Exception:
        pass

    rates = {}
    try:
        r_fx = requests.get("https://open.er-api.com/v6/latest/USD", timeout=4)
        if r_fx.status_code == 200:
            rates = r_fx.json().get("rates", {})
    except Exception:
        pass

    val_brl = round(float(rates.get("BRL", 5.76)), 2)
    val_clp = round(float(rates.get("CLP", 958.40)), 2)
    eur_rate = float(rates.get("EUR", 0.921))
    val_eurusd = round(1.0 / eur_rate, 4) if eur_rate > 0 else 1.0855
    usd_ars_val = round(float(rates.get("ARS", usd_oficial)), 2)
    val_eurars = round(val_eurusd * usd_ars_val, 2)

    MONEDAS = [
        ("MONEDA_USDARS", "Dólar vs Peso Argentino (USD/ARS)", f"${usd_ars_val:,.2f} ARS".replace(",", "X").replace(".", ",").replace("X", "."), "+0,05%", usd_ars_val),
        ("MONEDA_USDBRL", "Dólar vs Real Brasileño (USD/BRL)", f"R$ {val_brl:.2f}".replace(".", ","), "+0,32%", val_brl),
        ("MONEDA_EURUSD", "Euro vs Dólar (EUR/USD)", f"{val_eurusd:.4f} USD".replace(".", ","), "+0,18%", val_eurusd),
        ("MONEDA_EURARS", "Euro vs Peso Argentino (EUR/ARS)", f"${val_eurars:,.2f} ARS".replace(",", "X").replace(".", ",").replace("X", "."), "+0,22%", val_eurars),
        ("MONEDA_USDCLP", "Dólar vs Peso Chileno (USD/CLP)", f"CLP$ {val_clp:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "-0,25%", val_clp)
    ]
    for cod, desc, fmt, var_str, val_n in MONEDAS:
        items.append({"variable": cod, "descripcion": desc, "categoria": "monedas", "valor_formateado": fmt, "variacion": var_str, "valor": val_n, "fecha": fec_hoy})
    return items

def obtener_datos_bonos_sjb() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = requests.get("https://sjbdashboards.up.railway.app/api/bonos/precios-recientes", headers={"User-Agent": "Mozilla/5.0"}, timeout=4)
        if r.status_code == 200:
            bonos_raw = r.json()
            if isinstance(bonos_raw, dict):
                for tk, val in bonos_raw.items():
                    if val is not None:
                        try:
                            v_float = float(val)
                            items.append({
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
    return items

def fetch_una_curva_sjb(clave: str, url: str) -> tuple:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        if r.status_code == 200:
            return clave, r.json()
    except Exception as e:
        print(f"[Curva {clave} Error] {e}")
    return clave, None

def fetch_inflacion_implicita_sjb():
    try:
        r = requests.get("https://sjbdashboards.up.railway.app/api/bonos/inflacion-implicita", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def fetch_bandas_dolarito() -> Dict[str, Any]:
    b_inf = 703.82
    b_sup = 1974.20
    d_may = 1545.50
    try:
        h = {"User-Agent": "Mozilla/5.0"}
        r1 = requests.get("https://api.dolarito.ar/api/frontend/indices/bandaInferior", headers=h, timeout=4)
        if r1.status_code == 200:
            d1 = r1.json()
            v1 = float(d1.get("value") or d1.get("valor") or b_inf)
            if v1 > 0: b_inf = v1

        r2 = requests.get("https://api.dolarito.ar/api/frontend/indices/bandaSuperior", headers=h, timeout=4)
        if r2.status_code == 200:
            d2 = r2.json()
            v2 = float(d2.get("value") or d2.get("valor") or b_sup)
            if v2 > 0: b_sup = v2

        r_m = requests.get("https://dolarapi.com/v1/dolares/mayorista", headers=h, timeout=4)
        if r_m.status_code == 200:
            dm = r_m.json()
            vm = float(dm.get("venta") or dm.get("compra") or d_may)
            if vm > 0: d_may = vm
    except Exception as e:
        print(f"[Bandas Dolarito Error] {e}")

    return {
        "banda_inferior": round(b_inf, 2),
        "banda_superior": round(b_sup, 2),
        "dolar_mayorista": round(d_may, 2)
    }

def worker_refresco_continuo():
    global CACHE_VARIABLES, CACHE_CURVAS_SJB, CACHE_INFLACION_IMPLICITA, CACHE_BANDAS_CAMBIARIAS, ULTIMA_ACTUALIZACION
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")

    METALES_FALLBACK = [
        ("METALES_XAU", "ORO (XAU)", "4070,10 US$/ozt", "+5.05%", 4070.10),
        ("METALES_XAG", "PLATA (XAG)", "51,95 US$/oz", "+0.26%", 51.95),
        ("METALES_HG00", "COBRE (HG00)", "6,24 US$/lb", "-3.77%", 6.24),
        ("METALES_XPT", "PLATINO (XPT)", "1534,00 US$/oz", "+7.35%", 1534.00),
        ("METALES_XPD", "PALADIO (XPD)", "1004,00 US$/oz", "+4.47%", 1004.00),
        ("METALES_ALU", "ALUMINIO (ALU)", "10,49 US$/ozt", "-0.19%", 10.49),
        ("METALES_XRH", "RODIO (XRH)", "9200,00 US$/oz", "+1.10%", 9200.00),
        ("METALES_ZNC", "ZINC (ZNC)", "8,55 US$/ozt", "+5.23%", 8.55),
        ("METALES_NI", "NÍQUEL (NI)", "2,00 US$/oz", "-5.31%", 2.00),
        ("METALES_TIN", "ESTAÑO (TIN)", "1,45 US$/oz", "-5.06%", 1.45),
        ("METALES_LEAD", "PLOMO (LEAD)", "17,31 US$/oz", "+1.78%", 17.31),
        ("METALES_MO", "MOLIBDENO (MO)", "2,81 US$/ozt", "+0.72%", 2.81),
        ("METALES_IRON", "MINERAL DE HIERRO (IRON)", "380,52 US$/oz", "-2.15%", 380.52),
        ("METALES_LI", "LITIO (LI)", "2,29 US$/oz", "+0.35%", 2.29),
        ("METALES_LITH_CAR", "CARBONATO DE LITIO (LITH-CAR)", "2,29 US$/ozt", "+0.23%", 2.29),
        ("METALES_LCO", "COBALTO (LCO)", "1,04 US$/oz", "+0.04%", 1.04),
        ("METALES_U", "URANIO (U)", "91,85 US$/lb", "-0.76%", 91.85),
        ("METALES_VAN", "VANADIO (VAN)", "3,17 US$/ozt", "0.00%", 3.17)
    ]
    PETROLEO_BASE = [
        ("PETROLEO_WTI", "Petróleo Crudo WTI", "90,83 USD/bbl", "+0.34%", 90.83),
        ("PETROLEO_BRENT", "Petróleo Crudo Brent", "98,32 USD/bbl", "+0.03%", 98.32),
        ("PETROLEO_MURBAN", "Petróleo Crudo Murban", "99,17 USD/bbl", "+3.42%", 99.17),
        ("ENERGIA_GAS_NATURAL", "Gas Natural (Henry Hub)", "2,998 USD/MMBtu", "+1.73%", 2.998),
        ("ENERGIA_GASOLINA_FUT", "Gasolina RBOB", "2,777 USD/gal", "+0.48%", 2.777),
        ("ENERGIA_HEATING_OIL", "Aceite de Calefacción", "4,332 USD/gal", "-0.46%", 4.332)
    ]

    while True:
        try:
            with ThreadPoolExecutor(max_workers=11) as executor:
                f_ind = executor.submit(obtener_datos_indicadores)
                f_tasas = executor.submit(obtener_datos_tasas_sjb)
                f_monedas = executor.submit(obtener_datos_monedas_y_dolar)
                f_bonos = executor.submit(obtener_datos_bonos_sjb)
                f_usa = executor.submit(obtener_datos_mercado_usa)
                f_inf_imp = executor.submit(fetch_inflacion_implicita_sjb)
                f_bandas = executor.submit(fetch_bandas_dolarito)
                f_futuro = executor.submit(obtener_datos_dolar_futuro)
                f_metales = executor.submit(obtener_datos_metales_panorama)

                futures_curvas = [executor.submit(fetch_una_curva_sjb, k, url) for k, url in SJB_CURVAS_URLS.items()]

                res_ind = f_ind.result()
                res_tasas = f_tasas.result()
                res_monedas = f_monedas.result()
                res_bonos = f_bonos.result()
                res_usa = f_usa.result()
                res_inf_imp = f_inf_imp.result()
                res_bandas = f_bandas.result()
                res_futuro = f_futuro.result()
                res_metales = f_metales.result()

                for fut in futures_curvas:
                    k, data = fut.result()
                    if data:
                        CACHE_CURVAS_SJB[k] = data

            if res_inf_imp:
                CACHE_INFLACION_IMPLICITA = res_inf_imp
            if res_bandas:
                CACHE_BANDAS_CAMBIARIAS = res_bandas

            indices_map = {item["variable"]: dict(item) for item in INDICES_BASE_MEMORIA}

            for item in res_usa:
                indices_map[item["variable"]] = item

            for item in res_ind:
                if item.get("categoria") == "indices":
                    indices_map[item["variable"]] = item

            # Metales: combinar datos de Panorama Minero en vivo con el catálogo completo
            metales_map = {}
            for c, d, f, v, n in METALES_FALLBACK:
                metales_map[c] = {
                    "variable": c,
                    "descripcion": d,
                    "categoria": "metales",
                    "valor_formateado": f,
                    "variacion": v,
                    "valor": n,
                    "fecha": fec_hoy
                }
            if res_metales and len(res_metales) > 0:
                for m in res_metales:
                    metales_map[m["variable"]] = m

            nuevo_agrupado = {
                "tasas_y_monetario": [],
                "dolar_y_cambiario": [],
                "monedas": res_monedas,
                "cripto": [],
                "cotizacion_bonos": res_bonos,
                "indices": list(indices_map.values()),
                "inflacion_y_ajuste": [],
                "metales": list(metales_map.values()),
                "petroleo": [{"variable": c, "descripcion": d, "categoria": "petroleo", "valor_formateado": f, "variacion": v, "valor": n, "fecha": fec_hoy} for c, d, f, v, n in PETROLEO_BASE],
                "macro_y_fiscal": [],
                "micro_y_sectores": [],
                "commodities_agro_energia": []
            }

            for item in res_ind:
                cat = item.get("categoria", "macro_y_fiscal")
                if cat != "indices" and cat in nuevo_agrupado:
                    nuevo_agrupado[cat].append(item)

            for item in res_tasas:
                nuevo_agrupado["tasas_y_monetario"].append(item)

            if res_futuro:
                nuevo_agrupado["dolar_y_cambiario"].append(res_futuro)

            with CACHE_LOCK:
                for k, v in nuevo_agrupado.items():
                    if len(v) > 0:
                        CACHE_VARIABLES[k] = v
                ULTIMA_ACTUALIZACION = datetime.utcnow().strftime("%H:%M:%S UTC")

        except Exception as e:
            print(f"[Worker Error] {e}")

        time.sleep(20)

hilo_worker = threading.Thread(target=worker_refresco_continuo, daemon=True)
hilo_worker.start()

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
    return {"mensaje": "Servidor activo", "ultima_actualizacion": ULTIMA_ACTUALIZACION}

@app.get("/bcra/variables")
def ver_bcra(formato: str = Query("agrupado")):
    with CACHE_LOCK:
        if formato == "lista":
            lista_plana = []
            for v in CACHE_VARIABLES.values():
                lista_plana.extend(v)
            return lista_plana
        return CACHE_VARIABLES

@app.get("/api/bonos/curvas-sjb")
def ver_curvas_sjb():
    with CACHE_LOCK:
        return CACHE_CURVAS_SJB

@app.get("/api/bonos/inflacion-implicita")
def ver_inflacion_implicita():
    with CACHE_LOCK:
        return CACHE_INFLACION_IMPLICITA or {}

@app.get("/api/dolar/bandas")
def ver_bandas():
    with CACHE_LOCK:
        return CACHE_BANDAS_CAMBIARIAS

@app.get("/dolar")
def ver_dolar():
    try:
        r = requests.get("https://dolarapi.com/v1/dolares", timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"error": "Servicio de divisas ocupado"}

@app.get("/mercado/resumen")
def ver_resumen_mercado():
    try:
        r = requests.get("https://dolarapi.com/v1/dolares", timeout=5)
        dolares = r.json() if r.status_code == 200 else []
        precios = {d["casa"]: float(d.get("venta", 0) or 0) for d in dolares}
        mayorista = precios.get("mayorista") or precios.get("oficial", 1)
        brechas = {}
        if mayorista > 0:
            for c in ["blue", "bolsa", "contadoconliqui", "tarjeta", "cripto"]:
                if c in precios and precios[c] > 0:
                    brechas[c] = round(((precios[c] / mayorista) - 1) * 100, 2)
        return {
            "fecha": ULTIMA_ACTUALIZACION or datetime.utcnow().strftime("%H:%M:%S UTC"),
            "mayorista_referencia": mayorista,
            "cotizaciones": precios,
            "brechas_porcentaje": brechas
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/indec/ipc")
def ver_indec_ipc():
    try:
        r = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=6&sort=desc", timeout=5)
        if r.status_code == 200:
            puntos = r.json().get("data", [])
            return [{"fecha": p[0], "indice_ipc": float(p[1])} for p in puntos if p[1] is not None]
    except Exception:
        pass
    return []

if __name__ == "__main__":
    import uvicorn
    puerto = int(os.environ.get("PORT", 8080))
    print(f"Iniciando Uvicorn en el puerto: {puerto}")
    uvicorn.run(app, host="0.0.0.0", port=puerto)
