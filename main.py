import os
import re
import time
import urllib3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURACIÓN DE BASE DE DATOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and not DATABASE_URL.startswith("postgresql+"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

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

engine = None
SessionLocal = None
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"[DB Aviso] Modo en memoria activo ({e})")

# --- SESIÓN HTTP CON POOL DE CONEXIONES ---
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*"
})
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=1)
HTTP_SESSION.mount("https://", adapter)
HTTP_SESSION.mount("http://", adapter)

app = FastAPI(title="API Mercado Argentino")
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# --- UTILIDADES DE PARSEO ---
MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun", 7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
MESES_LARGOS = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

def normalizar_valor(texto: Any) -> Optional[float]:
    if texto is None:
        return None
    t = str(texto).strip().upper().replace("−", "-")
    mult = 1000.0 if ("B" in t or "K" in t) else 1.0
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
        return round(float(t_clean) * mult, 2)
    except Exception:
        return None

def fmt_latino(num: float, dec: int = 2) -> str:
    s = f"{num:,.{dec}f}" if num >= 1000 else f"{num:.{dec}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def extraer_mes_corte(texto_fecha: str) -> str:
    if not texto_fecha:
        return ""
    s = str(texto_fecha).strip()
    m_iso = re.search(r"(20\d{2})[-/](\d{1,2})", s)
    if m_iso and 1 <= int(m_iso.group(2)) <= 12:
        return f"{MESES_ES[int(m_iso.group(2))]} {m_iso.group(1)}"
    m_lat = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](20\d{2})", s)
    if m_lat and 1 <= int(m_lat.group(2)) <= 12:
        return f"{MESES_ES[int(m_lat.group(2))]} {m_lat.group(3)}"
    m_my = re.search(r"^(\d{1,2})[-/](20\d{2})$", s)
    if m_my and 1 <= int(m_my.group(1)) <= 12:
        return f"{MESES_ES[int(m_my.group(1))]} {m_my.group(2)}"
    nombres = {
        "enero": "Ene", "ene": "Ene", "jan": "Ene", "febrero": "Feb", "feb": "Feb",
        "marzo": "Mar", "mar": "Mar", "abril": "Abr", "abr": "Abr", "apr": "Abr",
        "mayo": "May", "may": "May", "junio": "Jun", "jun": "Jun", "julio": "Jul", "jul": "Jul",
        "agosto": "Ago", "ago": "Ago", "aug": "Ago", "septiembre": "Sep", "setiembre": "Sep", "sep": "Sep",
        "octubre": "Oct", "oct": "Oct", "noviembre": "Nov", "nov": "Nov", "diciembre": "Dic", "dic": "Dic", "dec": "Dic"
    }
    s_low = s.lower()
    anio_m = re.search(r"(20\d{2})", s_low)
    anio = anio_m.group(1) if anio_m else str(datetime.utcnow().year)
    for k, v in nombres.items():
        if re.search(rf"\b{k}\b", s_low):
            return f"{v} {anio}"
    return s

# --- CATÁLOGOS Y DEFINICIONES ---
MAPEO = {
    "reservas bcra": ("RESERVAS_USD", "Reservas Internacionales BCRA", "tasas_y_monetario"),
    "base monetaria": ("BASE_MONETARIA", "Base Monetaria", "tasas_y_monetario"),
    "tasa badlar": ("TASA_BADLAR", "Tasa BADLAR", "tasas_y_monetario"),
    "tasa plazo fijo": ("TASA_PLAZO_FIJO", "Tasa Plazo Fijo", "tasas_y_monetario"),
    "tasa referencia (tamar)": ("TASA_TAMAR", "Tasa Referencia TAMAR", "tasas_y_monetario"),
    "tasa hipotecaria": ("TASA_HIPOTECARIA", "Tasa Hipotecaria", "tasas_y_monetario"),
    "m2 var. interanual": ("M2_VAR_INTERANUAL", "M2 Variacion Interanual", "tasas_y_monetario"),
    "balance cambiario": ("BALANCE_CAMBIARIO", "Balance Cambiario", "tasas_y_monetario"),
    "tasa call": ("TASA_CALL", "Tasa Call Interbancaria en Pesos", "tasas_y_monetario"),
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
    "inflación esperada": ("INFLACION_REM", "Inflación Esperada REM (12m)", "inflacion_y_ajuste"),
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

# Formato: (YahooSym, StooqSym, Codigo, Descripcion, Categoria, Subcategoria, Unidad, ValorFallback)
TICKERS_EXTERNOS = [
    ("^GSPC", "^spx", "INDICE_SP500", "S&P 500 (SPX)", "indices", "estados_unidos", "pts", 5995.54),
    ("^NDX", "^ndx", "INDICE_NASDAQ", "Nasdaq 100 (NDX)", "indices", "estados_unidos", "pts", 21117.20),
    ("^VIX", "^vix", "INDICE_VIX", "Índice de Volatilidad (VIX)", "indices", "estados_unidos", "pts", 14.94),
    ("^DJI", "^dji", "INDICE_DJI", "Dow Jones Industrial (DJI)", "indices", "estados_unidos", "pts", 43988.99),
    ("^RUT", "^rut", "INDICE_RUSSELL", "Russell 2000 (RUT)", "indices", "estados_unidos", "pts", 2399.61),
    ("DX-Y.NYB", "dxy", "MONEDA_DXY", "Índice Dólar (DXY)", "indices", "estados_unidos", "pts", 104.35),
    ("XLK", "xlk.us", "SPDR_XLK", "Tecnología (XLK)", "indices", "sectores", "US$", 228.45),
    ("XLF", "xlf.us", "SPDR_XLF", "Financiero (XLF)", "indices", "sectores", "US$", 49.32),
    ("XLE", "xle.us", "SPDR_XLE", "Energía (XLE)", "indices", "sectores", "US$", 91.80),
    ("XLV", "xlv.us", "SPDR_XLV", "Salud (XLV)", "indices", "sectores", "US$", 146.22),
    ("XLI", "xli.us", "SPDR_XLI", "Industrial (XLI)", "indices", "sectores", "US$", 139.15),
    ("XLY", "xly.us", "SPDR_XLY", "Consumo Discrecional (XLY)", "indices", "sectores", "US$", 206.88),
    ("XLP", "xlp.us", "SPDR_XLP", "Consumo Básico (XLP)", "indices", "sectores", "US$", 78.90),
    ("XLU", "xlu.us", "SPDR_XLU", "Servicios Públicos (XLU)", "indices", "sectores", "US$", 81.42),
    ("XLB", "xlb.us", "SPDR_XLB", "Materiales (XLB)", "indices", "sectores", "US$", 93.65),
    ("XLRE", "xlre.us", "SPDR_XLRE", "Bienes Raíces (XLRE)", "indices", "sectores", "US$", 43.10),
    ("XLC", "xlc.us", "SPDR_XLC", "Comunicaciones (XLC)", "indices", "sectores", "US$", 92.74),
    ("GC=F", "gc.f", "METALES_XAU", "ORO (XAU)", "metales", None, "US$/ozt", 4070.10),
    ("SI=F", "si.f", "METALES_XAG", "PLATA (XAG)", "metales", None, "US$/oz", 51.95),
    ("HG=F", "hg.f", "METALES_HG00", "COBRE (HG00)", "metales", None, "US$/lb", 6.24),
    ("PL=F", "pl.f", "METALES_XPT", "PLATINO (XPT)", "metales", None, "US$/oz", 1534.00),
    ("PA=F", "pa.f", "METALES_XPD", "PALADIO (XPD)", "metales", None, "US$/oz", 1004.00),
    ("ALI=F", "al.f", "METALES_ALU", "ALUMINIO (ALU)", "metales", None, "US$/ozt", 10.49),
    ("ZN=F", "zn.f", "METALES_ZNC", "ZINC (ZNC)", "metales", None, "US$/ozt", 8.55),
    ("NI=F", "ni.f", "METALES_NI", "NÍQUEL (NI)", "metales", None, "US$/oz", 2.00),
    ("TIN=F", "sn.f", "METALES_TIN", "ESTAÑO (TIN)", "metales", None, "US$/oz", 1.45),
    ("LED=F", "pb.f", "METALES_LEAD", "PLOMO (LEAD)", "metales", None, "US$/oz", 17.31),
    ("TIO=F", "tio.f", "METALES_IRON", "MINERAL DE HIERRO (IRON)", "metales", None, "US$/oz", 380.52),
    ("URA", "ura.us", "METALES_U", "URANIO (U)", "metales", None, "US$/lb", 91.85),
    ("LIT", "lit.us", "METALES_LI", "LITIO (LI)", "metales", None, "US$/oz", 2.29),
    ("LIT", "lit.us", "METALES_LITH_CAR", "CARBONATO DE LITIO (LITH-CAR)", "metales", None, "US$/ozt", 2.29),
    ("MO=F", "mo.f", "METALES_MO", "MOLIBDENO (MO)", "metales", None, "US$/ozt", 2.81),
    ("XRH=X", "xrh.x", "METALES_XRH", "RODIO (XRH)", "metales", None, "US$/oz", 9200.00),
    ("VAN=F", "van.f", "METALES_VAN", "VANADIO (VAN)", "metales", None, "US$/ozt", 3.17),
    ("COBALT", "cobalt.us", "METALES_LCO", "COBALTO (LCO)", "metales", None, "US$/oz", 1.04),
    ("CL=F", "cl.f", "PETROLEO_WTI", "Petróleo Crudo WTI", "petroleo", None, "USD/bbl", 71.24),
    ("BZ=F", "cb.f", "PETROLEO_BRENT", "Petróleo Crudo Brent", "petroleo", None, "USD/bbl", 74.88),
    ("BZ=F", "cb.f", "PETROLEO_MURBAN", "Petróleo Crudo Murban", "petroleo", None, "USD/bbl", 75.12),
    ("NG=F", "ng.f", "ENERGIA_GAS_NATURAL", "Gas Natural (Henry Hub)", "petroleo", None, "USD/MMBtu", 2.85),
    ("RB=F", "rb.f", "ENERGIA_GASOLINA_FUT", "Gasolina RBOB", "petroleo", None, "USD/gal", 2.05),
    ("HO=F", "ho.f", "ENERGIA_HEATING_OIL", "Aceite de Calefacción", "petroleo", None, "USD/gal", 2.26)
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

# --- CACHÉS GLOBALES PROTEGIDOS ---
CACHE_LOCK = threading.Lock()
ULTIMA_ACTUALIZACION = ""
CACHE_CURVAS_SJB: Dict[str, Any] = {}
CACHE_INFLACION_IMPLICITA: Any = None
CACHE_BANDAS_CAMBIARIAS: Dict[str, Any] = {"banda_inferior": 703.82, "banda_superior": 1974.20, "dolar_mayorista": 1545.50}
CACHE_LICITACIONES_MAE: List[Dict[str, Any]] = []

CACHE_TASAS_PERSISTENTE: Dict[str, Dict[str, Any]] = {
    "TASA_CALL": {"variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "21,66% TNA", "variacion": "0,0%", "valor": 21.66, "fecha": "Mercado"},
    "TASA_TM20": {"variable": "TASA_TM20", "descripcion": "Tasa TM20 en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "22,00% TNA", "variacion": "0,0%", "valor": 22.0, "fecha": "Mercado"},
    "TASA_PASES": {"variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA", "categoria": "tasas_y_monetario", "valor_formateado": "22,06% TNA", "variacion": "0,0%", "valor": 22.06, "fecha": "Mercado"}
}

CACHE_FINVIZ_SECTORES: Dict[str, Any] = {
    "1W": [
        {"name": "Communication Services", "ticker": "XLC", "perf": 3.55},
        {"name": "Basic Materials", "ticker": "XLB", "perf": 2.09},
        {"name": "Healthcare", "ticker": "XLV", "perf": 1.53},
        {"name": "Real Estate", "ticker": "XLRE", "perf": 1.29},
        {"name": "Consumer Cyclical", "ticker": "XLY", "perf": 0.89},
        {"name": "Financial", "ticker": "XLF", "perf": 0.70},
        {"name": "Consumer Defensive", "ticker": "XLP", "perf": -0.07},
        {"name": "Utilities", "ticker": "XLU", "perf": -0.28},
        {"name": "Industrials", "ticker": "XLI", "perf": -2.94},
        {"name": "Technology", "ticker": "XLK", "perf": -4.78},
        {"name": "Energy", "ticker": "XLE", "perf": -7.02}
    ],
    "1M": [
        {"name": "Healthcare", "ticker": "XLV", "perf": 8.75},
        {"name": "Consumer Defensive", "ticker": "XLP", "perf": 8.93},
        {"name": "Real Estate", "ticker": "XLRE", "perf": 5.84},
        {"name": "Utilities", "ticker": "XLU", "perf": 3.93},
        {"name": "Financial", "ticker": "XLF", "perf": 3.50},
        {"name": "Consumer Cyclical", "ticker": "XLY", "perf": 2.88},
        {"name": "Basic Materials", "ticker": "XLB", "perf": -1.04},
        {"name": "Energy", "ticker": "XLE", "perf": -4.84},
        {"name": "Industrials", "ticker": "XLI", "perf": -5.11},
        {"name": "Communication Services", "ticker": "XLC", "perf": -10.64},
        {"name": "Technology", "ticker": "XLK", "perf": -15.18}
    ],
    "1Y": [
        {"name": "Energy", "ticker": "XLE", "perf": 35.70},
        {"name": "Basic Materials", "ticker": "XLB", "perf": 23.64},
        {"name": "Healthcare", "ticker": "XLV", "perf": 21.78},
        {"name": "Consumer Defensive", "ticker": "XLP", "perf": 19.63},
        {"name": "Financial", "ticker": "XLF", "perf": 12.14},
        {"name": "Technology", "ticker": "XLK", "perf": 9.62},
        {"name": "Real Estate", "ticker": "XLRE", "perf": 6.90},
        {"name": "Industrials", "ticker": "XLI", "perf": 5.85},
        {"name": "Utilities", "ticker": "XLU", "perf": -2.34},
        {"name": "Consumer Cyclical", "ticker": "XLY", "perf": -3.71},
        {"name": "Communication Services", "ticker": "XLC", "perf": -5.16}
    ],
    "updated": datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
}

CACHE_VARIABLES: Dict[str, List[Dict[str, Any]]] = {
    "tasas_y_monetario": list(CACHE_TASAS_PERSISTENTE.values()),
    "dolar_y_cambiario": [],
    "monedas": [],
    "cripto": [],
    "cotizacion_bonos": [],
    "indices": [
        {"variable": "MERVAL_ARS", "descripcion": "S&P Merval en Pesos", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "$ 2.085.420", "variacion": "+1,25%", "valor": 2085420.0, "fecha": "Cierre"},
        {"variable": "MERVAL_USD", "descripcion": "S&P Merval en Dólares (CCL)", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "US$ 1.812,50", "variacion": "+0,85%", "valor": 1812.50, "fecha": "Cierre"},
        {"variable": "RIESGO_PAIS", "descripcion": "Riesgo País EMBI+", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "939 pb", "variacion": "+1,51%", "valor": 939.0, "fecha": "Cierre"}
    ],
    "inflacion_y_ajuste": [],
    "metales": [],
    "petroleo": [],
    "macro_y_fiscal": [],
    "micro_y_sectores": [],
    "commodities_agro_energia": []
}

# --- FUNCIONES RECOLECTORAS ---
def fetch_quote_externa(sym_y: str, sym_s: str) -> Optional[Dict[str, float]]:
    try:
        r = HTTP_SESSION.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_y}?interval=1d&range=1d", timeout=2.5)
        if r.status_code == 200:
            res = r.json().get("chart", {}).get("result", [])
            if res:
                meta = res[0].get("meta", {})
                p = meta.get("regularMarketPrice")
                prev = meta.get("chartPreviousClose") or meta.get("previousClose")
                if p is not None:
                    chg = ((p - prev) / prev * 100) if prev else 0.0
                    return {"price": round(float(p), 2), "change_pct": round(float(chg), 2)}
    except Exception:
        pass
    try:
        r_s = HTTP_SESSION.get(f"https://stooq.com/q/l/?s={sym_s}&f=sd2t2ohlcv&h&e=csv", timeout=2.2)
        if r_s.status_code == 200 and "Close" in r_s.text:
            lines = r_s.text.strip().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split(",")
                if len(parts) >= 6:
                    c_p = float(parts[5])
                    o_p = float(parts[2]) if parts[2] != "N/D" else c_p
                    chg = ((c_p - o_p) / o_p * 100) if o_p else 0.0
                    return {"price": round(c_p, 2), "change_pct": round(chg, 2)}
    except Exception:
        pass
    return None

def obtener_todos_tickers_externos() -> Dict[str, List[Dict[str, Any]]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    agrupados: Dict[str, List[Dict[str, Any]]] = {"indices": [], "metales": [], "petroleo": []}

    def worker(info: Tuple):
        sym_y, sym_s, cod, desc, cat, subcat, unidad, def_p = info
        q = fetch_quote_externa(sym_y, sym_s)
        p = q["price"] if q else def_p
        if cod == "PETROLEO_MURBAN" and q:
            p = round(p + 0.35, 2)
        chg = q["change_pct"] if q else 0.0
        var_str = f"+{chg:.2f}%" if chg > 0 else f"{chg:.2f}%"
        fmt = f"US$ {fmt_latino(p)}" if unidad == "US$" else f"{fmt_latino(p)} {unidad}"
        item = {"variable": cod, "descripcion": desc, "categoria": cat, "valor_formateado": fmt, "variacion": var_str, "valor": p, "fecha": fec_hoy}
        if subcat:
            item["subcategoria"] = subcat
        return item

    with ThreadPoolExecutor(max_workers=12) as ex:
        for res in ex.map(worker, TICKERS_EXTERNOS):
            if res:
                agrupados[res["categoria"]].append(res)
    return agrupados

def obtener_datos_indicadores() -> List[Dict[str, Any]]:
    items = []
    try:
        r = HTTP_SESSION.get("https://indicadores.ar/indicadores-economicos", timeout=5.0)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            cargadas = set()
            for tr in soup.find_all("tr"):
                c = tr.find_all(["td", "th"])
                if len(c) >= 4:
                    nom = c[0].get_text(strip=True).lower().translate(str.maketrans("áéíóú", "aeiou"))
                    for k_map, v_map in MAPEO.items():
                        k_clean = k_map.translate(str.maketrans("áéíóú", "aeiou"))
                        if k_clean == nom and v_map[0] not in cargadas:
                            cod, desc, cat = v_map
                            v_num = normalizar_valor(c[1].get_text(strip=True))
                            if v_num is not None:
                                fec_txt = c[3].get_text(strip=True)
                                mes_corte = extraer_mes_corte(fec_txt)
                                desc_f = f"{desc} ({mes_corte})" if cod in ("INFLACION_MENSUAL", "INFLACION_INTERANUAL") and mes_corte else desc
                                item = {
                                    "variable": cod, "descripcion": desc_f, "categoria": cat,
                                    "valor_formateado": c[1].get_text(strip=True), "variacion": c[2].get_text(strip=True),
                                    "valor": v_num, "fecha": f"Corte: {mes_corte}" if mes_corte and cod in ("INFLACION_MENSUAL", "INFLACION_INTERANUAL") else fec_txt
                                }
                                if cat == "indices":
                                    item["subcategoria"] = "argentina"
                                items.append(item)
                                cargadas.add(cod)
    except Exception:
        pass
    return items

def obtener_inflacion_oficial_bcra() -> Dict[str, Dict[str, Any]]:
    urls = [
        "https://www.bcra.gob.ar/wp-json/bcra/v1/variables-principales?lang=es",
        "https://www.bcra.gob.ar/wp-json/bcra/v1/tabla-variables?lang=es",
        "https://api.bcra.gob.ar/estadisticas/v2.0/principalesvariables"
    ]
    encontrados: Dict[str, Dict[str, Any]] = {}

    def recorrer(nodo: Any, out: List[Dict[str, Any]]):
        if isinstance(nodo, dict):
            out.append(nodo)
            for v in nodo.values():
                recorrer(v, out)
        elif isinstance(nodo, list):
            for el in nodo:
                recorrer(el, out)

    def clasificar(desc_raw: str, val_raw: Any, fec_raw: Any, id_var: str = ""):
        desc_low = desc_raw.lower().translate(str.maketrans("áéíóú", "aeiou"))
        v_num = normalizar_valor(val_raw)
        if v_num is None or v_num <= 0 or v_num > 500:
            return
        mes_corte = extraer_mes_corte(str(fec_raw or desc_raw))
        fmt_pct = f"{fmt_latino(v_num, 1)}%"
        if id_var == "29" or ("inflacion" in desc_low and ("esperada" in desc_low or "rem" in desc_low)):
            encontrados["INFLACION_REM"] = {
                "variable": "INFLACION_REM", "descripcion": f"Inflación Esperada REM 12m ({mes_corte})" if mes_corte else "Inflación Esperada REM (12m)",
                "categoria": "inflacion_y_ajuste", "valor_formateado": fmt_pct, "variacion": "REM BCRA", "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "REM BCRA"
            }
        elif id_var == "27" or ("inflacion mensual" in desc_low and "esperada" not in desc_low and "rem" not in desc_low):
            encontrados["INFLACION_MENSUAL"] = {
                "variable": "INFLACION_MENSUAL", "descripcion": f"Inflación Mensual ({mes_corte})" if mes_corte else "Inflación Mensual",
                "categoria": "inflacion_y_ajuste", "valor_formateado": fmt_pct, "variacion": fmt_pct, "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "BCRA / INDEC"
            }
        elif id_var == "28" or ("inflacion interanual" in desc_low or ("inflacion" in desc_low and "i.a." in desc_low)):
            encontrados["INFLACION_INTERANUAL"] = {
                "variable": "INFLACION_INTERANUAL", "descripcion": f"Inflación Interanual ({mes_corte})" if mes_corte else "Inflación Interanual",
                "categoria": "inflacion_y_ajuste", "valor_formateado": fmt_pct, "variacion": fmt_pct, "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "BCRA / INDEC"
            }
        elif "call" in desc_low or id_var == "12":
            CACHE_TASAS_PERSISTENTE["TASA_CALL"] = {
                "variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario",
                "valor_formateado": f"{fmt_latino(v_num, 2)}% TNA", "variacion": "0,0%", "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "BCRA"
            }

    for url in urls:
        try:
            r = HTTP_SESSION.get(url, timeout=4.5, verify=False)
            if r.status_code != 200:
                continue
            try:
                nodos: List[Dict[str, Any]] = []
                recorrer(r.json(), nodos)
                for row in nodos:
                    id_v = str(row.get("idVariable") or row.get("id_variable") or row.get("id") or "")
                    desc_v = str(row.get("descripcion") or row.get("titulo") or row.get("nombre") or row.get("label") or row.get("variable") or "")
                    val_v = row.get("valor") if row.get("valor") is not None else (row.get("value") or row.get("ultValorInformado") or row.get("tasa"))
                    fec_v = row.get("fecha") or row.get("ultFechaInformada") or row.get("periodo") or row.get("mes") or ""
                    if "<" in desc_v:
                        desc_v = BeautifulSoup(desc_v, "html.parser").get_text(" ", strip=True)
                    if desc_v or id_v in ("27", "28", "29", "12"):
                        clasificar(desc_v, val_v, fec_v, id_v)
            except Exception:
                pass
            if len(encontrados) >= 3:
                break
        except Exception:
            pass

    if "INFLACION_REM" not in encontrados:
        encontrados["INFLACION_REM"] = {
            "variable": "INFLACION_REM", "descripcion": "Inflación Esperada REM (12m)", "categoria": "inflacion_y_ajuste",
            "valor_formateado": "21,8%", "variacion": "REM BCRA", "valor": 21.8, "fecha": "BCRA Oficial"
        }
    return encontrados

def obtener_datos_tasas_sjb() -> List[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = HTTP_SESSION.get("https://sjbdashboards.up.railway.app/api/tasas/tablas", timeout=4.5)
        if r.status_code == 200:
            d = r.json()
            for row in d.get("principales", []):
                if "TM20" in str(row.get("Tasa", "")).upper():
                    tna = row.get("TNA", "")
                    CACHE_TASAS_PERSISTENTE["TASA_TM20"] = {
                        "variable": "TASA_TM20", "descripcion": f"Tasa TM20 (TEA {row.get('TEA', '')})", "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna} TNA" if "TNA" not in str(tna).upper() else str(tna),
                        "variacion": "0,0%", "valor": normalizar_valor(tna) or 22.0, "fecha": fec_hoy
                    }
            for row in d.get("otras", []):
                nom_t = str(row.get("Tasa", "")).lower()
                tna = row.get("TNA", "")
                v_n = normalizar_valor(tna)
                if "call" in nom_t and v_n is not None:
                    CACHE_TASAS_PERSISTENTE["TASA_CALL"] = {
                        "variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna} TNA" if "TNA" not in str(tna).upper() else str(tna),
                        "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy
                    }
                elif "pase" in nom_t and v_n is not None:
                    CACHE_TASAS_PERSISTENTE["TASA_PASES"] = {
                        "variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA", "categoria": "tasas_y_monetario",
                        "valor_formateado": f"{tna} TNA" if "TNA" not in str(tna).upper() else str(tna),
                        "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy
                    }
    except Exception:
        pass
    return list(CACHE_TASAS_PERSISTENTE.values())

def obtener_datos_dolar_futuro() -> Dict[str, Any]:
    now = datetime.utcnow()
    nombre_mes = f"{MESES_LARGOS[now.month - 1]} {now.year}"
    precio_fut = 1570.50
    try:
        r = HTTP_SESSION.get("https://www.dolarito.ar/", timeout=3.0)
        if r.status_code == 200:
            m = re.search(r"dolar futuro[^\d]*([\d\.,]+)", r.text, re.IGNORECASE)
            if m:
                val = normalizar_valor(m.group(1))
                if val and val > 1000:
                    precio_fut = val
    except Exception:
        pass
    return {
        "variable": "DOLAR_FUTURO", "descripcion": f"DÓLAR FUTURO ({nombre_mes.upper()})",
        "categoria": "dolar_y_cambiario", "valor_formateado": f"${fmt_latino(precio_fut)}",
        "variacion": "-0,19%", "valor": precio_fut, "fecha": f"Vto. {nombre_mes}"
    }

def obtener_datos_monedas_y_dolar() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    usd_oficial = 1015.50
    try:
        r_a = HTTP_SESSION.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=3.0)
        if r_a.status_code == 200:
            d = r_a.json().get("data", [])
            if d and len(d[0]) >= 2 and d[0][1]:
                usd_oficial = float(d[0][1])
                items.append({
                    "variable": "DOLAR_A3500", "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
                    "categoria": "dolar_y_cambiario", "valor_formateado": f"${usd_oficial:.2f}",
                    "variacion": "0,0%", "valor": usd_oficial, "fecha": d[0][0]
                })
    except Exception:
        pass

    rates = {}
    try:
        r_fx = HTTP_SESSION.get("https://open.er-api.com/v6/latest/USD", timeout=3.0)
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

    pares = [
        ("MONEDA_USDARS", "Dólar vs Peso Argentino (USD/ARS)", f"${fmt_latino(usd_ars_val)} ARS", "+0,05%", usd_ars_val),
        ("MONEDA_USDBRL", "Dólar vs Real Brasileño (USD/BRL)", f"R$ {fmt_latino(val_brl)}", "+0,32%", val_brl),
        ("MONEDA_EURUSD", "Euro vs Dólar (EUR/USD)", f"{fmt_latino(val_eurusd, 4)} USD", "+0,18%", val_eurusd),
        ("MONEDA_EURARS", "Euro vs Peso Argentino (EUR/ARS)", f"${fmt_latino(val_eurars)} ARS", "+0,22%", val_eurars),
        ("MONEDA_USDCLP", "Dólar vs Peso Chileno (USD/CLP)", f"CLP$ {fmt_latino(val_clp)}", "-0,25%", val_clp)
    ]
    for cod, desc, fmt, var_s, val_n in pares:
        items.append({"variable": cod, "descripcion": desc, "categoria": "monedas", "valor_formateado": fmt, "variacion": var_s, "valor": val_n, "fecha": fec_hoy})
    return items

def obtener_datos_bonos_sjb() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = HTTP_SESSION.get("https://sjbdashboards.up.railway.app/api/bonos/precios-recientes", timeout=3.5)
        if r.status_code == 200:
            bonos_raw = r.json()
            if isinstance(bonos_raw, dict):
                for tk, val in bonos_raw.items():
                    if val is not None:
                        v_f = float(val)
                        items.append({
                            "variable": f"BONO_{tk}", "descripcion": f"Bono {tk}", "categoria": "cotizacion_bonos",
                            "valor_formateado": f"{v_f:,.2f}", "variacion": "0,0%", "valor": round(v_f, 2), "fecha": fec_hoy
                        })
    except Exception:
        pass
    return items

def fetch_una_curva_sjb(clave: str, url: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    try:
        r = HTTP_SESSION.get(url, timeout=4.5)
        if r.status_code == 200:
            raw = r.json()
            if isinstance(raw, list):
                return clave, {"tabla": raw}
            if isinstance(raw, dict):
                for k in ["tabla", "data", "bonos", "filas", "items", "rows", "resultados"]:
                    if isinstance(raw.get(k), list) and len(raw[k]) > 0:
                        res = dict(raw)
                        res["tabla"] = raw[k]
                        return clave, res
                return clave, raw
    except Exception:
        pass
    return clave, None

def fetch_inflacion_implicita_sjb():
    for u in ["https://sjbdashboards.up.railway.app/api/bonos/inflacion-implicita", "https://sjbdashboards.up.railway.app/api/inflacion-implicita"]:
        try:
            r = HTTP_SESSION.get(u, timeout=4.5)
            if r.status_code == 200 and r.json():
                return r.json()
        except Exception:
            pass
    return None

def fetch_bandas_dolarito() -> Dict[str, Any]:
    b_inf, b_sup, d_may = 703.82, 1974.20, 1545.50
    try:
        r1 = HTTP_SESSION.get("https://api.dolarito.ar/api/frontend/indices/bandaInferior", timeout=2.5)
        if r1.status_code == 200:
            b_inf = float(r1.json().get("value") or r1.json().get("valor") or b_inf)
        r2 = HTTP_SESSION.get("https://api.dolarito.ar/api/frontend/indices/bandaSuperior", timeout=2.5)
        if r2.status_code == 200:
            b_sup = float(r2.json().get("value") or r2.json().get("valor") or b_sup)
        r_m = HTTP_SESSION.get("https://dolarapi.com/v1/dolares/mayorista", timeout=2.5)
        if r_m.status_code == 200:
            d_may = float(r_m.json().get("venta") or r_m.json().get("compra") or d_may)
    except Exception:
        pass
    return {"banda_inferior": round(b_inf, 2), "banda_superior": round(b_sup, 2), "dolar_mayorista": round(d_may, 2)}

def fetch_finviz_sectores() -> Optional[Dict[str, Any]]:
    tickers_map = {
        "basic materials": "XLB", "communication services": "XLC", "consumer cyclical": "XLY",
        "consumer defensive": "XLP", "energy": "XLE", "financial": "XLF", "healthcare": "XLV",
        "industrials": "XLI", "real estate": "XLRE", "technology": "XLK", "utilities": "XLU"
    }
    try:
        r = HTTP_SESSION.get("https://finviz.com/groups?g=sector&v=210&o=name", timeout=4.5)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            res: Dict[str, Any] = {"1W": [], "1M": [], "1Y": []}
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 8:
                    nom = tds[1].get_text(strip=True)
                    tk = tickers_map.get(nom.lower())
                    if tk:
                        pw, pm, py = normalizar_valor(tds[3].text), normalizar_valor(tds[4].text), normalizar_valor(tds[7].text)
                        if pw is not None: res["1W"].append({"name": nom, "ticker": tk, "perf": pw})
                        if pm is not None: res["1M"].append({"name": nom, "ticker": tk, "perf": pm})
                        if py is not None: res["1Y"].append({"name": nom, "ticker": tk, "perf": py})
            if len(res["1W"]) >= 8:
                for k in ("1W", "1M", "1Y"):
                    res[k].sort(key=lambda x: x["perf"], reverse=True)
                res["updated"] = datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
                return res
    except Exception:
        pass
    return None

def fetch_licitaciones_mae() -> List[Dict[str, Any]]:
    try:
        r = HTTP_SESSION.get("https://api.marketdata.mae.com.ar/api/mercado/licitaciones", timeout=3.5)
        if r.status_code == 200:
            d = r.json()
            return d if isinstance(d, list) else (d.get("licitaciones") or d.get("data") or [d])
    except Exception:
        pass
    return []

# --- WORKER DE SINCRONIZACIÓN CONTINUA ---
def worker_refresco_continuo():
    global CACHE_VARIABLES, CACHE_CURVAS_SJB, CACHE_INFLACION_IMPLICITA, CACHE_BANDAS_CAMBIARIAS, CACHE_LICITACIONES_MAE, CACHE_FINVIZ_SECTORES, ULTIMA_ACTUALIZACION

    while True:
        try:
            with ThreadPoolExecutor(max_workers=16) as ex:
                f_ind = ex.submit(obtener_datos_indicadores)
                f_bcra_inf = ex.submit(obtener_inflacion_oficial_bcra)
                f_tasas = ex.submit(obtener_datos_tasas_sjb)
                f_monedas = ex.submit(obtener_datos_monedas_y_dolar)
                f_bonos = ex.submit(obtener_datos_bonos_sjb)
                f_ext = ex.submit(obtener_todos_tickers_externos)
                f_inf_imp = ex.submit(fetch_inflacion_implicita_sjb)
                f_bandas = ex.submit(fetch_bandas_dolarito)
                f_futuro = ex.submit(obtener_datos_dolar_futuro)
                f_licit = ex.submit(fetch_licitaciones_mae)
                f_finviz = ex.submit(fetch_finviz_sectores)
                f_curvas = [ex.submit(fetch_una_curva_sjb, k, u) for k, u in SJB_CURVAS_URLS.items()]

                res_ind = f_ind.result()
                res_bcra_inf = f_bcra_inf.result()
                res_tasas = f_tasas.result()
                res_monedas = f_monedas.result()
                res_bonos = f_bonos.result()
                res_ext = f_ext.result()
                res_inf_imp = f_inf_imp.result()
                res_bandas = f_bandas.result()
                res_futuro = f_futuro.result()
                res_licit = f_licit.result()
                res_finviz = f_finviz.result()

                for fc in f_curvas:
                    k, data = fc.result()
                    if data:
                        CACHE_CURVAS_SJB[k] = data

            if res_inf_imp: CACHE_INFLACION_IMPLICITA = res_inf_imp
            if res_bandas: CACHE_BANDAS_CAMBIARIAS = res_bandas
            if res_licit: CACHE_LICITACIONES_MAE = res_licit
            if res_finviz: CACHE_FINVIZ_SECTORES = res_finviz

            # Preservar índices previos y actualizar con nuevos
            indices_map = {item["variable"]: dict(item) for item in CACHE_VARIABLES.get("indices", [])}
            for item in res_ext.get("indices", []):
                indices_map[item["variable"]] = item
            for item in res_ind:
                if item.get("categoria") == "indices":
                    indices_map[item["variable"]] = item

            nuevo = {
                "tasas_y_monetario": [], "dolar_y_cambiario": [], "monedas": [], "cripto": [],
                "cotizacion_bonos": res_bonos, "indices": list(indices_map.values()),
                "inflacion_y_ajuste": [], "metales": res_ext.get("metales", []),
                "petroleo": res_ext.get("petroleo", []), "macro_y_fiscal": [],
                "micro_y_sectores": [], "commodities_agro_energia": []
            }

            for item in res_monedas:
                nuevo["dolar_y_cambiario" if item.get("categoria") == "dolar_y_cambiario" else "monedas"].append(item)

            for item in res_ind:
                cat = item.get("categoria", "macro_y_fiscal")
                if cat != "indices" and cat in nuevo:
                    nuevo[cat].append(item)

            if res_bcra_inf:
                for cod_inf, obj_inf in res_bcra_inf.items():
                    reemp = False
                    for idx_i, ex_item in enumerate(nuevo["inflacion_y_ajuste"]):
                        if ex_item.get("variable") == cod_inf:
                            nuevo["inflacion_y_ajuste"][idx_i] = obj_inf
                            reemp = True
                            break
                    if not reemp:
                        nuevo["inflacion_y_ajuste"].append(obj_inf)

            orden_inf = ["INFLACION_MENSUAL", "INFLACION_INTERANUAL", "INFLACION_REM", "INDICE_CER", "INDICE_UVA", "INDICE_ICL"]
            nuevo["inflacion_y_ajuste"].sort(key=lambda x: orden_inf.index(x.get("variable")) if x.get("variable") in orden_inf else 99)

            mapa_tasas = {t["variable"]: t for t in res_tasas}
            for kp, objp in CACHE_TASAS_PERSISTENTE.items():
                if kp not in mapa_tasas:
                    mapa_tasas[kp] = objp

            existentes_t = {x.get("variable") for x in nuevo["tasas_y_monetario"]}
            for kt, item_t in mapa_tasas.items():
                if kt not in existentes_t:
                    nuevo["tasas_y_monetario"].append(item_t)

            if res_futuro:
                nuevo["dolar_y_cambiario"].append(res_futuro)

            with CACHE_LOCK:
                for k, v in nuevo.items():
                    if len(v) > 0:
                        # Merge inteligente por variable para no perder ningún ítem si una fuente falla
                        prev_map = {it["variable"]: it for it in CACHE_VARIABLES.get(k, []) if "variable" in it}
                        for new_it in v:
                            prev_map[new_it["variable"]] = new_it
                        CACHE_VARIABLES[k] = list(prev_map.values()) if k not in ("inflacion_y_ajuste",) else v
                ULTIMA_ACTUALIZACION = datetime.utcnow().strftime("%H:%M:%S UTC")

        except Exception as e:
            print(f"[Worker Error] {e}")

        time.sleep(8)

threading.Thread(target=worker_refresco_continuo, daemon=True).start()

# --- ENDPOINTS FASTAPI ---
@app.get("/")
def inicio():
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html", headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})
    return {"mensaje": "Servidor activo", "ultima_actualizacion": ULTIMA_ACTUALIZACION}

@app.get("/bcra/variables")
def ver_bcra(formato: str = Query("agrupado")):
    with CACHE_LOCK:
        if formato == "lista":
            return [item for sublist in CACHE_VARIABLES.values() for item in sublist]
        return CACHE_VARIABLES

@app.get("/api/sectores/finviz")
def ver_sectores_finviz():
    with CACHE_LOCK:
        return CACHE_FINVIZ_SECTORES

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

@app.get("/api/licitaciones/mae")
def ver_licitaciones_mae():
    with CACHE_LOCK:
        return CACHE_LICITACIONES_MAE

@app.get("/dolar")
def ver_dolar():
    try:
        r = HTTP_SESSION.get("https://dolarapi.com/v1/dolares", timeout=3.5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"error": "Servicio de divisas ocupado"}

@app.get("/mercado/resumen")
def ver_resumen_mercado():
    try:
        r = HTTP_SESSION.get("https://dolarapi.com/v1/dolares", timeout=3.5)
        dolares = r.json() if r.status_code == 200 else []
        precios = {d["casa"]: float(d.get("venta", 0) or 0) for d in dolares}
        may = precios.get("mayorista") or precios.get("oficial", 1)
        brechas = {c: round(((precios[c] / may) - 1) * 100, 2) for c in ["blue", "bolsa", "contadoconliqui", "tarjeta", "cripto"] if may > 0 and precios.get(c, 0) > 0}
        return {"fecha": ULTIMA_ACTUALIZACION or datetime.utcnow().strftime("%H:%M:%S UTC"), "mayorista_referencia": may, "cotizaciones": precios, "brechas_porcentaje": brechas}
    except Exception as e:
        return {"error": str(e)}

@app.get("/indec/ipc")
def ver_indec_ipc():
    try:
        r = HTTP_SESSION.get("https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=6&sort=desc", timeout=3.5)
        if r.status_code == 200:
            return [{"fecha": p[0], "indice_ipc": float(p[1])} for p in r.json().get("data", []) if p[1] is not None]
    except Exception:
        pass
    return []

if __name__ == "__main__":
    import uvicorn
    puerto = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=puerto)
