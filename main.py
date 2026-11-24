import os
import re
import json
import time
import urllib3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Depends, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import ssl
from urllib.parse import quote
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

class MAEAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        except Exception:
            pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

# --- SESIÓN HTTP GLOBAL CON CONNECTION POOLING Y CACHÉ DE CICLO ---
GLOBAL_HTTP_SESSION = requests.Session()
_adapter_pool = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=1)
GLOBAL_HTTP_SESSION.mount("https://", _adapter_pool)
GLOBAL_HTTP_SESSION.mount("http://", _adapter_pool)

MAE_HTTP_SESSION = requests.Session()
MAE_HTTP_SESSION.mount("https://", MAEAdapter(pool_connections=16, pool_maxsize=16, max_retries=1))

_RAVA_HTML_CACHE = {"ts": 0.0, "html": ""}
_RAVA_CACHE_LOCK = threading.Lock()
_DOLARITO_CICLO_CACHE = {"ts": 0.0, "data": {}}
_DOLARITO_CACHE_LOCK = threading.Lock()

def obtener_html_rava_compartido(ttl: float = 6.0) -> str:
    """Descarga rava.com una única vez por ciclo y comparte el HTML entre Acciones, Merval, Cauciones y Dólares."""
    now = time.time()
    if now - _RAVA_HTML_CACHE["ts"] < ttl and _RAVA_HTML_CACHE["html"]:
        return _RAVA_HTML_CACHE["html"]
    with _RAVA_CACHE_LOCK:
        if time.time() - _RAVA_HTML_CACHE["ts"] < ttl and _RAVA_HTML_CACHE["html"]:
            return _RAVA_HTML_CACHE["html"]
        try:
            r = GLOBAL_HTTP_SESSION.get(
                f"https://www.rava.com/?_t={int(now * 1000)}",
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"},
                timeout=4.2
            )
            if r.status_code == 200 and len(r.text) > 500:
                _RAVA_HTML_CACHE["html"] = r.text
                _RAVA_HTML_CACHE["ts"] = time.time()
        except Exception:
            pass
        return _RAVA_HTML_CACHE["html"]


LICITACIONES_AGENDA_COMPLETA = [
    {"fecha": "2026-10-14", "horario": "10:00 - 15:00", "titulo": "LICITACIÓN SECRETARÍA DE FINANZAS — LECAP (S30O6 / S30N6) Y BONCAP - REAPERTURA"},
    {"fecha": "2026-10-15", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES YPF S.A. CLASE XXXI"},
    {"fecha": "2026-10-21", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES PAN AMERICAN ENERGY S.L. CLASE XXIV"},
    {"fecha": "2026-10-22", "horario": "10:00 - 16:00", "titulo": "LETRAS DE TESORERÍA PROVINCIA DEL NEUQUÉN SERIE IV"},
    {"fecha": "2026-10-28", "horario": "10:00 - 15:00", "titulo": "LICITACIÓN SECRETARÍA DE FINANZAS — BONOS CER (TZX27 / TZX28) Y LECAP"},
    {"fecha": "2026-10-29", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES GENNEIA S.A. CLASE XLII"},
    {"fecha": "2026-11-03", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES CRESUD S.A.C.I.F. Y A. CLASE XLVI"},
    {"fecha": "2026-11-04", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES YPF S.A. CLASE XXXII - NUEVA"},
    {"fecha": "2026-11-05", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES PAMPA ENERGÍA S.A. CLASE XXIII"},
    {"fecha": "2026-11-06", "horario": "10:00 - 15:00", "titulo": "LETRAS DE TESORERÍA DE LA PROVINCIA DE BUENOS AIRES - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "BONO DEL TESORO NACIONAL CAPITALIZABLE EN PESOS CON VTO. 30/06/2027 (T30J7) - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "BONO DEL TESORO NACIONAL EN PESOS CERO CUPÓN CON AJUSTE POR CER VTO. 30/06/2028 (TZX28) - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "LETRA DEL TESORO NACIONAL CAPITALIZABLE EN PESOS CON VTO. 29/01/2027 (S29E7) - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "LETRA DEL TESORO NACIONAL EN PESOS A TASA TAMAR CON VTO. 30/04/2027 (M30A7) - NUEVA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "LETRA DEL TESORO NACIONAL VINCULADA AL DÓLAR ESTADOUNIDENSE CERO CUPÓN CON VTO. 15/01/2027 (D15E7) - REAPERTURA"},
    {"fecha": "2026-11-12", "horario": "10:00 - 16:00", "titulo": "ON BAJO IMPACTO MADERO HARBOUR SERIE II"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 15 (REAPERTURA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 18 (REAPERTURA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 21 (NUEVA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 22 (NUEVA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 23 (NUEVA)"},
    {"fecha": "2026-11-13", "horario": "10:00 - 16:00", "titulo": "ON BAJO IMPACTO INGACOT COMERCIAL SERIE I"},
    {"fecha": "2026-11-18", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES VISTA ENERGY ARGENTINA S.A.U. CLASE XXVI"},
    {"fecha": "2026-11-19", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES TELECOM ARGENTINA S.A. CLASE XXI"},
    {"fecha": "2026-11-20", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES IRSA INVERSIONES Y REPRESENTACIONES S.A. CLASE XXII"},
    {"fecha": "2026-11-25", "horario": "10:00 - 15:00", "titulo": "LICITACIÓN SECRETARÍA DE FINANZAS — LECAP (S29E7 / S26F7), BONCAP Y CER"},
    {"fecha": "2026-11-26", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES TRANSPORTADORA DE GAS DEL SUR S.A. (TGS) CLASE VIII"},
    {"fecha": "2026-12-02", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES ALUAR ALUMINIO ARGENTINO S.A.I.C. SERIE XI"},
    {"fecha": "2026-12-03", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES CENTRAL PUERTO S.A. CLASE VI"},
    {"fecha": "2026-12-09", "horario": "10:00 - 15:00", "titulo": "LICITACIÓN SECRETARÍA DE FINANZAS — LETRAS Y BONOS DEL TESORO NACIONAL"},
    {"fecha": "2026-12-16", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES LOMA NEGRA C.I.A.S.A. CLASE V"}
]

def consultar_url_mae_con_puentes(target_url: str) -> Any:
    sess = MAE_HTTP_SESSION
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://marketdata.mae.com.ar",
        "Referer": "https://marketdata.mae.com.ar/calendario-licitaciones"
    }
    candidatos = [
        target_url,
        f"https://api.allorigins.win/raw?url={quote(target_url, safe='')}",
        f"https://api.codetabs.com/v1/proxy?quest={quote(target_url, safe='')}",
        f"https://corsproxy.io/?{quote(target_url, safe='')}"
    ]
    for u in candidatos:
        try:
            r = sess.get(u, headers=hdrs, timeout=2.5, verify=False)
            if r.status_code == 200 and r.text.strip():
                return r.json()
        except Exception:
            pass
    return None


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

app = FastAPI(title="Monitor Maestro De Mercado API v6.0", version="6.0.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)

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

def normalizar_valor(texto: Any) -> Optional[float]:
    if texto is None:
        return None
    t = str(texto).strip().upper().replace("−", "-")
    multiplicador = 1000.0 if ("B" in t or "K" in t) else 1.0
    t_clean = re.sub(r"[^\d,\.\-]", "", t)
    if not t_clean:
        return None
    if "," in t_clean and "." in t_clean:
        t_clean = t_clean.replace(".", "").replace(",", ".")
    elif "." in t_clean and "," not in t_clean:
        partes = t_clean.split(".")
        pref_sin_signo = partes[0].lstrip("-")
        if len(partes) > 2 or (len(partes[-1]) == 3 and pref_sin_signo not in ("0", "") and int(pref_sin_signo or "0") >= 10):
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

MESES_CORTOS_ES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}

def extraer_mes_corte_legible(texto_fecha: str) -> str:
    if not texto_fecha:
        return ""
    s = str(texto_fecha).strip()
    m_iso = re.search(r"(20\d{2})[-/](\d{1,2})", s)
    if m_iso and 1 <= int(m_iso.group(2)) <= 12:
        return f"{MESES_CORTOS_ES[int(m_iso.group(2))]} {m_iso.group(1)}"
    m_lat = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](20\d{2})", s)
    if m_lat and 1 <= int(m_lat.group(2)) <= 12:
        return f"{MESES_CORTOS_ES[int(m_lat.group(2))]} {m_lat.group(3)}"
    m_my = re.search(r"^(\d{1,2})[-/](20\d{2})$", s)
    if m_my and 1 <= int(m_my.group(1)) <= 12:
        return f"{MESES_CORTOS_ES[int(m_my.group(1))]} {m_my.group(2)}"
    mapeo_txt = {
        "enero": "Ene", "ene": "Ene", "jan": "Ene", "febrero": "Feb", "feb": "Feb",
        "marzo": "Mar", "mar": "Mar", "abril": "Abr", "abr": "Abr", "apr": "Abr",
        "mayo": "May", "may": "May", "junio": "Jun", "jun": "Jun", "julio": "Jul", "jul": "Jul",
        "agosto": "Ago", "ago": "Ago", "aug": "Ago", "septiembre": "Sep", "setiembre": "Sep", "sep": "Sep",
        "octubre": "Oct", "oct": "Oct", "noviembre": "Nov", "nov": "Nov", "diciembre": "Dic", "dic": "Dic", "dec": "Dic"
    }
    s_low = s.lower()
    anio_m = re.search(r"(20\d{2})", s_low)
    anio_str = anio_m.group(1) if anio_m else str(datetime.utcnow().year)
    for k_mes, v_mes in mapeo_txt.items():
        if re.search(r"\b" + k_mes + r"\b", s_low):
            return f"{v_mes} {anio_str}"
    return s

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
    "call interbancario": ("TASA_CALL", "Tasa Call Interbancaria en Pesos", "tasas_y_monetario"),
    "dólar blue": ("DOLAR_BLUE", "Dólar Blue", "dolar_y_cambiario"),
    # "dólar mep" desactivado de indicadores.ar para usar exclusivamente AL30/AL30D y Rava
    # "dólar ccl" desactivado de indicadores.ar para usar exclusivamente AL30/AL30C y Rava
    "dólar oficial": ("DOLAR_OFICIAL", "Dólar Oficial Minorista", "dolar_y_cambiario"),
    "dólar tarjeta": ("DOLAR_TARJETA", "Dólar Tarjeta", "dolar_y_cambiario"),
    "brecha cambiaria": ("BRECHA_CAMBIARIA", "Brecha Cambiaria", "dolar_y_cambiario"),
    "inflación mensual": ("INFLACION_MENSUAL", "Inflación Mensual", "inflacion_y_ajuste"),
    "inflación interanual": ("INFLACION_INTERANUAL", "Inflación Interanual", "inflacion_y_ajuste"),
    "índice cer": ("INDICE_CER", "Índice CER", "inflacion_y_ajuste"),
    "índice uva": ("INDICE_UVA", "Índice UVA", "inflacion_y_ajuste"),
    "índice icl": ("INDICE_ICL", "Índice ICL (Alquileres)", "inflacion_y_ajuste"),
    "inflación esperada": ("INFLACION_REM", "Inflación Esperada REM (12m)", "inflacion_y_ajuste"),
    "inflación rem": ("INFLACION_REM", "Inflación Esperada REM (12m)", "inflacion_y_ajuste"),
    "riesgo país": ("RIESGO_PAIS", "Riesgo País EMBI+", "indices"),
    "riesgo pais": ("RIESGO_PAIS", "Riesgo País EMBI+", "indices"),
    "merval": ("MERVAL_ARS", "MSCI Merval en Pesos (MMSCI)", "indices"),
    "s&p merval": ("MERVAL_ARS", "MSCI Merval en Pesos (MMSCI)", "indices"),
    "merval en usd": ("MERVAL_USD", "MSCI Merval en Dólares (CCL)", "indices"),
    "s&p merval en usd": ("MERVAL_USD", "MSCI Merval en Dólares (CCL)", "indices"),
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
    "desempleo": ("DESEMPLEO", "Tasa de Desempleo (EPH)", "macro_y_fiscal"),
    "tasa de desempleo": ("DESEMPLEO", "Tasa de Desempleo (EPH)", "macro_y_fiscal"),
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
    ("EWZ", "ewz.us", "INDICE_EWZ", "iShares MSCI Brazil ETF (EWZ)", "brasil", "usd"),
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
    {"variable": "MERVAL_ARS", "descripcion": "Merval", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "81.255 pts", "variacion": "+0,1%", "var_mes": "-11,8%", "var_anio": "-24,1%", "valor": 81255.0, "fecha": "Mes: -11,8% | Año: -24,1%"},
    {"variable": "MERVAL_USD", "descripcion": "Merval USD", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "46 pts", "variacion": "-1,2%", "var_mes": "-17,4%", "var_anio": "-34,6%", "valor": 46.0, "fecha": "Mes: -17,4% | Año: -34,6%"},
    {"variable": "RIESGO_PAIS", "descripcion": "Riesgo País EMBI+", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "1.038 pb", "variacion": "+11,5%", "var_mes": "+31,9%", "var_anio": "+81,8%", "valor": 1038.0, "fecha": "Mes: +31,9% | Año: +81,8%"},
    {"variable": "INDICE_SP500", "descripcion": "S&P 500 (SPX)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "5.995,54 pts", "variacion": "+0,38%", "valor": 5995.54, "fecha": "Cierre"},
    {"variable": "INDICE_NASDAQ", "descripcion": "Nasdaq 100 (NDX)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "21.117,20 pts", "variacion": "+0,09%", "valor": 21117.20, "fecha": "Cierre"},
    {"variable": "INDICE_VIX", "descripcion": "Índice de Volatilidad (VIX)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "14,94 pts", "variacion": "-1,71%", "valor": 14.94, "fecha": "Cierre"},
    {"variable": "INDICE_DJI", "descripcion": "Dow Jones Industrial (DJI)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "43.988,99 pts", "variacion": "+0,59%", "valor": 43988.99, "fecha": "Cierre"},
    {"variable": "INDICE_RUSSELL", "descripcion": "Russell 2000 (RUT)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "2.399,61 pts", "variacion": "+0,71%", "valor": 2399.61, "fecha": "Cierre"},
    {"variable": "MONEDA_DXY", "descripcion": "Índice Dólar (DXY)", "categoria": "indices", "subcategoria": "estados_unidos", "valor_formateado": "104,35 pts", "variacion": "+0,15%", "valor": 104.35, "fecha": "Cierre"},
    {"variable": "INDICE_EWZ", "descripcion": "iShares MSCI Brazil ETF (EWZ)", "categoria": "indices", "subcategoria": "brasil", "valor_formateado": "US$ 26,45", "variacion": "+0,85%", "valor": 26.45, "fecha": "En vivo"},
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
    "bopreales": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=BOPREALES&moneda=MEP",
    "bopreales_mep": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=BOPREALES&moneda=MEP",
    "bopreales_ars": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=BOPREALES&moneda=ARS",
    "lecaps": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=LECAP%20y%20BONCAP&moneda=ARS",
    "cer": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=BONOS%20CER&moneda=ARS",
    "soberanos": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=MEP",
    "soberanos_mep": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=MEP",
    "soberanos_ars": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=ARS",
    "soberanos_ccl": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=CCL",
    "duales": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=DUALES&moneda=ARS&incluir_cer_tamar=true&incluir_dl_tamar=true",
    "tamar": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=LETAM&moneda=ARS",
    "dollar_linked": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=D%C3%B3lar%20Linked&moneda=ARS"
}

CACHE_CURVAS_SJB: Dict[str, Any] = {}
CACHE_INFLACION_IMPLICITA: Any = None
CACHE_BANDAS_CAMBIARIAS: Dict[str, Any] = {"banda_inferior": 702.92, "banda_superior": 1976.68, "dolar_mayorista": 1544.00, "fecha_banda": "2026-11-20"}
CACHE_LICITACIONES_MAE: List[Dict[str, Any]] = []

CACHE_TASAS_PERSISTENTE: Dict[str, Dict[str, Any]] = {
    "TASA_TAMAR": {"variable": "TASA_TAMAR", "descripcion": "Tasa Referencia TAMAR", "categoria": "tasas_y_monetario", "valor_formateado": "34,25% TNA", "variacion": "0,0%", "valor": 34.25, "fecha": "Mercado"},
    "TASA_BADLAR": {"variable": "TASA_BADLAR", "descripcion": "Tasa BADLAR", "categoria": "tasas_y_monetario", "valor_formateado": "32,50% TNA", "variacion": "0,0%", "valor": 32.50, "fecha": "Mercado"},
    "TASA_PLAZO_FIJO": {"variable": "TASA_PLAZO_FIJO", "descripcion": "Tasa Plazo Fijo 30d (Prom. BCRA)", "categoria": "tasas_y_monetario", "valor_formateado": "20,44% TNA", "variacion": "0,0%", "valor": 20.44, "fecha": "BCRA Oficial (29 bancos)"},
    "TASA_CALL": {"variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "21,66% TNA", "variacion": "0,0%", "valor": 21.66, "fecha": "Mercado"},
    "TASA_TM20": {"variable": "TASA_TM20", "descripcion": "Tasa TM20 en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "22,00% TNA", "variacion": "0,0%", "valor": 22.0, "fecha": "Mercado"},
    "TASA_PASES": {"variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA", "categoria": "tasas_y_monetario", "valor_formateado": "22,06% TNA", "variacion": "0,0%", "valor": 22.06, "fecha": "Mercado"},
    "TASA_CAUCION_1D": {"variable": "TASA_CAUCION_1D", "descripcion": "Caución Bursátil 1 Día (Pesos)", "categoria": "tasas_y_monetario", "valor_formateado": "14,10% TNA", "variacion": "-", "var_mes": "-21,7%", "var_anio": "-88,3%", "valor": 14.10, "fecha": "Mes: -21,7% | Año: -88,3%"},
    "TASA_CAUCION_7D": {"variable": "TASA_CAUCION_7D", "descripcion": "Caución Bursátil 7 Días (Pesos)", "categoria": "tasas_y_monetario", "valor_formateado": "15,10% TNA", "variacion": "-", "var_mes": "-22,2%", "var_anio": "-31,4%", "valor": 15.10, "fecha": "Mes: -22,2% | Año: -31,4%"},
    "TASA_CAUCION_14D": {"variable": "TASA_CAUCION_14D", "descripcion": "Caución Bursátil 14 Días (Pesos)", "categoria": "tasas_y_monetario", "valor_formateado": "17,10% TNA", "variacion": "-", "var_mes": "-10,9%", "var_anio": "-22,3%", "valor": 17.10, "fecha": "Mes: -10,9% | Año: -22,3%"}
}

CACHE_FINVIZ_SECTORES: Dict[str, Any] = {
    "1D": [
        {"name": "Consumer Cyclical", "ticker": "XLY", "perf": 1.40},
        {"name": "Financial", "ticker": "XLF", "perf": 1.15},
        {"name": "Energy", "ticker": "XLE", "perf": 0.84},
        {"name": "Industrials", "ticker": "XLI", "perf": 0.65},
        {"name": "Technology", "ticker": "XLK", "perf": 0.52},
        {"name": "Communication Services", "ticker": "XLC", "perf": 0.48},
        {"name": "Basic Materials", "ticker": "XLB", "perf": 0.35},
        {"name": "Consumer Defensive", "ticker": "XLP", "perf": -0.15},
        {"name": "Healthcare", "ticker": "XLV", "perf": -0.28},
        {"name": "Utilities", "ticker": "XLU", "perf": -0.45},
        {"name": "Real Estate", "ticker": "XLRE", "perf": -0.62}
    ],
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
    "dolar_y_cambiario": [
        {"variable": "DOLAR_OFICIAL", "descripcion": "Dólar Oficial", "categoria": "dolar_y_cambiario", "compra": 1520.0, "venta": 1570.0, "valor": 1570.0, "valor_formateado": "$1.570,00", "variacion": "0,00%", "fecha": "En vivo"},
        {"variable": "DOLAR_BLUE", "descripcion": "Dólar Blue", "categoria": "dolar_y_cambiario", "compra": 1545.0, "venta": 1565.0, "valor": 1565.0, "valor_formateado": "$1.565,00", "variacion": "+0,64%", "fecha": "En vivo"},
        {"variable": "DOLAR_NETFLIX", "descripcion": "Dólar Netflix", "categoria": "dolar_y_cambiario", "compra": None, "venta": 2370.70, "valor": 2370.70, "valor_formateado": "$2.370,70", "variacion": "0,00%", "fecha": "En vivo"},
        {"variable": "DOLAR_MEP", "descripcion": "Dólar MEP", "categoria": "dolar_y_cambiario", "compra": None, "venta": 1584.11, "valor": 1584.11, "valor_formateado": "$1.584,11", "variacion": "0,00%", "fecha": "AL30 / AL30D"},
        {"variable": "DOLAR_CCL", "descripcion": "Dólar CCL", "categoria": "dolar_y_cambiario", "compra": None, "venta": 1684.47, "valor": 1684.47, "valor_formateado": "$1.684,47", "variacion": "0,00%", "fecha": "AL30 / AL30C"},
        {"variable": "DOLAR_CRIPTO", "descripcion": "Dólar Cripto", "categoria": "dolar_y_cambiario", "compra": 1654.73, "venta": 1658.43, "valor": 1658.43, "valor_formateado": "$1.658,43", "variacion": "+0,55%", "fecha": "En vivo"},
        {"variable": "DOLAR_TARJETA", "descripcion": "Dólar Tarjeta", "categoria": "dolar_y_cambiario", "compra": None, "venta": 2041.00, "valor": 2041.00, "valor_formateado": "$2.041,00", "variacion": "0,00%", "fecha": "En vivo"},
        {"variable": "DOLAR_A3500", "descripcion": "Dólar Mayorista", "categoria": "dolar_y_cambiario", "compra": 1535.0, "venta": 1544.0, "valor": 1544.0, "valor_formateado": "$1.544,00", "variacion": "-0,10%", "fecha": "En vivo"},
        {"variable": "DOLAR_FUTURO", "descripcion": "Dólar Futuro", "categoria": "dolar_y_cambiario", "compra": 1563.0, "venta": 1563.5, "valor": 1563.5, "valor_formateado": "$1.563,50", "variacion": "0,00%", "fecha": "En vivo"}
    ],
    "monedas": [
        {"variable": "MONEDA_USDARS", "descripcion": "Dólar vs Peso Argentino (USD/ARS)", "categoria": "monedas", "valor_formateado": "$1.544,00 ARS", "variacion": "0,00%", "valor": 1544.0, "fecha": "En vivo"},
        {"variable": "MONEDA_USDBRL", "descripcion": "Dólar vs Real Brasileño (USD/BRL)", "categoria": "monedas", "valor_formateado": "R$ 5,7600", "variacion": "0,00%", "valor": 5.76, "fecha": "En vivo"},
        {"variable": "MONEDA_EURUSD", "descripcion": "Euro vs Dólar (EUR/USD)", "categoria": "monedas", "valor_formateado": "1,0855 USD", "variacion": "0,00%", "valor": 1.0855, "fecha": "En vivo"},
        {"variable": "MONEDA_EURARS", "descripcion": "Euro vs Peso Argentino (EUR/ARS)", "categoria": "monedas", "valor_formateado": "$1.676,01 ARS", "variacion": "0,00%", "valor": 1676.01, "fecha": "En vivo"},
        {"variable": "MONEDA_USDCLP", "descripcion": "Dólar vs Peso Chileno (USD/CLP)", "categoria": "monedas", "valor_formateado": "CLP$ 958,40", "variacion": "0,00%", "valor": 958.4, "fecha": "En vivo"}
    ],
    "cripto": [],
    "cotizacion_bonos": [],
    "indices": [dict(x) for x in INDICES_BASE_MEMORIA],
    "inflacion_y_ajuste": [
        {"variable": "INFLACION_MENSUAL", "descripcion": "Inflación Mensual (Oct 2026)", "categoria": "inflacion_y_ajuste", "valor_formateado": "2,1%", "variacion": "2,1%", "valor": 2.1, "fecha": "Corte: Oct 2026"},
        {"variable": "INFLACION_INTERANUAL", "descripcion": "Inflación Interanual (Oct 2026)", "categoria": "inflacion_y_ajuste", "valor_formateado": "29,4%", "variacion": "29,4%", "valor": 29.4, "fecha": "Corte: Oct 2026"},
        {"variable": "INFLACION_REM", "descripcion": "Inflación Esperada REM (12m)", "categoria": "inflacion_y_ajuste", "valor_formateado": "21,8%", "variacion": "REM BCRA", "valor": 21.8, "fecha": "BCRA Oficial"},
        {"variable": "INDICE_CER", "descripcion": "Índice CER", "categoria": "inflacion_y_ajuste", "valor_formateado": "685,40", "variacion": "+0,07%", "valor": 685.40, "fecha": "En vivo"},
        {"variable": "INDICE_UVA", "descripcion": "Índice UVA", "categoria": "inflacion_y_ajuste", "valor_formateado": "$1.712,50", "variacion": "+0,07%", "valor": 1712.50, "fecha": "En vivo"},
        {"variable": "INDICE_ICL", "descripcion": "Índice ICL (Alquileres)", "categoria": "inflacion_y_ajuste", "valor_formateado": "26,45", "variacion": "+0,08%", "valor": 26.45, "fecha": "En vivo"}
    ],
    "metales": [],
    "petroleo": [],
    "macro_y_fiscal": [
        {"variable": "BALANZA_COMERCIAL", "descripcion": "Balanza Comercial", "categoria": "macro_y_fiscal", "valor_formateado": "US$ 1.120 M", "variacion": "+12,4%", "valor": 1120.0, "fecha": "INDEC ICA"},
        {"variable": "EXPORTACIONES", "descripcion": "Exportaciones", "categoria": "macro_y_fiscal", "valor_formateado": "US$ 7.240 M", "variacion": "+8,1%", "valor": 7240.0, "fecha": "INDEC ICA"},
        {"variable": "IMPORTACIONES", "descripcion": "Importaciones", "categoria": "macro_y_fiscal", "valor_formateado": "US$ 6.120 M", "variacion": "+6,5%", "valor": 6120.0, "fecha": "INDEC ICA"},
        {"variable": "RESULTADO_FISCAL", "descripcion": "Resultado Fiscal Primario", "categoria": "macro_y_fiscal", "valor_formateado": "$ 980.500 M", "variacion": "Superávit", "valor": 980500.0, "fecha": "Min. Economía"},
        {"variable": "RECAUDACION_TRIBUTARIA", "descripcion": "Recaudación Tributaria", "categoria": "macro_y_fiscal", "valor_formateado": "$ 14.850.000 M", "variacion": "+38,2% i.a.", "valor": 14850000.0, "fecha": "ARCA"},
        {"variable": "EMAE_ACTIVIDAD", "descripcion": "EMAE Actividad Económica", "categoria": "macro_y_fiscal", "valor_formateado": "+4,2% i.a.", "variacion": "+0,4% m/m", "valor": 4.2, "fecha": "INDEC"},
        {"variable": "PIB_ARG", "descripcion": "PIB Argentina (Var. i.a.)", "categoria": "macro_y_fiscal", "valor_formateado": "+3,8% i.a.", "variacion": "+3,8%", "valor": 3.8, "fecha": "INDEC"},
        {"variable": "SALARIO_MINIMO", "descripcion": "Salario Mínimo Vital y Móvil", "categoria": "macro_y_fiscal", "valor_formateado": "$ 322.000", "variacion": "Vigente", "valor": 322000.0, "fecha": "Oficial"},
        {"variable": "JUBILACION_MINIMA", "descripcion": "Jubilación Mínima (con bono)", "categoria": "macro_y_fiscal", "valor_formateado": "$ 385.000", "variacion": "+2,1%", "valor": 385000.0, "fecha": "ANSES"},
        {"variable": "CANASTA_BASICA_TOTAL", "descripcion": "Canasta Básica Total (Hogar 4)", "categoria": "macro_y_fiscal", "valor_formateado": "$ 1.185.400", "variacion": "+2,0%", "valor": 1185400.0, "fecha": "INDEC"},
        {"variable": "CANASTA_BASICA_ALIMENTARIA", "descripcion": "Canasta Básica Alimentaria", "categoria": "macro_y_fiscal", "valor_formateado": "$ 528.900", "variacion": "+1,9%", "valor": 528900.0, "fecha": "INDEC"},
        {"variable": "POBREZA", "descripcion": "Tasa de Pobreza (EPH)", "categoria": "macro_y_fiscal", "valor_formateado": "38,1%", "variacion": "-14,8 pp", "valor": 38.1, "fecha": "INDEC EPH"},
        {"variable": "INDIGENCIA", "descripcion": "Tasa de Indigencia (EPH)", "categoria": "macro_y_fiscal", "valor_formateado": "8,2%", "variacion": "-9,9 pp", "valor": 8.2, "fecha": "INDEC EPH"},
        {"variable": "TRABAJADORES_REGISTRADOS", "descripcion": "Trabajadores Registrados", "categoria": "macro_y_fiscal", "valor_formateado": "10,18 M", "variacion": "+0,2%", "valor": 10.18, "fecha": "SIPA"}
    ],
    "micro_y_sectores": [
        {"variable": "PRESTAMOS_PRIVADOS", "descripcion": "Préstamos al Sector Privado", "categoria": "micro_y_sectores", "valor_formateado": "$ 74.200 MM", "variacion": "+4,8% m/m", "valor": 74200.0, "fecha": "BCRA"},
        {"variable": "CREDITOS_HIPOTECARIOS", "descripcion": "Créditos Hipotecarios (Stock)", "categoria": "micro_y_sectores", "valor_formateado": "$ 3.450 MM", "variacion": "+9,5% m/m", "valor": 3450.0, "fecha": "BCRA"},
        {"variable": "PRODUCCION_INDUSTRIAL", "descripcion": "Producción Industrial (IPI)", "categoria": "micro_y_sectores", "valor_formateado": "+5,1% i.a.", "variacion": "+0,6% m/m", "valor": 5.1, "fecha": "INDEC"},
        {"variable": "CAPACIDAD_INSTALADA", "descripcion": "Capacidad Instalada Industria", "categoria": "micro_y_sectores", "valor_formateado": "63,4%", "variacion": "+1,2 pp", "valor": 63.4, "fecha": "INDEC"},
        {"variable": "ISAC_CONSTRUCCION", "descripcion": "ISAC Construcción", "categoria": "micro_y_sectores", "valor_formateado": "+6,8% i.a.", "variacion": "+1,1% m/m", "valor": 6.8, "fecha": "INDEC"},
        {"variable": "DESPACHOS_CEMENTO", "descripcion": "Despachos de Cemento", "categoria": "micro_y_sectores", "valor_formateado": "945.200 tn", "variacion": "+7,4% i.a.", "valor": 945200.0, "fecha": "AFCP"},
        {"variable": "PRODUCCION_AUTOMOTRIZ", "descripcion": "Producción Automotriz", "categoria": "micro_y_sectores", "valor_formateado": "48.350 unid.", "variacion": "+6,2% i.a.", "valor": 48350.0, "fecha": "ADEFA"},
        {"variable": "ESCRITURAS_CABA", "descripcion": "Escrituras Inmobiliarias CABA", "categoria": "micro_y_sectores", "valor_formateado": "5.420 op.", "variacion": "+24,5% i.a.", "valor": 5420.0, "fecha": "Col. Escribanos"},
        {"variable": "DEMANDA_ELECTRICA", "descripcion": "Demanda Eléctrica (CAMMESA)", "categoria": "micro_y_sectores", "valor_formateado": "11.840 GWh", "variacion": "+2,3% i.a.", "valor": 11840.0, "fecha": "CAMMESA"},
        {"variable": "VENTAS_SUPERMERCADOS", "descripcion": "Ventas en Supermercados", "categoria": "micro_y_sectores", "valor_formateado": "+1,8% i.a.", "variacion": "+0,5% m/m", "valor": 1.8, "fecha": "INDEC"},
        {"variable": "VENTAS_SHOPPINGS", "descripcion": "Ventas en Shoppings", "categoria": "micro_y_sectores", "valor_formateado": "+4,5% i.a.", "variacion": "+1,1% m/m", "valor": 4.5, "fecha": "INDEC"},
        {"variable": "CONFIANZA_CONSUMIDOR", "descripcion": "Índice Confianza del Consumidor", "categoria": "micro_y_sectores", "valor_formateado": "46,2 pts", "variacion": "+2,4%", "valor": 46.2, "fecha": "UTDT"},
        {"variable": "CONFIANZA_GOBIERNO", "descripcion": "Índice Confianza en el Gobierno", "categoria": "micro_y_sectores", "valor_formateado": "2,48 pts", "variacion": "+1,6%", "valor": 2.48, "fecha": "UTDT"}
    ],
    "commodities_agro_energia": [
        {"variable": "AGRO_SOJA", "descripcion": "Soja (Pizarra Rosario)", "categoria": "commodities_agro_energia", "precio_ars": "$ 485.000", "precio_usd": "US$ 314,00", "valor_ars": 485000.0, "valor_usd": 314.0, "valor_formateado": "$ 485.000 / US$ 314,00", "variacion": "0,00%", "valor": 314.0, "fecha": "Pizarra Rosario"},
        {"variable": "AGRO_MAIZ", "descripcion": "Maíz (Pizarra Rosario)", "categoria": "commodities_agro_energia", "precio_ars": "$ 285.000", "precio_usd": "US$ 184,50", "valor_ars": 285000.0, "valor_usd": 184.5, "valor_formateado": "$ 285.000 / US$ 184,50", "variacion": "0,00%", "valor": 184.5, "fecha": "Pizarra Rosario"},
        {"variable": "AGRO_TRIGO", "descripcion": "Trigo (Pizarra Rosario)", "categoria": "commodities_agro_energia", "precio_ars": "$ 295.000", "precio_usd": "US$ 191,00", "valor_ars": 295000.0, "valor_usd": 191.0, "valor_formateado": "$ 295.000 / US$ 191,00", "variacion": "0,00%", "valor": 191.0, "fecha": "Pizarra Rosario"},
        {"variable": "AGRO_GIRASOL", "descripcion": "Girasol (Pizarra Rosario)", "categoria": "commodities_agro_energia", "precio_ars": "$ 540.000", "precio_usd": "US$ 349,50", "valor_ars": 540000.0, "valor_usd": 349.5, "valor_formateado": "$ 540.000 / US$ 349,50", "variacion": "0,00%", "valor": 349.5, "fecha": "Pizarra Rosario"},
        {"variable": "AGRO_SORGO", "descripcion": "Sorgo (Pizarra Rosario)", "categoria": "commodities_agro_energia", "precio_ars": "$ 255.000", "precio_usd": "US$ 165,00", "valor_ars": 255000.0, "valor_usd": 165.0, "valor_formateado": "$ 255.000 / US$ 165,00", "variacion": "0,00%", "valor": 165.0, "fecha": "Pizarra Rosario"}
    ]
}
CACHE_LOCK = threading.Lock()
ULTIMA_ACTUALIZACION = ""

def fetch_quote_externa(sym_y: str, sym_s: str) -> Optional[Dict[str, float]]:
    h = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_y}?interval=1d&range=1d"
        r = GLOBAL_HTTP_SESSION.get(url, headers=h, timeout=2.8)
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
        r_s = GLOBAL_HTTP_SESSION.get(url_s, headers=h, timeout=2.5)
        if r_s.status_code == 200 and "Close" in r_s.text:
            lines = r_s.text.strip().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split(",")
                if len(parts) >= 6 and parts[5] != "N/D":
                    close_p = float(parts[5])
                    open_p = float(parts[2]) if parts[2] != "N/D" else close_p
                    chg_pct = ((close_p - open_p) / open_p * 100) if open_p else 0.0
                    return {"price": round(close_p, 2), "change_pct": round(chg_pct, 2)}
    except Exception:
        pass
    return None

def _parse_float_limpio(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("%", "").replace("−", "-")
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif "." in s:
        partes = s.split(".")
        if len(partes) == 2 and len(partes[1]) == 3 and partes[0].lstrip("-") not in ("0", "", "1", "2", "3", "4", "5", "6", "7", "8", "9"):
            s = s.replace(".", "")
    try:
        return float(s)
    except Exception:
        return None

def _fmt_pct_con_signo(val: Optional[float], def_val: float = 0.0) -> str:
    v = val if val is not None else def_val
    return f"+{v:.1f}%".replace(".", ",") if v > 0 else f"{v:.1f}%".replace(".", ",")


ACCIONES_MERVAL_BASE = [
    {"especie": "ALUA", "ultimo": 915.0, "dia": 1.44, "mes": -4.20, "anio": -12.50, "anterior": 902.0, "apertura": 905.0, "minimo": 900.0, "maximo": 920.0, "hora": "17:00", "vol_nominal": 1450200, "vol_efectivo": 1326933000},
    {"especie": "BBAR", "ultimo": 14850.0, "dia": 2.98, "mes": 8.40, "anio": 115.20, "anterior": 14420.0, "apertura": 14500.0, "minimo": 14400.0, "maximo": 14920.0, "hora": "17:00", "vol_nominal": 412300, "vol_efectivo": 6122655000},
    {"especie": "BMA", "ultimo": 18200.0, "dia": 2.54, "mes": 6.80, "anio": 124.60, "anterior": 17750.0, "apertura": 17800.0, "minimo": 17700.0, "maximo": 18350.0, "hora": "17:00", "vol_nominal": 389100, "vol_efectivo": 7081620000},
    {"especie": "BYMA", "ultimo": 395.0, "dia": 0.77, "mes": 1.20, "anio": 38.40, "anterior": 392.0, "apertura": 392.0, "minimo": 390.0, "maximo": 398.0, "hora": "17:00", "vol_nominal": 2890000, "vol_efectivo": 1141550000},
    {"especie": "CEPU", "ultimo": 2410.0, "dia": 1.90, "mes": 4.10, "anio": 64.20, "anterior": 2365.0, "apertura": 2370.0, "minimo": 2360.0, "maximo": 2425.0, "hora": "17:00", "vol_nominal": 1120400, "vol_efectivo": 2700164000},
    {"especie": "COME", "ultimo": 298.5, "dia": 3.47, "mes": 11.20, "anio": 142.80, "anterior": 288.5, "apertura": 290.0, "minimo": 288.0, "maximo": 301.0, "hora": "17:00", "vol_nominal": 8450000, "vol_efectivo": 2522325000},
    {"especie": "CRES", "ultimo": 2180.0, "dia": -0.91, "mes": -2.40, "anio": 29.80, "anterior": 2200.0, "apertura": 2195.0, "minimo": 2170.0, "maximo": 2210.0, "hora": "17:00", "vol_nominal": 540200, "vol_efectivo": 1177636000},
    {"especie": "CVH", "ultimo": 9850.0, "dia": -1.50, "mes": -3.10, "anio": 41.20, "anterior": 10000.0, "apertura": 9980.0, "minimo": 9800.0, "maximo": 10020.0, "hora": "17:00", "vol_nominal": 68400, "vol_efectivo": 673740000},
    {"especie": "EDN", "ultimo": 3690.0, "dia": 2.22, "mes": 5.90, "anio": 88.50, "anterior": 3610.0, "apertura": 3620.0, "minimo": 3600.0, "maximo": 3715.0, "hora": "17:00", "vol_nominal": 620100, "vol_efectivo": 2288169000},
    {"especie": "GGAL", "ultimo": 12450.0, "dia": 3.15, "mes": 9.20, "anio": 148.90, "anterior": 12070.0, "apertura": 12150.0, "minimo": 12050.0, "maximo": 12520.0, "hora": "17:00", "vol_nominal": 1850400, "vol_efectivo": 23037480000},
    {"especie": "IRSA", "ultimo": 2640.0, "dia": -0.56, "mes": 1.80, "anio": 52.10, "anterior": 2655.0, "apertura": 2650.0, "minimo": 2625.0, "maximo": 2670.0, "hora": "17:00", "vol_nominal": 410000, "vol_efectivo": 1082400000},
    {"especie": "LOMA", "ultimo": 3890.0, "dia": -1.77, "mes": -5.10, "anio": 19.40, "anterior": 3960.0, "apertura": 3950.0, "minimo": 3875.0, "maximo": 3965.0, "hora": "17:00", "vol_nominal": 495000, "vol_efectivo": 1925550000},
    {"especie": "METR", "ultimo": 2950.0, "dia": 4.24, "mes": 14.50, "anio": 118.40, "anterior": 2830.0, "apertura": 2850.0, "minimo": 2830.0, "maximo": 2975.0, "hora": "17:00", "vol_nominal": 530200, "vol_efectivo": 1564090000},
    {"especie": "MIRG", "ultimo": 26800.0, "dia": -2.19, "mes": -6.80, "anio": 8.90, "anterior": 27400.0, "apertura": 27300.0, "minimo": 26700.0, "maximo": 27400.0, "hora": "17:00", "vol_nominal": 19500, "vol_efectivo": 522600000},
    {"especie": "PAMP", "ultimo": 6780.0, "dia": 1.65, "mes": 3.40, "anio": 74.10, "anterior": 6670.0, "apertura": 6690.0, "minimo": 6650.0, "maximo": 6810.0, "hora": "17:00", "vol_nominal": 980500, "vol_efectivo": 6647790000},
    {"especie": "SUPV", "ultimo": 4120.0, "dia": 2.74, "mes": 7.60, "anio": 132.40, "anterior": 4010.0, "apertura": 4030.0, "minimo": 4000.0, "maximo": 4150.0, "hora": "17:00", "vol_nominal": 890300, "vol_efectivo": 3668036000},
    {"especie": "TECO2", "ultimo": 3740.0, "dia": -1.32, "mes": -2.90, "anio": 34.50, "anterior": 3790.0, "apertura": 3780.0, "minimo": 3720.0, "maximo": 3800.0, "hora": "17:00", "vol_nominal": 460200, "vol_efectivo": 1721148000},
    {"especie": "TGNO4", "ultimo": 5490.0, "dia": 3.78, "mes": 12.10, "anio": 96.40, "anterior": 5290.0, "apertura": 5310.0, "minimo": 5280.0, "maximo": 5520.0, "hora": "17:00", "vol_nominal": 415800, "vol_efectivo": 2282742000},
    {"especie": "TGSU2", "ultimo": 9620.0, "dia": 1.80, "mes": 4.90, "anio": 82.30, "anterior": 9450.0, "apertura": 9480.0, "minimo": 9440.0, "maximo": 9660.0, "hora": "17:00", "vol_nominal": 512000, "vol_efectivo": 4925440000},
    {"especie": "TRAN", "ultimo": 3510.0, "dia": 2.33, "mes": 6.20, "anio": 91.80, "anterior": 3430.0, "apertura": 3440.0, "minimo": 3420.0, "maximo": 3530.0, "hora": "17:00", "vol_nominal": 480900, "vol_efectivo": 1687959000},
    {"especie": "TXAR", "ultimo": 845.0, "dia": -1.97, "mes": -5.80, "anio": -14.20, "anterior": 862.0, "apertura": 860.0, "minimo": 840.0, "maximo": 864.0, "hora": "17:00", "vol_nominal": 1920000, "vol_efectivo": 1622400000},
    {"especie": "VALO", "ultimo": 438.0, "dia": 0.46, "mes": 2.10, "anio": 44.60, "anterior": 436.0, "apertura": 436.0, "minimo": 434.0, "maximo": 441.0, "hora": "17:00", "vol_nominal": 1350000, "vol_efectivo": 591300000},
    {"especie": "YPFD", "ultimo": 58900.0, "dia": 2.88, "mes": 8.70, "anio": 112.50, "anterior": 57250.0, "apertura": 57500.0, "minimo": 57200.0, "maximo": 59150.0, "hora": "17:00", "vol_nominal": 642000, "vol_efectivo": 37813800000}
]

TICKERS_PANEL_LIDER_SET = {x["especie"] for x in ACCIONES_MERVAL_BASE}
CACHE_ACCIONES_ARGENTINA: Dict[str, Any] = {
    "merval": list(ACCIONES_MERVAL_BASE),
    "lo_mejor": sorted(ACCIONES_MERVAL_BASE, key=lambda x: x["dia"], reverse=True)[:7],
    "lo_peor": sorted(ACCIONES_MERVAL_BASE, key=lambda x: x["dia"])[:7],
    "updated": ""
}

def obtener_acciones_rava_en_vivo() -> Dict[str, Any]:
    global CACHE_ACCIONES_ARGENTINA
    ts_ms = int(time.time() * 1000)
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.rava.com/"
    }
    mapa_merval = {x["especie"]: dict(x) for x in CACHE_ACCIONES_ARGENTINA.get("merval", ACCIONES_MERVAL_BASE)}
    candidatos_ranking = {}

    urls = [
        f"https://www.rava.com/?_t={ts_ms}",
        f"https://www.rava.com/cotizaciones/acciones-argentinas?_t={ts_ms}"
    ]
    for u in urls:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers=hdrs, timeout=4.5)
            if r.status_code == 200 and r.text:
                html_u = r.text.replace("&quot;", '"')
                for m_obj in re.finditer(r'\{[^{}]*"(?:simbolo|especie)"\s*:\s*"([A-Z0-9\.]{2,10})"[^{}]*\}', html_u):
                    b = m_obj.group(0)
                    sim = m_obj.group(1).upper().strip()
                    if sim in ("MERVAL", "IMV", "DOLAR", "RIESGO", "CAUCION"):
                        continue
                    def _ext(campo_regex):
                        m_c = re.search(campo_regex, b, re.IGNORECASE)
                        return _parse_float_limpio(m_c.group(1)) if m_c else None

                    ult = _ext(r'"(?:ultimo|px_last|close|valor)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    dia = _ext(r'"(?:variacion|var_dia|dia|change|porc)"\s*:\s*"?([+\-−]?[\d\.,]+)"?')
                    mes = _ext(r'"(?:var_mes|mes|mensual|var_mtd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?')
                    anio = _ext(r'"(?:var_anio|anio|anual|var_ytd|ytd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?')
                    ant = _ext(r'"(?:anterior|cierre_ant|prev)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    ape = _ext(r'"(?:apertura|open)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    min_v = _ext(r'"(?:minimo|min|low)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    max_v = _ext(r'"(?:maximo|max|high)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    vn = _ext(r'"(?:volumen|vol_nominal|nominal)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    ve = _ext(r'"(?:monto|vol_efectivo|efectivo)"\s*:\s*"?([+\-]?[\d\.,]+)"?')
                    m_h = re.search(r'"hora"\s*:\s*"([^"]+)"', b, re.IGNORECASE)
                    hora_s = m_h.group(1)[:5] if m_h else "17:00"

                    if ult and ult > 0 and dia is not None and -40 <= dia <= 40:
                        item_acc = {
                            "especie": sim,
                            "ultimo": round(ult, 2),
                            "dia": round(dia, 2),
                            "mes": round(mes if mes is not None else (mapa_merval.get(sim, {}).get("mes", 0.0)), 2),
                            "anio": round(anio if anio is not None else (mapa_merval.get(sim, {}).get("anio", 0.0)), 2),
                            "anterior": round(ant if ant else ult / (1 + (dia / 100.0) if dia != -100 else 1), 2),
                            "apertura": round(ape if ape else ult, 2),
                            "minimo": round(min_v if min_v else ult, 2),
                            "maximo": round(max_v if max_v else ult, 2),
                            "hora": hora_s,
                            "vol_nominal": int(vn) if vn else mapa_merval.get(sim, {}).get("vol_nominal", 0),
                            "vol_efectivo": int(ve) if ve else mapa_merval.get(sim, {}).get("vol_efectivo", 0)
                        }
                        if sim in TICKERS_PANEL_LIDER_SET:
                            mapa_merval[sim] = item_acc
                        if len(sim) <= 6 and not sim.endswith(("D", "C")):
                            candidatos_ranking[sim] = item_acc

                plano = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
                for tk in TICKERS_PANEL_LIDER_SET:
                    mt = re.search(r'\b' + tk + r'\b\s+([\d\.]+(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)', plano)
                    if mt:
                        u_v = _parse_float_limpio(mt.group(1))
                        d_v = _parse_float_limpio(mt.group(2))
                        m_v = _parse_float_limpio(mt.group(3))
                        a_v = _parse_float_limpio(mt.group(4))
                        if u_v and d_v is not None:
                            prev = mapa_merval.get(tk, {})
                            prev["ultimo"] = round(u_v, 2)
                            prev["dia"] = round(d_v, 2)
                            if m_v is not None: prev["mes"] = round(m_v, 2)
                            if a_v is not None: prev["anio"] = round(a_v, 2)
                            mapa_merval[tk] = prev
                            candidatos_ranking[tk] = prev
        except Exception:
            pass

    lista_merval = sorted(list(mapa_merval.values()), key=lambda x: x["especie"])
    universo = list(candidatos_ranking.values()) if len(candidatos_ranking) >= 10 else list(lista_merval)
    lo_mejor = sorted([x for x in universo if x["dia"] >= 0], key=lambda x: x["dia"], reverse=True)[:7]
    lo_peor = sorted([x for x in universo if x["dia"] < 0], key=lambda x: x["dia"])[:7]
    if not lo_peor:
        lo_peor = sorted(universo, key=lambda x: x["dia"])[:5]

    CACHE_ACCIONES_ARGENTINA = {
        "merval": lista_merval,
        "lo_mejor": lo_mejor,
        "lo_peor": lo_peor,
        "updated": datetime.utcnow().strftime("%H:%M:%S UTC")
    }
    return CACHE_ACCIONES_ARGENTINA

def _extraer_rp_de_rava_perfil() -> Optional[Dict[str, float]]:
    """Extrae Riesgo Pais en vivo desde https://www.rava.com/perfil/RIESGO%20PAIS."""
    ts_ms = int(time.time() * 1000)
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.9",
        "Cache-Control": "no-cache",
        "Referer": "https://www.rava.com/"
    }
    urls = [
        f"https://www.rava.com/perfil/RIESGO%20PAIS?_t={ts_ms}",
        f"https://api.allorigins.win/raw?url={quote(f'https://www.rava.com/perfil/RIESGO%20PAIS?_t={ts_ms}', safe='')}",
        f"https://corsproxy.io/?{quote(f'https://www.rava.com/perfil/RIESGO%20PAIS?_t={ts_ms}', safe='')}"
    ]
    for u in urls:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers=hdrs, timeout=4.0, verify=False)
            if r.status_code != 200 or len(r.text) < 300:
                continue
            txt = r.text
            m_tit = re.search(
                r"RIESGO\s+PA[IÍ]S\s+Argentina\s+\$?\s*([1-9](?:\.\d{3}|\d{2,3})(?:,\d{1,2})?)\s*\(([+\-−]?\d{1,2}[,\.]\d{1,2})%\)",
                txt,
                re.IGNORECASE
            )
            if m_tit:
                v_rp = _parse_float_limpio(m_tit.group(1))
                v_dia = _parse_float_limpio(m_tit.group(2))
                if v_rp and 100 <= v_rp <= 5000:
                    res: Dict[str, float] = {"ult": v_rp}
                    if v_dia is not None and -45 <= v_dia <= 45:
                        res["dia"] = v_dia
                    plano = BeautifulSoup(txt, "html.parser").get_text(" ", strip=True)
                    m_ma = re.search(
                        r"\b" + re.escape(m_tit.group(1)) + r"\b\s+[+\-−]?\d{1,2},\d{1,2}%?\s+([+\-−]?\d{1,3},\d{1,2})%?\s+([+\-−]?\d{1,3},\d{1,2})%?",
                        plano
                    )
                    if m_ma:
                        vm = _parse_float_limpio(m_ma.group(1))
                        va = _parse_float_limpio(m_ma.group(2))
                        if vm is not None and -90 <= vm <= 300: res["mes"] = vm
                        if va is not None and -95 <= va <= 500: res["anio"] = va
                    return res

            ext = _extraer_rp_y_merval_de_html_rava(txt)
            if "RIESGO_PAIS" in ext and ext["RIESGO_PAIS"].get("ult"):
                return ext["RIESGO_PAIS"]
        except Exception:
            pass
    return None

def _extraer_rp_de_bondterminal() -> Optional[Dict[str, float]]:
    """Consulta BondTerminal (https://bondterminal.com/riesgo-pais) como unico respaldo."""
    ts_ms = int(time.time() * 1000)
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Referer": "https://bondterminal.com/riesgo-pais"
    }
    candidatos = [
        f"https://bondterminal.com/riesgo-pais?_t={ts_ms}",
        f"https://bondterminal.com/api/riesgo-pais/scenario?_t={ts_ms}",
        f"https://bondterminal.com/api/riesgo-pais?_t={ts_ms}",
        f"https://api.allorigins.win/raw?url={quote(f'https://bondterminal.com/riesgo-pais?_t={ts_ms}', safe='')}"
    ]
    for u in candidatos:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers=hdrs, timeout=3.5, verify=False)
            if r.status_code != 200 or not r.text.strip():
                continue
            txt = r.text
            for pat_json in [
                r'"(?:officialValue|btLiveValue|weightedSpreadBps|valueBps|riesgo[_\s]?pais|embi)"\s*:\s*"?([1-9]\d{2,3}(?:[\.,]\d+)?)"?'
            ]:
                m_j = re.search(pat_json, txt, re.IGNORECASE)
                if m_j:
                    v_rp = _parse_float_limpio(m_j.group(1))
                    if v_rp and 100 <= v_rp <= 5000:
                        return {"ult": v_rp}

            plano = BeautifulSoup(txt, "html.parser").get_text(" ", strip=True)
            m_txt = re.search(
                r"(?:RIESGO\s+PA[IÍ]S|EMBI\+?)\D{0,35}([1-9](?:\.\d{3}|\d{2,3})(?:,\d{1,2})?)\s*(?:bps|pb|pts)?",
                plano,
                re.IGNORECASE
            )
            if m_txt:
                v_rp = _parse_float_limpio(m_txt.group(1))
                if v_rp and 100 <= v_rp <= 5000:
                    return {"ult": v_rp}
        except Exception:
            pass
    return None

def _extraer_rp_y_merval_de_html_rava(raw_html: str) -> Dict[str, Dict[str, float]]:
    encontrados: Dict[str, Dict[str, float]] = {}
    if not raw_html or len(raw_html) < 200:
        return encontrados
    html_u = (
        raw_html.replace("&quot;", '"')
        .replace("&#34;", '"')
        .replace("&apos;", "'")
        .replace("\\/", "/")
    )

    m_tit = re.search(
        r"RIESGO\s+PA[IÍ]S\s+Argentina\s+\$?\s*([1-9](?:\.\d{3}|\d{2,3})(?:,\d{1,2})?)\s*\(([+\-−]?\d{1,2}[,\.]\d{1,2})%\)",
        html_u,
        re.IGNORECASE
    )
    if m_tit:
        vu = _parse_float_limpio(m_tit.group(1))
        vd = _parse_float_limpio(m_tit.group(2))
        if vu and 100 <= vu <= 5000:
            encontrados["RIESGO_PAIS"] = {"ult": vu}
            if vd is not None:
                encontrados["RIESGO_PAIS"]["dia"] = vd

    patrones_busqueda = [
        ("RIESGO_PAIS", r'"(?:simbolo|especie|nombre|titulo)"\s*:\s*"RIESGO\s*PA[IÍ]S"', 100, 5000),
        ("MERVAL_USD", r'"(?:simbolo|especie|nombre|titulo)"\s*:\s*"MERVAL\s*(?:USD|CCL)"', 10, 5000),
        ("MERVAL_ARS", r'"(?:simbolo|especie|nombre|titulo)"\s*:\s*"(?:MERVAL|S&P\s*MERVAL|IMV)"', 20000, 10000000),
    ]
    for clave, pat_sim, min_v, max_v in patrones_busqueda:
        if clave in encontrados:
            continue
        for m in re.finditer(pat_sim, html_u, re.IGNORECASE):
            ini = max(0, m.start() - 350)
            fin = min(len(html_u), m.end() + 650)
            ventana = html_u[ini:fin]
            m_u = re.search(r'"(?:ultimo|px_last|close|valor|cierre)"\s*:\s*"?([+\-]?[\d\.,]+)"?', ventana, re.IGNORECASE)
            if not m_u:
                continue
            vu = _parse_float_limpio(m_u.group(1))
            if vu is None or not (min_v <= vu <= max_v):
                continue
            m_d = re.search(r'"(?:variacion|var_dia|dia|change|porc)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', ventana, re.IGNORECASE)
            m_m = re.search(r'"(?:var_mes|mes|mensual|var_mtd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', ventana, re.IGNORECASE)
            m_a = re.search(r'"(?:var_anio|anio|anual|var_ytd|ytd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', ventana, re.IGNORECASE)
            res_k: Dict[str, float] = {"ult": vu}
            vd = _parse_float_limpio(m_d.group(1)) if m_d else None
            vm = _parse_float_limpio(m_m.group(1)) if m_m else None
            va = _parse_float_limpio(m_a.group(1)) if m_a else None
            if vd is not None and -45.0 <= vd <= 45.0: res_k["dia"] = vd
            if vm is not None and -85.0 <= vm <= 250.0: res_k["mes"] = vm
            if va is not None and -95.0 <= va <= 500.0: res_k["anio"] = va
            encontrados[clave] = res_k
            break

    soup = BeautifulSoup(raw_html, "html.parser")
    for tr in soup.find_all("tr"):
        celdas = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(celdas) < 2:
            continue
        esp = celdas[0].upper().strip()
        clave_tr = None
        min_v, max_v = 0.0, 0.0
        if "RIESGO PAIS" in esp or "RIESGO PAÍS" in esp:
            clave_tr, min_v, max_v = "RIESGO_PAIS", 100, 5000
        elif "MERVAL" in esp and ("USD" in esp or "CCL" in esp):
            clave_tr, min_v, max_v = "MERVAL_USD", 10, 5000
        elif esp in ("MERVAL", "S&P MERVAL", "IMV"):
            clave_tr, min_v, max_v = "MERVAL_ARS", 20000, 10000000

        if clave_tr and clave_tr not in encontrados:
            u_v = _parse_float_limpio(celdas[1])
            if u_v and min_v <= u_v <= max_v:
                r_tab: Dict[str, float] = {"ult": u_v}
                if len(celdas) > 2 and _parse_float_limpio(celdas[2]) is not None:
                    r_tab["dia"] = float(_parse_float_limpio(celdas[2]))
                if len(celdas) > 3 and _parse_float_limpio(celdas[3]) is not None:
                    r_tab["mes"] = float(_parse_float_limpio(celdas[3]))
                if len(celdas) > 4 and _parse_float_limpio(celdas[4]) is not None:
                    r_tab["anio"] = float(_parse_float_limpio(celdas[4]))
                encontrados[clave_tr] = r_tab
    return encontrados

def obtener_msci_merval_en_vivo(ccl_ref: float = 1749.47) -> Dict[str, Dict[str, Any]]:
    prev_rp = next((x for x in CACHE_VARIABLES.get("indices", []) if x.get("variable") == "RIESGO_PAIS"), {})
    prev_m_ars = next((x for x in CACHE_VARIABLES.get("indices", []) if x.get("variable") == "MERVAL_ARS"), {})
    prev_m_usd = next((x for x in CACHE_VARIABLES.get("indices", []) if x.get("variable") == "MERVAL_USD"), {})

    datos = {
        "MERVAL_ARS": {
            "ult": float(prev_m_ars.get("valor") or 81255.0),
            "dia": _parse_float_limpio(prev_m_ars.get("variacion")) or 0.0,
            "mes": _parse_float_limpio(prev_m_ars.get("var_mes")) or 0.0,
            "anio": _parse_float_limpio(prev_m_ars.get("var_anio")) or 0.0
        },
        "MERVAL_USD": {
            "ult": float(prev_m_usd.get("valor") or 46.0),
            "dia": _parse_float_limpio(prev_m_usd.get("variacion")) or 0.0,
            "mes": _parse_float_limpio(prev_m_usd.get("var_mes")) or 0.0,
            "anio": _parse_float_limpio(prev_m_usd.get("var_anio")) or 0.0
        },
        "RIESGO_PAIS": {
            "ult": float(prev_rp.get("valor") or 1106.0),
            "dia": _parse_float_limpio(prev_rp.get("variacion")) or 6.9,
            "mes": _parse_float_limpio(prev_rp.get("var_mes")) or 0.0,
            "anio": _parse_float_limpio(prev_rp.get("var_anio")) or 0.0
        }
    }
    rp_vivo_ok = False
    rp_fuente = "CACHE"
    merval_ars_ok = False
    merval_usd_ok = False

    rava_pf = _extraer_rp_de_rava_perfil()
    if rava_pf and rava_pf.get("ult"):
        datos["RIESGO_PAIS"].update(rava_pf)
        rp_vivo_ok = True
        rp_fuente = "RAVA_PERFIL"

    html_comp = obtener_html_rava_compartido(ttl=4.0)
    if html_comp:
        ext_c = _extraer_rp_y_merval_de_html_rava(html_comp)
        for k_e, v_e in ext_c.items():
            if k_e == "RIESGO_PAIS" and rp_vivo_ok:
                continue
            datos[k_e].update(v_e)
            if k_e == "RIESGO_PAIS":
                rp_vivo_ok = True
                rp_fuente = "RAVA_HOME"
            elif k_e == "MERVAL_ARS":
                merval_ars_ok = True
            elif k_e == "MERVAL_USD":
                merval_usd_ok = True

    if merval_ars_ok and not merval_usd_ok:
        ccl_rava = (_DOLARITO_CICLO_CACHE.get("data") or {}).get("DOLAR_CCL") or BASE_DOLARITO_EXACTA.get("DOLAR_CCL") or {}
        ccl_px = float(ccl_rava.get("venta") or ccl_ref or 1749.47)
        if ccl_px > 800:
            datos["MERVAL_USD"]["ult"] = round(datos["MERVAL_ARS"]["ult"] / ccl_px, 2)
            ccl_dia = _parse_float_limpio(ccl_rava.get("variacion")) or 0.0
            datos["MERVAL_USD"]["dia"] = round(((1 + datos["MERVAL_ARS"]["dia"] / 100.0) / (1 + ccl_dia / 100.0) - 1) * 100.0, 2)

    if not rp_vivo_ok:
        bt_rp = _extraer_rp_de_bondterminal()
        if bt_rp and bt_rp.get("ult"):
            datos["RIESGO_PAIS"].update(bt_rp)
            rp_vivo_ok = True
            rp_fuente = "BONDTERMINAL"

    res_out = {}
    config_items = [
        ("MERVAL_ARS", "Merval", f"{int(round(datos['MERVAL_ARS']['ult'])):,} pts".replace(",", ".")),
        ("MERVAL_USD", "Merval USD", f"{int(round(datos['MERVAL_USD']['ult'])):,} pts".replace(",", ".")),
        ("RIESGO_PAIS", "Riesgo País EMBI+", f"{int(round(datos['RIESGO_PAIS']['ult'])):,} pb".replace(",", "."))
    ]
    for cod, desc, fmt_val in config_items:
        d_info = datos[cod]
        var_dia = _fmt_pct_con_signo(d_info["dia"])
        var_mes = _fmt_pct_con_signo(d_info["mes"])
        var_anio = _fmt_pct_con_signo(d_info["anio"])
        res_out[cod] = {
            "variable": cod,
            "descripcion": desc,
            "categoria": "indices",
            "subcategoria": "argentina",
            "valor_formateado": fmt_val,
            "variacion": var_dia,
            "var_mes": var_mes,
            "var_anio": var_anio,
            "valor": round(float(d_info["ult"]), 2),
            "fecha": f"Mes: {var_mes} | Año: {var_anio}",
            "_live": rp_vivo_ok if cod == "RIESGO_PAIS" else True,
            "_fuente": rp_fuente if cod == "RIESGO_PAIS" else "RAVA"
        }
    return res_out

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
                "chg_pct": chg,
                "fecha": fec_hoy
            }
        return None

    with ThreadPoolExecutor(max_workers=6) as ex:
        res = list(ex.map(worker_ticker, US_TICKERS_DEF))
    return [r for r in res if r is not None]

METALES_TICKERS_TOTAL = [
    ("GC=F", "gc.f", "METALES_XAU", "ORO (XAU)", "US$/ozt", 4070.10),
    ("SI=F", "si.f", "METALES_XAG", "PLATA (XAG)", "US$/oz", 51.95),
    ("HG=F", "hg.f", "METALES_HG00", "COBRE (HG00)", "US$/lb", 6.24),
    ("PL=F", "pl.f", "METALES_XPT", "PLATINO (XPT)", "US$/oz", 1534.00),
    ("PA=F", "pa.f", "METALES_XPD", "PALADIO (XPD)", "US$/oz", 1004.00),
    ("ALI=F", "al.f", "METALES_ALU", "ALUMINIO (ALU)", "US$/ozt", 10.49),
    ("ZN=F", "zn.f", "METALES_ZNC", "ZINC (ZNC)", "US$/ozt", 8.55),
    ("NI=F", "ni.f", "METALES_NI", "NÍQUEL (NI)", "US$/oz", 2.00),
    ("TIN=F", "sn.f", "METALES_TIN", "ESTAÑO (TIN)", "US$/oz", 1.45),
    ("LED=F", "pb.f", "METALES_LEAD", "PLOMO (LEAD)", "US$/oz", 17.31),
    ("TIO=F", "tio.f", "METALES_IRON", "MINERAL DE HIERRO (IRON)", "US$/oz", 380.52),
    ("URA", "ura.us", "METALES_U", "URANIO (U)", "US$/lb", 91.85),
    ("LIT", "lit.us", "METALES_LI", "LITIO (LI)", "US$/oz", 2.29),
    ("LIT", "lit.us", "METALES_LITH_CAR", "CARBONATO DE LITIO (LITH-CAR)", "US$/ozt", 2.29),
    ("MO=F", "mo.f", "METALES_MO", "MOLIBDENO (MO)", "US$/ozt", 2.81),
    ("XRH=X", "xrh.x", "METALES_XRH", "RODIO (XRH)", "US$/oz", 9200.00),
    ("VAN=F", "van.f", "METALES_VAN", "VANADIO (VAN)", "US$/ozt", 3.17),
    ("COBALT", "cobalt.us", "METALES_LCO", "COBALTO (LCO)", "US$/oz", 1.04)
]

def obtener_datos_metales_en_vivo() -> List[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    def fetch_metal_worker(info):
        sym_y, sym_s, cod, desc, unidad, def_p = info
        q = fetch_quote_externa(sym_y, sym_s)
        if q:
            p = q["price"]
            chg = q["change_pct"]
            var_str = f"+{chg:.2f}%" if chg > 0 else f"{chg:.2f}%"
            fmt = f"{p:,.2f} {unidad}".replace(",", "X").replace(".", ",").replace("X", ".") if p >= 1000 else f"{p:.2f} {unidad}".replace(".", ",")
            return {"variable": cod, "descripcion": desc, "categoria": "metales", "valor_formateado": fmt, "variacion": var_str, "valor": p, "fecha": fec_hoy}
        else:
            fmt = f"{def_p:,.2f} {unidad}".replace(",", "X").replace(".", ",").replace("X", ".") if def_p >= 1000 else f"{def_p:.2f} {unidad}".replace(".", ",")
            return {"variable": cod, "descripcion": desc, "categoria": "metales", "valor_formateado": fmt, "variacion": "0.00%", "valor": def_p, "fecha": fec_hoy}

    with ThreadPoolExecutor(max_workers=8) as ex:
        resultados = list(ex.map(fetch_metal_worker, METALES_TICKERS_TOTAL))
    return [r for r in resultados if r]

PETROLEO_TICKERS_TOTAL = [
    ("CL=F", "cl.f", "PETROLEO_WTI", "Petróleo Crudo WTI", "USD/bbl", 71.24),
    ("BZ=F", "cb.f", "PETROLEO_BRENT", "Petróleo Crudo Brent", "USD/bbl", 74.88),
    ("BZ=F", "cb.f", "PETROLEO_MURBAN", "Petróleo Crudo Murban", "USD/bbl", 75.12),
    ("NG=F", "ng.f", "ENERGIA_GAS_NATURAL", "Gas Natural (Henry Hub)", "USD/MMBtu", 2.85),
    ("RB=F", "rb.f", "ENERGIA_GASOLINA_FUT", "Gasolina RBOB", "USD/gal", 2.05),
    ("HO=F", "ho.f", "ENERGIA_HEATING_OIL", "Aceite de Calefacción", "USD/gal", 2.26)
]

AGRO_GRANOS_BASE = [
    ("AGRO_SOJA", "Soja (Pizarra Rosario)", 550000.0, 354.72, False, "="),
    ("AGRO_MAIZ", "Maíz (Pizarra Rosario)", 271000.0, 174.78, False, "+0,00%"),
    ("AGRO_TRIGO", "Trigo (Pizarra Rosario)", 286850.0, 185.00, False, "+0,00%"),
    ("AGRO_GIRASOL", "Girasol (Pizarra Rosario)", 744240.0, 480.00, True, "+0,00%"),
    ("AGRO_SORGO", "Sorgo (Pizarra Rosario)", 279090.0, 180.00, True, "+0,00%"),
]

_BCR_CACHE = {"ts": 0.0, "datos": {}}
_BCR_LOCK = threading.Lock()

def _parsear_tarjetas_bcr(html_txt: str) -> Dict[str, Dict[str, Any]]:
    res: Dict[str, Dict[str, Any]] = {}
    if not html_txt or len(html_txt) < 300:
        return res
    soup = BeautifulSoup(html_txt, "html.parser")
    plano = soup.get_text(" ", strip=True)
    m_fecha = re.search(r"Precios\s+Pizarra\s+del\s+d[ií]a\s+(\d{2}/\d{2}/20\d{2})", plano, re.IGNORECASE)
    fecha_piz = m_fecha.group(1) if m_fecha else ""

    granos_def = [
        ("AGRO_TRIGO", r"TRIGO"),
        ("AGRO_MAIZ", r"MA[IÍ]Z"),
        ("AGRO_GIRASOL", r"GIRASOL"),
        ("AGRO_SOJA", r"SOJA"),
        ("AGRO_SORGO", r"SORGO"),
    ]
    posiciones = []
    for cod, pat in granos_def:
        m = re.search(r"" + pat + r"", plano)
        if m:
            posiciones.append((m.start(), cod))
    posiciones.sort(key=lambda x: x[0])

    for i, (start_idx, cod) in enumerate(posiciones):
        end_idx = posiciones[i + 1][0] if i + 1 < len(posiciones) else min(len(plano), start_idx + 220)
        bloque = plano[start_idx:end_idx]

        # Ignorar subproductos si aparecieran más abajo
        if any(w in bloque.lower() for w in ["aceite", "harina", "pellet"]):
            continue

        es_estimativo = bool(re.search(r"S/C|\(E\)", bloque, re.IGNORECASE))
        m_ars = re.search(r"\$\s*([1-9]\d{2}\.\d{3}(?:,\d{1,2})?)", bloque)
        m_usd = re.search(r"US\$\s*(?:\(E\)\s*)?(\d{2,4}(?:,\d{1,2})?)", bloque, re.IGNORECASE)

        if m_ars:
            p_ars = normalizar_valor(m_ars.group(1))
            p_usd = normalizar_valor(m_usd.group(1)) if m_usd else None
            if p_ars and 80000 <= p_ars <= 2500000:
                res[cod] = {
                    "precio_ars": p_ars,
                    "precio_usd": p_usd,
                    "estimativo": es_estimativo,
                    "fecha_pizarra": fecha_piz,
                }
    return res

def _obtener_cache_bcr_no_bloqueante() -> Dict[str, Dict[str, Any]]:
    now = time.time()
    if now - _BCR_CACHE["ts"] < 60.0 and _BCR_CACHE["datos"]:
        return _BCR_CACHE["datos"]
    if not _BCR_LOCK.acquire(blocking=False):
        return _BCR_CACHE["datos"]
    try:
        r = GLOBAL_HTTP_SESSION.get(
            f"https://www.cac.bcr.com.ar/es/precios-de-pizarra?_t={int(now)}",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=3.0,
            verify=False,
        )
        if r.status_code == 200 and r.text:
            parsed = _parsear_tarjetas_bcr(r.text)
            if len(parsed) >= 3:
                _BCR_CACHE["datos"] = parsed
        _BCR_CACHE["ts"] = time.time()
    except Exception:
        _BCR_CACHE["ts"] = time.time()
    finally:
        _BCR_LOCK.release()
    return _BCR_CACHE["datos"]

def obtener_agro_en_vivo(tc_mayorista: float = 1545.50) -> Dict[str, Dict[str, Any]]:
    tc = tc_mayorista if tc_mayorista and tc_mayorista > 800 else 1550.0
    bcr_live = _obtener_cache_bcr_no_bloqueante()
    salida: Dict[str, Dict[str, Any]] = {}

    for cod, desc, def_ars, def_usd, def_est, def_var in AGRO_GRANOS_BASE:
        vivo = bcr_live.get(cod)
        if vivo and vivo.get("precio_ars"):
            p_ars = round(float(vivo["precio_ars"]), 2)
            p_usd = round(float(vivo["precio_usd"]), 2) if vivo.get("precio_usd") else round(p_ars / tc, 2)
            es_est = bool(vivo.get("estimativo"))
            f_piz = vivo.get("fecha_pizarra") or "Pizarra Rosario"
        else:
            p_ars = def_ars
            p_usd = def_usd
            es_est = def_est
            f_piz = "Pizarra Rosario"

        suf_est = " (E)" if es_est else ""
        fmt_ars = f"$ {p_ars:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + suf_est
        fmt_usd = f"US$ {p_usd:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + suf_est

        salida[cod] = {
            "variable": cod,
            "descripcion": desc,
            "categoria": "commodities_agro_energia",
            "precio_ars": fmt_ars,
            "precio_usd": fmt_usd,
            "valor_ars": p_ars,
            "valor_usd": p_usd,
            "valor_formateado": f"{fmt_ars} / {fmt_usd}",
            "variacion": def_var,
            "valor": p_usd,
            "fecha": f"BCR {f_piz}" if f_piz != "Pizarra Rosario" else "Pizarra Rosario",
        }
    return salida

def obtener_datos_petroleo_en_vivo() -> List[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    def fetch_petroleo_worker(info):
        sym_y, sym_s, cod, desc, unidad, def_p = info
        q = fetch_quote_externa(sym_y, sym_s)
        if q:
            p = q["price"]
            if cod == "PETROLEO_MURBAN":
                p = round(p + 0.35, 2)
            chg = q["change_pct"]
            var_str = f"+{chg:.2f}%" if chg > 0 else f"{chg:.2f}%"
            fmt = f"{p:.2f} {unidad}".replace(".", ",")
            return {"variable": cod, "descripcion": desc, "categoria": "petroleo", "valor_formateado": fmt, "variacion": var_str, "valor": p, "fecha": fec_hoy}
        else:
            fmt = f"{def_p:.2f} {unidad}".replace(".", ",")
            return {"variable": cod, "descripcion": desc, "categoria": "petroleo", "valor_formateado": fmt, "variacion": "0.00%", "valor": def_p, "fecha": fec_hoy}

    with ThreadPoolExecutor(max_workers=6) as ex:
        resultados = list(ex.map(fetch_petroleo_worker, PETROLEO_TICKERS_TOTAL))
    return [r for r in resultados if r]

def obtener_datos_indicadores() -> List[Dict[str, Any]]:
    items = []
    try:
        r = GLOBAL_HTTP_SESSION.get("https://indicadores.ar/indicadores-economicos", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            cargadas = set()
            for tr in soup.find_all("tr"):
                c = tr.find_all(["td", "th"])
                if len(c) >= 4:
                    nom = c[0].get_text(strip=True).lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
                    for k_map, v_map in MAPEO.items():
                        k_clean = k_map.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
                        cod, desc, cat = v_map
                        k_base = re.sub(r"\s*\([^)]*\)", "", k_clean).strip()
                        coincide = (k_clean == nom) or (
                            cat in ("macro_y_fiscal", "micro_y_sectores") and len(k_base) >= 5 and k_base in nom
                        ) or (
                            cod in ("TASA_BADLAR", "TASA_TAMAR", "TASA_PLAZO_FIJO", "TASA_CALL")
                            and k_clean.replace("tasa ", "").replace("referencia ", "").strip("()") in nom
                        )
                        if coincide and cod not in cargadas:
                            v_num = normalizar_valor(c[1].get_text(strip=True))
                            if v_num is not None:
                                fecha_txt = c[3].get_text(strip=True)
                                mes_corte = extraer_mes_corte_legible(fecha_txt)
                                desc_final = desc
                                if cod == "INFLACION_MENSUAL" and mes_corte:
                                    desc_final = f"Inflación Mensual ({mes_corte})"
                                elif cod == "INFLACION_INTERANUAL" and mes_corte:
                                    desc_final = f"Inflación Interanual ({mes_corte})"

                                val_fmt_raw = c[1].get_text(strip=True)
                                if cod.startswith("TASA_") and "TNA" not in val_fmt_raw.upper():
                                    val_fmt_raw = f"{val_fmt_raw} TNA"

                                item_dict = {
                                    "variable": cod,
                                    "descripcion": desc_final,
                                    "categoria": cat,
                                    "valor_formateado": val_fmt_raw,
                                    "variacion": c[2].get_text(strip=True),
                                    "valor": v_num,
                                    "fecha": f"Corte: {mes_corte}" if mes_corte and cod in ("INFLACION_MENSUAL", "INFLACION_INTERANUAL") else fecha_txt
                                }
                                if cat == "indices":
                                    item_dict["subcategoria"] = "argentina"
                                items.append(item_dict)
                                if cod in CACHE_TASAS_PERSISTENTE and cod != "TASA_PLAZO_FIJO":
                                    CACHE_TASAS_PERSISTENTE[cod] = dict(item_dict)
                                cargadas.add(cod)
    except Exception:
        pass
    return items

def obtener_inflacion_oficial_bcra() -> Dict[str, Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/html, */*"}
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
        desc_low = desc_raw.lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        v_num = normalizar_valor(val_raw)
        if v_num is None or v_num <= 0 or v_num > 500:
            return
        mes_corte = extraer_mes_corte_legible(str(fec_raw or desc_raw))
        fmt_pct = f"{v_num:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")
        if id_var == "29" or ("inflacion" in desc_low and ("esperada" in desc_low or "rem" in desc_low)):
            encontrados["INFLACION_REM"] = {
                "variable": "INFLACION_REM",
                "descripcion": f"Inflación Esperada REM 12m ({mes_corte})" if mes_corte else "Inflación Esperada REM (12m)",
                "categoria": "inflacion_y_ajuste", "valor_formateado": fmt_pct, "variacion": "REM BCRA", "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "REM BCRA"
            }
        elif id_var == "27" or ("inflacion mensual" in desc_low and "esperada" not in desc_low and "rem" not in desc_low):
            encontrados["INFLACION_MENSUAL"] = {
                "variable": "INFLACION_MENSUAL",
                "descripcion": f"Inflación Mensual ({mes_corte})" if mes_corte else "Inflación Mensual",
                "categoria": "inflacion_y_ajuste", "valor_formateado": fmt_pct, "variacion": fmt_pct, "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "BCRA / INDEC"
            }
        elif id_var == "28" or ("inflacion interanual" in desc_low or ("inflacion" in desc_low and "i.a." in desc_low)):
            encontrados["INFLACION_INTERANUAL"] = {
                "variable": "INFLACION_INTERANUAL",
                "descripcion": f"Inflación Interanual ({mes_corte})" if mes_corte else "Inflación Interanual",
                "categoria": "inflacion_y_ajuste", "valor_formateado": fmt_pct, "variacion": fmt_pct, "valor": v_num,
                "fecha": f"Corte: {mes_corte}" if mes_corte else "BCRA / INDEC"
            }

    for url in urls:
        try:
            r = GLOBAL_HTTP_SESSION.get(url, headers=headers, timeout=2.5, verify=False)
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
                    if desc_v or id_v in ("27", "28", "29"):
                        clasificar(desc_v, val_v, fec_v, id_v)
            except Exception:
                pass
            if len(encontrados) >= 3:
                break
        except Exception:
            pass

    if "INFLACION_REM" not in encontrados:
        encontrados["INFLACION_REM"] = {
            "variable": "INFLACION_REM", "descripcion": "Inflación Esperada REM (12m)",
            "categoria": "inflacion_y_ajuste", "valor_formateado": "21,8%", "variacion": "REM BCRA", "valor": 21.8, "fecha": "BCRA Oficial"
        }
    return encontrados

def _actualizar_cauciones_en_vivo():
    base_c = {
        "1D": {"desc": "Caución Bursátil 1 Día (Pesos)", "tasa": 14.10, "dia": None, "mes": -21.7, "anio": -88.3},
        "7D": {"desc": "Caución Bursátil 7 Días (Pesos)", "tasa": 15.10, "dia": None, "mes": -22.2, "anio": -31.4},
        "14D": {"desc": "Caución Bursátil 14 Días (Pesos)", "tasa": 17.10, "dia": None, "mes": -10.9, "anio": -22.3}
    }
    ts_ms = int(time.time() * 1000)
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.rava.com/"
    }

    htmls_rava = [obtener_html_rava_compartido(ttl=3.5)]
    for u_rava in [
        f"https://www.rava.com/cotizaciones/cauciones?_t={ts_ms}",
        f"https://www.rava.com/perfil/CAUCION%201D?_t={ts_ms}",
        f"https://www.rava.com/perfil/CAUCION%207D?_t={ts_ms}",
        f"https://www.rava.com/perfil/CAUCION%2014D?_t={ts_ms}"
    ]:
        try:
            r_rc = GLOBAL_HTTP_SESSION.get(u_rava, headers=hdrs, timeout=3.2)
            if r_rc.status_code == 200 and len(r_rc.text) > 400:
                htmls_rava.append(r_rc.text)
        except Exception:
            pass

    for raw_rava_html in htmls_rava:
        if not raw_rava_html:
            continue
        try:
            html_u = raw_rava_html.replace("&quot;", '"').replace("\\/", "/")
            # 1) Buscar objetos JSON que contengan CAUCION 1D / 7D / 14D en cualquier orden de claves
            for m_obj in re.finditer(r'\{[^{}]*CAUCI[OÓ]N[^\{\}]*\}', html_u, re.IGNORECASE):
                b = m_obj.group(0)
                m_nom = re.search(r'"(?:simbolo|especie|nombre|titulo)"\s*:\s*"([^"]*CAUCI[OÓ]N[^"]*)"', b, re.IGNORECASE)
                if not m_nom:
                    continue
                nom_c = m_nom.group(1).upper()
                if "USD" in nom_c or "DOLAR" in nom_c:
                    continue
                plz = "14D" if "14" in nom_c else ("7D" if "7" in nom_c else ("1D" if "1" in nom_c else None))
                if not plz:
                    continue
                m_u = re.search(r'"(?:ultimo|tasa|valor|close|px_last)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_d = re.search(r'"(?:variacion|var_dia|dia|change|porc)"\s*:\s*"?([+\-−]?[\d\.,]+|-)"?', b, re.IGNORECASE)
                m_m = re.search(r'"(?:var_mes|mes|mensual|var_mtd)"\s*:\s*"?([+\-−]?[\d\.,]+|-)"?', b, re.IGNORECASE)
                m_a = re.search(r'"(?:var_anio|anio|anual|var_ytd|ytd)"\s*:\s*"?([+\-−]?[\d\.,]+|-)"?', b, re.IGNORECASE)

                vu = normalizar_valor(m_u.group(1)) if m_u else None
                if vu is not None and 0.5 <= vu <= 150.0:
                    base_c[plz]["tasa"] = vu
                if m_d:
                    base_c[plz]["dia"] = None if m_d.group(1).strip() == "-" else normalizar_valor(m_d.group(1))
                if m_m and m_m.group(1).strip() != "-":
                    vm = normalizar_valor(m_m.group(1))
                    if vm is not None: base_c[plz]["mes"] = vm
                if m_a and m_a.group(1).strip() != "-":
                    va = normalizar_valor(m_a.group(1))
                    if va is not None: base_c[plz]["anio"] = va

            # 2) Buscar tabla HTML renderizada: CAUCION 1D | 14,10 | - | -21,7 | -88,3
            plano = BeautifulSoup(raw_rava_html, "html.parser").get_text(" ", strip=True)
            for plz in ("1D", "7D", "14D"):
                mt = re.search(
                    r'\bCAUCI[OÓ]N\s+' + plz + r'\b\s+([\d]+[\.,][\d]+)\s+(-|[+\-−]?[\d]+[\.,][\d]+)\s+(-|[+\-−]?[\d]+[\.,][\d]+)\s+(-|[+\-−]?[\d]+[\.,][\d]+)',
                    plano, re.IGNORECASE
                )
                if mt:
                    u_v = normalizar_valor(mt.group(1))
                    if u_v is not None and 0.5 <= u_v <= 150.0:
                        base_c[plz]["tasa"] = u_v
                    d_raw = mt.group(2).strip()
                    base_c[plz]["dia"] = None if d_raw == "-" else normalizar_valor(d_raw)
                    if mt.group(3).strip() != "-":
                        m_v = normalizar_valor(mt.group(3))
                        if m_v is not None: base_c[plz]["mes"] = m_v
                    if mt.group(4).strip() != "-":
                        a_v = normalizar_valor(mt.group(4))
                        if a_v is not None: base_c[plz]["anio"] = a_v
        except Exception:
            pass

    def _f_pct(v):
        if v is None:
            return "-"
        return f"+{v:.1f}%".replace(".", ",") if v > 0 else f"{v:.1f}%".replace(".", ",")

    CACHE_TASAS_PERSISTENTE.pop("TASA_CAUCIONES", None)

    for plz in ("1D", "7D", "14D"):
        d_p = base_c[plz]
        cod_var = f"TASA_CAUCION_{plz}"
        t_val = d_p["tasa"]
        var_d = _f_pct(d_p["dia"])
        var_m = _f_pct(d_p["mes"])
        var_a = _f_pct(d_p["anio"])
        CACHE_TASAS_PERSISTENTE[cod_var] = {
            "variable": cod_var,
            "descripcion": d_p["desc"],
            "categoria": "tasas_y_monetario",
            "valor_formateado": f"{t_val:.2f}% TNA".replace(".", ","),
            "variacion": var_d,
            "var_mes": var_m,
            "var_anio": var_a,
            "valor": round(float(t_val), 2),
            "fecha": f"Mes: {var_m} | Año: {var_a}"
        }

_BCRA_PF_CACHE = {"ts": 0.0, "data": None}
_BCRA_PF_LOCK = threading.Lock()

def obtener_promedio_plazo_fijo_bcra() -> Dict[str, Any]:
    """Consulta 1 vez por día (TTL 24h) https://www.bcra.gob.ar/plazos-fijos-online/ y promedia la TNA a 30 días de todos los bancos."""
    now = time.time()
    if now - _BCRA_PF_CACHE["ts"] < 86400.0 and _BCRA_PF_CACHE["data"]:
        return _BCRA_PF_CACHE["data"]

    if not _BCRA_PF_LOCK.acquire(blocking=False):
        return _BCRA_PF_CACHE["data"] or CACHE_TASAS_PERSISTENTE["TASA_PLAZO_FIJO"]

    try:
        r = GLOBAL_HTTP_SESSION.get(
            "https://www.bcra.gob.ar/plazos-fijos-online/",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=4.5,
            verify=False,
        )
        if r.status_code == 200 and r.text:
            soup = BeautifulSoup(r.text, "html.parser")
            tasas_bancos = []
            for tr in soup.find_all("tr"):
                celdas = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(celdas) < 2:
                    continue
                entidad = celdas[0].strip()
                if not entidad or "entidad" in entidad.lower() or "diez bancos" in entidad.lower() or "otros bancos" in entidad.lower():
                    continue
                # Columna Clientes (índice 1) y No Clientes (índice 2)
                t_cli = normalizar_valor(celdas[1]) if len(celdas) > 1 else None
                t_nocli = normalizar_valor(celdas[2]) if len(celdas) > 2 else None
                t_valida = t_cli if (t_cli is not None and 1.0 <= t_cli <= 200.0) else (
                    t_nocli if (t_nocli is not None and 1.0 <= t_nocli <= 200.0) else None
                )
                if t_valida is not None:
                    tasas_bancos.append(t_valida)

            if len(tasas_bancos) >= 10:
                prom = round(sum(tasas_bancos) / len(tasas_bancos), 2)
                fec_str = datetime.utcnow().strftime("%d %b %Y")
                obj_pf = {
                    "variable": "TASA_PLAZO_FIJO",
                    "descripcion": "Tasa Plazo Fijo 30d (Prom. BCRA)",
                    "categoria": "tasas_y_monetario",
                    "valor_formateado": f"{prom:.2f}% TNA".replace(".", ","),
                    "variacion": "0,0%",
                    "valor": prom,
                    "fecha": f"BCRA {fec_str} ({len(tasas_bancos)} bancos)",
                }
                _BCRA_PF_CACHE["data"] = obj_pf
                CACHE_TASAS_PERSISTENTE["TASA_PLAZO_FIJO"] = obj_pf
        _BCRA_PF_CACHE["ts"] = time.time()
    except Exception:
        # Si falla puntualmente, reintenta en 15 minutos sin bloquear
        _BCRA_PF_CACHE["ts"] = time.time() - 85500.0
    finally:
        _BCRA_PF_LOCK.release()

    return _BCRA_PF_CACHE["data"] or CACHE_TASAS_PERSISTENTE["TASA_PLAZO_FIJO"]

def obtener_datos_tasas_sjb() -> List[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = GLOBAL_HTTP_SESSION.get("https://sjbdashboards.up.railway.app/api/tasas/tablas", headers={"User-Agent": "Mozilla/5.0"}, timeout=4.5)
        if r.status_code == 200:
            d = r.json()
            for row in (d.get("principales") or []) + (d.get("otras") or []):
                nom_t = str(row.get("Tasa") or row.get("tasa") or "").lower()
                tna_val = str(row.get("TNA") or row.get("tna") or "").strip()
                tea_val = str(row.get("TEA") or row.get("tea") or "").strip()
                v_n = normalizar_valor(tna_val)
                if v_n is None:
                    continue
                fmt_tna = f"{tna_val} TNA" if "TNA" not in tna_val.upper() else tna_val
                suf_tea = f" (TEA {tea_val})" if tea_val else ""
                if "tamar" in nom_t:
                    CACHE_TASAS_PERSISTENTE["TASA_TAMAR"] = {"variable": "TASA_TAMAR", "descripcion": f"Tasa Referencia TAMAR{suf_tea}", "categoria": "tasas_y_monetario", "valor_formateado": fmt_tna, "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy}
                elif "badlar" in nom_t:
                    CACHE_TASAS_PERSISTENTE["TASA_BADLAR"] = {"variable": "TASA_BADLAR", "descripcion": f"Tasa BADLAR{suf_tea}", "categoria": "tasas_y_monetario", "valor_formateado": fmt_tna, "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy}
                elif "tm20" in nom_t:
                    CACHE_TASAS_PERSISTENTE["TASA_TM20"] = {"variable": "TASA_TM20", "descripcion": f"Tasa TM20{suf_tea}", "categoria": "tasas_y_monetario", "valor_formateado": fmt_tna, "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy}
                elif "call" in nom_t:
                    CACHE_TASAS_PERSISTENTE["TASA_CALL"] = {"variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": fmt_tna, "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy}
                elif "pase" in nom_t:
                    CACHE_TASAS_PERSISTENTE["TASA_PASES"] = {"variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA", "categoria": "tasas_y_monetario", "valor_formateado": fmt_tna, "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy}
    except Exception:
        pass
    obtener_promedio_plazo_fijo_bcra()
    _actualizar_cauciones_en_vivo()
    return list(CACHE_TASAS_PERSISTENTE.values())

BASE_DOLARITO_EXACTA: Dict[str, Dict[str, Any]] = {
    "DOLAR_OFICIAL": {"compra": 1540.00, "venta": 1590.00, "variacion": "+0,30%", "var_anio": "+7,4%", "fecha": "En vivo"},
    "DOLAR_BLUE": {"compra": 1555.00, "venta": 1575.00, "variacion": "0,00%", "var_anio": "+6,8%", "fecha": "En vivo"},
    "DOLAR_TARJETA": {"compra": None, "venta": 2067.00, "variacion": "+0,30%", "var_anio": "+7,4%", "fecha": "En vivo"},
    "DOLAR_NETFLIX": {"compra": None, "venta": 2400.90, "variacion": "+0,30%", "var_anio": "+7,4%", "fecha": "En vivo"},
    "DOLAR_MEP": {"compra": None, "venta": 1652.69, "variacion": "+0,50%", "var_anio": "+11,6%", "fecha": "AL30 / AL30D"},
    "DOLAR_CCL": {"compra": None, "venta": 1749.47, "variacion": "+0,30%", "var_anio": "+14,7%", "fecha": "AL30 / AL30C"},
    "DOLAR_CRIPTO": {"compra": 1775.00, "venta": 1779.98, "variacion": "+1,10%", "var_anio": "+15,4%", "fecha": "En vivo"},
    "DOLAR_A3500": {"compra": 1565.00, "venta": 1565.00, "variacion": "0,00%", "var_anio": "+7,4%", "fecha": "En vivo"},
    "DOLAR_FUTURO": {"compra": 1580.00, "venta": 1582.50, "variacion": "0,00%", "var_anio": "+8,2%", "fecha": "En vivo"}
}

def _fmt_var_dolar(val_raw: Any, default_str: str = "0,00%") -> str:
    if val_raw is None or val_raw == "" or str(val_raw).strip() == "-":
        return default_str
    v = normalizar_valor(val_raw)
    if v is None:
        return default_str
    if v == 0:
        return "0,00%"
    return f"+{v:.2f}%".replace(".", ",") if v > 0 else f"{v:.2f}%".replace(".", ",")

def obtener_dolares_rava_en_vivo() -> Dict[str, Dict[str, Any]]:
    res_rava: Dict[str, Dict[str, Any]] = {}
    ts_ms = int(time.time() * 1000)
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.rava.com/"
    }

    rava_directo: Dict[str, Dict[str, Any]] = {}
    urls_rava = [
        obtener_html_rava_compartido(ttl=3.0),
        f"https://www.rava.com/cotizaciones/dolares?_t={ts_ms}"
    ]

    def _mapear_especie_dolar_rava(sim: str) -> Optional[str]:
        s = sim.upper().strip()
        if s in ("MAYORISTA", "DOLAR MAYORISTA", "DÓLAR MAYORISTA", "A3500"):
            return "DOLAR_A3500"
        if s in ("OFICIAL", "DOLAR OFICIAL", "DÓLAR OFICIAL", "MINORISTA"):
            return "DOLAR_OFICIAL"
        if s in ("TARJETA", "DOLAR TARJETA", "DÓLAR TARJETA"):
            return "DOLAR_TARJETA"
        if s in ("MEP", "DOLAR MEP", "DÓLAR MEP", "BOLSA") or ("MEP" in s and "DOLAR" in s):
            return "DOLAR_MEP"
        if s in ("CCL", "DOLAR CCL", "DÓLAR CCL", "CONTADO CON LIQUI") or ("CCL" in s and "DOLAR" in s):
            return "DOLAR_CCL"
        if s in ("CRIPTO", "DOLAR CRIPTO", "DÓLAR CRIPTO", "USDT"):
            return "DOLAR_CRIPTO"
        if s in ("BLUE", "DOLAR BLUE", "DÓLAR BLUE"):
            return "DOLAR_BLUE"
        return None

    for src in urls_rava:
        raw_html = ""
        if src and src.startswith("http"):
            if all(k in rava_directo for k in ("DOLAR_A3500", "DOLAR_OFICIAL", "DOLAR_TARJETA", "DOLAR_MEP", "DOLAR_CCL", "DOLAR_CRIPTO")):
                break
            try:
                rr = GLOBAL_HTTP_SESSION.get(src, headers=hdrs, timeout=3.5)
                if rr.status_code == 200:
                    raw_html = rr.text
            except Exception:
                pass
        else:
            raw_html = src or ""

        if not raw_html:
            continue

        html_u = raw_html.replace("&quot;", '"').replace("&#34;", '"').replace("\\/", "/")

        # 1. Objetos JSON de Rava
        for m_obj in re.finditer(r'\{[^{}]*\}', html_u):
            b = m_obj.group(0)
            m_sim = re.search(r'"(?:simbolo|especie|nombre|identificador|titulo|name)"\s*:\s*"([^"]+)"', b, re.IGNORECASE)
            if not m_sim:
                continue
            cod_t = _mapear_especie_dolar_rava(m_sim.group(1))
            if not cod_t:
                continue
            m_u = re.search(r'"(?:ultimo|venta|valor|close|cierre|px_last|last)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
            m_c = re.search(r'"(?:compra|buy|bid)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
            m_d = re.search(r'"(?:variacion|var_dia|dia|change|porc|var)"\s*:\s*"?([+\-−]?[\d\.,]+|-)"?', b, re.IGNORECASE)
            m_a = re.search(r'"(?:var_anio|anio|anual|var_ytd|ytd)"\s*:\s*"?([+\-−]?[\d\.,]+|-)"?', b, re.IGNORECASE)
            vu = normalizar_valor(m_u.group(1)) if m_u else None
            vc = normalizar_valor(m_c.group(1)) if m_c else None
            vd_raw = m_d.group(1).strip() if m_d else None
            va_num = normalizar_valor(m_a.group(1)) if (m_a and m_a.group(1).strip() != "-") else None
            if vu and 900 <= vu <= 5000:
                solo_v = cod_t in ("DOLAR_MEP", "DOLAR_CCL", "DOLAR_TARJETA")
                prev_def_a = BASE_DOLARITO_EXACTA.get(cod_t, {}).get("var_anio", "0,0%")
                rava_directo[cod_t] = {
                    "compra": None if solo_v else (round(vc, 2) if (vc and vc > 900) else round(vu, 2)),
                    "venta": round(vu, 2),
                    "variacion": "0,00%" if vd_raw == "-" else _fmt_var_dolar(vd_raw, "0,00%"),
                    "var_anio": _fmt_pct_con_signo(va_num) if va_num is not None else prev_def_a,
                    "fecha": "AL30 / AL30D" if cod_t == "DOLAR_MEP" else ("AL30 / AL30C" if cod_t == "DOLAR_CCL" else "En vivo")
                }

        # 2. Celdas <tr> de la tabla Dólar de Rava (MAYORISTA | 1.565,00 | - | 2,0 | 7,4)
        soup = BeautifulSoup(raw_html, "html.parser")
        for tr in soup.find_all("tr"):
            celdas = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if len(celdas) >= 2:
                cod_t = _mapear_especie_dolar_rava(celdas[0])
                if cod_t:
                    vu = normalizar_valor(celdas[1])
                    vd_raw = celdas[2].strip() if len(celdas) > 2 else "0,00%"
                    va_num = normalizar_valor(celdas[4]) if (len(celdas) > 4 and celdas[4].strip() != "-") else None
                    if vu and 900 <= vu <= 5000:
                        solo_v = cod_t in ("DOLAR_MEP", "DOLAR_CCL", "DOLAR_TARJETA")
                        prev_def_a = rava_directo.get(cod_t, {}).get("var_anio") or BASE_DOLARITO_EXACTA.get(cod_t, {}).get("var_anio", "0,0%")
                        rava_directo[cod_t] = {
                            "compra": None if solo_v else round(vu, 2),
                            "venta": round(vu, 2),
                            "variacion": "0,00%" if vd_raw == "-" else _fmt_var_dolar(vd_raw, "0,00%"),
                            "var_anio": _fmt_pct_con_signo(va_num) if va_num is not None else prev_def_a,
                            "fecha": "AL30 / AL30D" if cod_t == "DOLAR_MEP" else ("AL30 / AL30C" if cod_t == "DOLAR_CCL" else "En vivo")
                        }

        # 3. Texto plano de respaldo capturando % DÍA, % MES y % AÑO
        plano = soup.get_text(" ", strip=True)
        patrones_plano = [
            ("DOLAR_A3500", r'\bMAYORISTA\b\s+([\d]{1,2}\.[\d]{3},[\d]{2})\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)'),
            ("DOLAR_OFICIAL", r'\bOFICIAL\b\s+([\d]{1,2}\.[\d]{3},[\d]{2})\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)'),
            ("DOLAR_TARJETA", r'\bTARJETA\b\s+([\d]{1,2}\.[\d]{3},[\d]{2})\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)'),
            ("DOLAR_MEP", r'\bMEP\b\s+([\d]{1,2}\.[\d]{3},[\d]{2})\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)'),
            ("DOLAR_CCL", r'\bCCL\b\s+([\d]{1,2}\.[\d]{3},[\d]{2})\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)'),
            ("DOLAR_CRIPTO", r'\bCRIPTO\b\s+([\d]{1,2}\.[\d]{3},[\d]{2})\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)\s+(-|[+\-−]?[\d]+,[\d]+)')
        ]
        for cod_k, pat in patrones_plano:
            if cod_k not in rava_directo:
                mt = re.search(pat, plano, re.IGNORECASE)
                if mt:
                    uv = normalizar_valor(mt.group(1))
                    dv = mt.group(2).strip()
                    av = normalizar_valor(mt.group(4)) if mt.group(4).strip() != "-" else None
                    if uv and 900 <= uv <= 5000:
                        solo_v = cod_k in ("DOLAR_MEP", "DOLAR_CCL", "DOLAR_TARJETA")
                        prev_def_a = BASE_DOLARITO_EXACTA.get(cod_k, {}).get("var_anio", "0,0%")
                        rava_directo[cod_k] = {
                            "compra": None if solo_v else round(uv, 2),
                            "venta": round(uv, 2),
                            "variacion": "0,00%" if dv == "-" else _fmt_var_dolar(dv, "0,00%"),
                            "var_anio": _fmt_pct_con_signo(av) if av is not None else prev_def_a,
                            "fecha": "AL30 / AL30D" if cod_k == "DOLAR_MEP" else ("AL30 / AL30C" if cod_k == "DOLAR_CCL" else "En vivo")
                        }

    for k, v in rava_directo.items():
        res_rava[k] = dict(v)
    return res_rava

def obtener_puntas_dolarito() -> Dict[str, Dict[str, Any]]:
    if time.time() - _DOLARITO_CICLO_CACHE["ts"] < 6.0 and _DOLARITO_CICLO_CACHE["data"]:
        return {k: dict(v) for k, v in _DOLARITO_CICLO_CACHE["data"].items()}
    if not _DOLARITO_CACHE_LOCK.acquire(blocking=False):
        if _DOLARITO_CICLO_CACHE["data"]:
            return {k: dict(v) for k, v in _DOLARITO_CICLO_CACHE["data"].items()}
        _DOLARITO_CACHE_LOCK.acquire()
    try:
        if time.time() - _DOLARITO_CICLO_CACHE["ts"] < 6.0 and _DOLARITO_CICLO_CACHE["data"]:
            return {k: dict(v) for k, v in _DOLARITO_CICLO_CACHE["data"].items()}
        return _obtener_puntas_dolarito_interno()
    finally:
        _DOLARITO_CACHE_LOCK.release()

def _obtener_puntas_dolarito_interno() -> Dict[str, Dict[str, Any]]:
    res: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in BASE_DOLARITO_EXACTA.items()}
    ts_ms = int(time.time() * 1000)
    hora_vivo = datetime.utcnow().strftime("%H:%M:%S UTC")

    # 1. Rava es la fuente maestra para MAYORISTA, OFICIAL, TARJETA, MEP, CCL y CRIPTO
    datos_rava = obtener_dolares_rava_en_vivo()
    for k_r, v_r in datos_rava.items():
        res[k_r] = dict(v_r)
        BASE_DOLARITO_EXACTA[k_r] = dict(v_r)

    # 2. Completar Blue y Futuro (y puntas de compra) sin pisar los valores de Rava
    try:
        r_da = GLOBAL_HTTP_SESSION.get(f"https://dolarapi.com/v1/dolares?_t={ts_ms}", headers={"User-Agent": "Mozilla/5.0"}, timeout=3.0)
        if r_da.status_code == 200:
            for item_da in r_da.json():
                casa = str(item_da.get("casa", "")).lower().strip()
                c_da = normalizar_valor(item_da.get("compra"))
                v_da = normalizar_valor(item_da.get("venta"))
                if not v_da or v_da < 900 or v_da > 5000:
                    continue
                if casa == "blue":
                    res["DOLAR_BLUE"] = {
                        "compra": round(c_da, 2) if (c_da and c_da > 900) else round(v_da - 20.0, 2),
                        "venta": round(v_da, 2),
                        "variacion": res["DOLAR_BLUE"].get("variacion", "0,00%"),
                        "fecha": hora_vivo
                    }
                    BASE_DOLARITO_EXACTA["DOLAR_BLUE"] = dict(res["DOLAR_BLUE"])
                elif casa == "oficial" and c_da and c_da > 900:
                    res["DOLAR_OFICIAL"]["compra"] = round(c_da, 2)
                    if "DOLAR_OFICIAL" not in datos_rava:
                        res["DOLAR_OFICIAL"]["venta"] = round(v_da, 2)
                    BASE_DOLARITO_EXACTA["DOLAR_OFICIAL"] = dict(res["DOLAR_OFICIAL"])
                elif casa == "mayorista" and "DOLAR_A3500" not in datos_rava:
                    res["DOLAR_A3500"] = {
                        "compra": round(c_da, 2) if (c_da and c_da > 900) else round(v_da, 2),
                        "venta": round(v_da, 2),
                        "variacion": res["DOLAR_A3500"].get("variacion", "0,00%"),
                        "fecha": hora_vivo
                    }
                    BASE_DOLARITO_EXACTA["DOLAR_A3500"] = dict(res["DOLAR_A3500"])
    except Exception:
        pass

    v_of = res["DOLAR_OFICIAL"]["venta"]
    if v_of and v_of > 1000:
        if "DOLAR_TARJETA" not in datos_rava:
            res["DOLAR_TARJETA"]["venta"] = round(v_of * 1.30, 2)
            res["DOLAR_TARJETA"]["var_anio"] = res["DOLAR_OFICIAL"].get("var_anio", "+7,4%")
        res["DOLAR_NETFLIX"]["venta"] = round(v_of * 1.51, 2)
        res["DOLAR_NETFLIX"]["variacion"] = res["DOLAR_OFICIAL"]["variacion"]
        res["DOLAR_NETFLIX"]["var_anio"] = res["DOLAR_OFICIAL"].get("var_anio", "+7,4%")

    _DOLARITO_CICLO_CACHE["data"] = {k: dict(v) for k, v in res.items()}
    _DOLARITO_CICLO_CACHE["ts"] = time.time()
    return res

def obtener_datos_dolar_futuro() -> Optional[Dict[str, Any]]:
    puntas = obtener_puntas_dolarito()
    info_f = puntas.get("DOLAR_FUTURO") or BASE_DOLARITO_EXACTA["DOLAR_FUTURO"]
    c_fut = info_f.get("compra") or 1563.00
    v_fut = float(info_f.get("venta") or 1563.50)
    var_fut = info_f.get("variacion") or "0,00%"
    return {
        "variable": "DOLAR_FUTURO",
        "descripcion": "DÓLAR FUTURO",
        "categoria": "dolar_y_cambiario",
        "compra": c_fut,
        "venta": v_fut,
        "valor_formateado": f"${v_fut:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "variacion": var_fut,
        "valor": v_fut,
        "fecha": info_f.get("fecha", "En vivo")
    }

def obtener_datos_monedas_y_dolar() -> List[Dict[str, Any]]:
    items = []
    hora_vivo = datetime.utcnow().strftime("%H:%M:%S UTC")

    # Leer el último A3500 sin bloquearse esperando el lock de Dolarito
    puntas_cache = _DOLARITO_CICLO_CACHE.get("data") or BASE_DOLARITO_EXACTA
    info_may = puntas_cache.get("DOLAR_A3500") or BASE_DOLARITO_EXACTA["DOLAR_A3500"]
    usd_oficial = float(info_may.get("venta") or 1544.00)
    var_may_str = info_may.get("variacion") or "0,00%"
    var_may_num = normalizar_valor(var_may_str) or 0.0
    fmt_may = f"${usd_oficial:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    items.append({
        "variable": "DOLAR_A3500",
        "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
        "categoria": "dolar_y_cambiario",
        "compra": usd_oficial,
        "venta": usd_oficial,
        "valor_formateado": fmt_may,
        "variacion": var_may_str,
        "valor": usd_oficial,
        "fecha": hora_vivo
    })

    # Consultar cotizaciones y variaciones diarias en vivo en paralelo desde Yahoo Finance / Stooq
    pares_fx = [("BRL=X", "usdbrl"), ("EURUSD=X", "eurusd"), ("CLP=X", "usdclp")]
    with ThreadPoolExecutor(max_workers=3) as ex_fx:
        res_fx = dict(zip([p[0] for p in pares_fx], ex_fx.map(lambda p: fetch_quote_externa(p[0], p[1]), pares_fx)))

    q_brl = res_fx.get("BRL=X") or {}
    q_eur = res_fx.get("EURUSD=X") or {}
    q_clp = res_fx.get("CLP=X") or {}

    # Respaldo instantáneo JSON abierto si Yahoo/Stooq no devolvieron algún par
    if not (q_brl.get("price") and q_eur.get("price") and q_clp.get("price")):
        try:
            r_er = GLOBAL_HTTP_SESSION.get("https://open.er-api.com/v6/latest/USD", timeout=3.0)
            if r_er.status_code == 200:
                rates = r_er.json().get("rates") or {}
                if not q_brl.get("price") and rates.get("BRL"):
                    q_brl = {"price": float(rates["BRL"]), "change_pct": 0.0}
                if not q_eur.get("price") and rates.get("EUR") and float(rates["EUR"]) > 0:
                    q_eur = {"price": 1.0 / float(rates["EUR"]), "change_pct": 0.0}
                if not q_clp.get("price") and rates.get("CLP"):
                    q_clp = {"price": float(rates["CLP"]), "change_pct": 0.0}
        except Exception:
            pass

    val_brl = round(float(q_brl.get("price") or 5.76), 4)
    chg_brl = float(q_brl.get("change_pct") or 0.0)

    val_eurusd = round(float(q_eur.get("price") or 1.0855), 4)
    chg_eurusd = float(q_eur.get("change_pct") or 0.0)

    val_clp = round(float(q_clp.get("price") or 958.40), 2)
    chg_clp = float(q_clp.get("change_pct") or 0.0)

    usd_ars_val = round(usd_oficial, 2)
    val_eurars = round(val_eurusd * usd_ars_val, 2)
    chg_eurars = round(((1.0 + chg_eurusd / 100.0) * (1.0 + var_may_num / 100.0) - 1.0) * 100.0, 2)

    def _fmt_v_fx(v):
        if v == 0:
            return "0,00%"
        return f"+{v:.2f}%".replace(".", ",") if v > 0 else f"{v:.2f}%".replace(".", ",")

    MONEDAS = [
        ("MONEDA_USDARS", "Dólar vs Peso Argentino (USD/ARS)", f"${usd_ars_val:,.2f} ARS".replace(",", "X").replace(".", ",").replace("X", "."), _fmt_v_fx(var_may_num), usd_ars_val),
        ("MONEDA_USDBRL", "Dólar vs Real Brasileño (USD/BRL)", f"R$ {val_brl:.4f}".replace(".", ","), _fmt_v_fx(chg_brl), val_brl),
        ("MONEDA_EURUSD", "Euro vs Dólar (EUR/USD)", f"{val_eurusd:.4f} USD".replace(".", ","), _fmt_v_fx(chg_eurusd), val_eurusd),
        ("MONEDA_EURARS", "Euro vs Peso Argentino (EUR/ARS)", f"${val_eurars:,.2f} ARS".replace(",", "X").replace(".", ",").replace("X", "."), _fmt_v_fx(chg_eurars), val_eurars),
        ("MONEDA_USDCLP", "Dólar vs Peso Chileno (USD/CLP)", f"CLP$ {val_clp:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), _fmt_v_fx(chg_clp), val_clp)
    ]
    for cod, desc, fmt, var_str, val_n in MONEDAS:
        items.append({"variable": cod, "descripcion": desc, "categoria": "monedas", "valor_formateado": fmt, "variacion": var_str, "valor": val_n, "fecha": hora_vivo})

    for cod_d, desc_d in [("DOLAR_OFICIAL", "Dólar Oficial Minorista"), ("DOLAR_BLUE", "Dólar Blue")]:
        if cod_d in puntas_cache:
            p_info = puntas_cache[cod_d]
            c_val = p_info["compra"]
            v_val = p_info["venta"]
            fmt_v = f"${v_val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            items.append({
                "variable": cod_d,
                "descripcion": desc_d,
                "categoria": "dolar_y_cambiario",
                "compra": c_val,
                "venta": v_val,
                "valor_formateado": fmt_v,
                "variacion": p_info.get("variacion", "0,00%"),
                "valor": v_val,
                "fecha": p_info.get("fecha", hora_vivo)
            })
    return items

def obtener_datos_bonos_sjb() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = GLOBAL_HTTP_SESSION.get("https://sjbdashboards.up.railway.app/api/bonos/precios-recientes", headers={"User-Agent": "Mozilla/5.0"}, timeout=3.0)
        if r.status_code == 200:
            bonos_raw = r.json()
            if isinstance(bonos_raw, dict):
                for tk, val in bonos_raw.items():
                    if val is not None:
                        try:
                            v_float = float(val)
                            items.append({
                                "variable": f"BONO_{tk}", "descripcion": f"Bono {tk}", "categoria": "cotizacion_bonos",
                                "valor_formateado": f"{v_float:,.2f}", "variacion": "0,0%", "valor": round(v_float, 2), "fecha": fec_hoy
                            })
                        except Exception:
                            pass
    except Exception:
        pass
    return items

def normalizar_respuesta_curva(raw_data: Any) -> Optional[Dict[str, Any]]:
    if not raw_data:
        return None
    if isinstance(raw_data, list):
        return {"tabla": raw_data}
    if isinstance(raw_data, dict):
        for k in ["tabla", "data", "bonos", "filas", "items", "rows", "resultados"]:
            if isinstance(raw_data.get(k), list) and len(raw_data.get(k)) > 0:
                res = dict(raw_data)
                res["tabla"] = raw_data.get(k)
                return res
        return raw_data
    return None


US_TREASURIES_BASE = [
    {"Ticker": "UST 1M", "Instrumento": "T-Bill 1 Mes", "Tramo": "Corto", "Dias": 30, "TIR": "4.52%", "Variacion": "0.00%", "MD": 0.08, "cnbc": "US1M", "yf": None},
    {"Ticker": "UST 2M", "Instrumento": "T-Bill 2 Meses", "Tramo": "Corto", "Dias": 60, "TIR": "4.48%", "Variacion": "0.00%", "MD": 0.16, "cnbc": "US2M", "yf": None},
    {"Ticker": "UST 3M", "Instrumento": "T-Bill 3 Meses", "Tramo": "Corto", "Dias": 91, "TIR": "4.42%", "Variacion": "-0.01%", "MD": 0.24, "cnbc": "US3M", "yf": "^IRX"},
    {"Ticker": "UST 6M", "Instrumento": "T-Bill 6 Meses", "Tramo": "Corto", "Dias": 182, "TIR": "4.31%", "Variacion": "-0.02%", "MD": 0.48, "cnbc": "US6M", "yf": None},
    {"Ticker": "UST 1Y", "Instrumento": "T-Bill 1 Año", "Tramo": "Corto", "Dias": 365, "TIR": "4.24%", "Variacion": "+0.01%", "MD": 0.95, "cnbc": "US1Y", "yf": None},
    {"Ticker": "UST 2Y", "Instrumento": "T-Note 2 Años", "Tramo": "Mediano", "Dias": 730, "TIR": "4.18%", "Variacion": "+0.02%", "MD": 1.88, "cnbc": "US2Y", "yf": None},
    {"Ticker": "UST 3Y", "Instrumento": "T-Note 3 Años", "Tramo": "Mediano", "Dias": 1095, "TIR": "4.15%", "Variacion": "+0.02%", "MD": 2.75, "cnbc": "US3Y", "yf": None},
    {"Ticker": "UST 5Y", "Instrumento": "T-Note 5 Años", "Tramo": "Mediano", "Dias": 1825, "TIR": "4.17%", "Variacion": "+0.03%", "MD": 4.38, "cnbc": "US5Y", "yf": "^FVX"},
    {"Ticker": "UST 7Y", "Instrumento": "T-Note 7 Años", "Tramo": "Mediano", "Dias": 2555, "TIR": "4.23%", "Variacion": "+0.03%", "MD": 5.85, "cnbc": "US7Y", "yf": None},
    {"Ticker": "UST 10Y", "Instrumento": "T-Note 10 Años", "Tramo": "Largo", "Dias": 3650, "TIR": "4.30%", "Variacion": "+0.04%", "MD": 7.85, "cnbc": "US10Y", "yf": "^TNX"},
    {"Ticker": "UST 20Y", "Instrumento": "T-Bond 20 Años", "Tramo": "Largo", "Dias": 7300, "TIR": "4.58%", "Variacion": "+0.03%", "MD": 12.60, "cnbc": "US20Y", "yf": None},
    {"Ticker": "UST 30Y", "Instrumento": "T-Bond 30 Años", "Tramo": "Largo", "Dias": 10950, "TIR": "4.51%", "Variacion": "+0.04%", "MD": 15.80, "cnbc": "US30Y", "yf": "^TYX"}
]

def fetch_curva_us_treasuries() -> Dict[str, Any]:
    filas = []
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    # 1. Consulta en tiempo real a CNBC Quotes API (devuelve los 12 tramos de US Treasuries en 1 request)
    cnbc_map = {}
    try:
        syms = "|".join([b["cnbc"] for b in US_TREASURIES_BASE])
        url_cnbc = f"https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols={syms}&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json&events=1"
        r = GLOBAL_HTTP_SESSION.get(url_cnbc, headers=headers, timeout=3.5)
        if r.status_code == 200:
            q_list = r.json().get("FormattedQuoteResult", {}).get("FormattedQuote", [])
            if isinstance(q_list, dict):
                q_list = [q_list]
            for q in q_list:
                sym = str(q.get("symbol", "")).upper()
                last_str = str(q.get("last", "")).replace("%", "").strip()
                chg_str = str(q.get("change", "")).replace("%", "").strip()
                val_y = normalizar_valor(last_str)
                val_c = normalizar_valor(chg_str)
                if val_y is not None and 0 < val_y < 20:
                    cnbc_map[sym] = (val_y, val_c if val_c is not None else 0.0)
    except Exception:
        pass

    # 2. Respaldo en vivo con Yahoo Finance para nodos clave (^IRX, ^FVX, ^TNX, ^TYX) si CNBC no respondió
    yf_map = {}
    if len(cnbc_map) < 4:
        for b in US_TREASURIES_BASE:
            if b["yf"]:
                q_yf = fetch_quote_externa(b["yf"], "")
                if q_yf and q_yf.get("price"):
                    yf_map[b["Ticker"]] = (q_yf["price"], q_yf.get("change_pct", 0.0))

    for b in US_TREASURIES_BASE:
        tk = b["Ticker"]
        cnbc_sym = b["cnbc"]
        tir_num = normalizar_valor(b["TIR"]) or 4.25
        var_str = b["Variacion"]

        if cnbc_sym in cnbc_map:
            tir_num, chg_val = cnbc_map[cnbc_sym]
            var_str = f"+{chg_val:.2f}%" if chg_val > 0 else f"{chg_val:.2f}%"
        elif tk in yf_map:
            tir_num, chg_val = yf_map[tk]
            var_str = f"+{chg_val:.2f}%" if chg_val > 0 else f"{chg_val:.2f}%"

        # Precio técnico referencial base 100 para paridad visual en la tabla
        dur = float(b["MD"])
        precio_ref = round(100.0 - (tir_num - 4.25) * dur * 0.85, 2)

        filas.append({
            "Ticker": tk,
            "Instrumento": b["Instrumento"],
            "Tramo": b["Tramo"],
            "Dias": b["Dias"],
            "Precio": precio_ref,
            "Variacion": var_str,
            "TIR": f"{tir_num:.2f}%",
            "MD": dur
        })

    return {"tabla": filas}

def fetch_bonos_cer_rava_fallback() -> Optional[Dict[str, Any]]:
    try:
        r = GLOBAL_HTTP_SESSION.get(
            f"https://www.rava.com/cotizaciones/bonos?_t={int(time.time()*1000)}",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=4.5
        )
        if r.status_code != 200:
            return None
        html_u = r.text.replace("&quot;", '"')
        cer_meta = {
            "TZXD6": ("2026-12-15", 30), "TZXE7": ("2027-01-31", 76), "TZXF7": ("2027-02-28", 104),
            "TX27": ("2027-03-25", 129), "TZXA7": ("2027-04-30", 165), "TZXM7": ("2027-05-31", 196),
            "TZX27": ("2027-06-30", 226), "TZXO7": ("2027-10-31", 349), "TZXD7": ("2027-12-15", 394),
            "TZXM8": ("2028-03-31", 501), "TZXJ8": ("2028-06-30", 592), "TX28": ("2028-11-09", 724),
            "TZX28": ("2028-12-15", 760), "TX29": ("2029-06-30", 957), "TX31": ("2031-11-30", 1840),
            "DICP": ("2033-12-31", 2602), "PARP": ("2038-12-31", 4428), "CUAP": ("2045-12-31", 6985)
        }
        filas = []
        vistos = set()
        for m_obj in re.finditer(r'\{[^{}]*"(?:simbolo|especie)"\s*:\s*"([A-Z0-9]{3,6})"[^{}]*\}', html_u):
            b = m_obj.group(0)
            sim = m_obj.group(1).upper().strip()
            if sim in cer_meta and sim not in vistos:
                m_u = re.search(r'"(?:ultimo|px_last|close|valor)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b)
                m_d = re.search(r'"(?:variacion|var_dia|dia|change|porc)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b)
                m_t = re.search(r'"(?:tir|tea|rendimiento)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b)
                m_p = re.search(r'"(?:paridad|parity)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b)
                px = normalizar_valor(m_u.group(1)) if m_u else None
                var_d = normalizar_valor(m_d.group(1)) if m_d else 0.0
                tir_v = normalizar_valor(m_t.group(1)) if m_t else None
                par_v = normalizar_valor(m_p.group(1)) if m_p else None
                if px and px > 10:
                    vistos.add(sim)
                    vto, dias = cer_meta[sim]
                    tir_final = tir_v if (tir_v is not None and -20 < tir_v < 80) else round(5.5 + (dias / 1000.0) * 1.4, 2)
                    tem_final = round(((1 + tir_final / 100.0) ** (1 / 12.0) - 1) * 100.0, 2)
                    filas.append({
                        "Ticker": sim,
                        "Vencimiento": vto,
                        "Dias": dias,
                        "Precio": px,
                        "Variacion": f"+{var_d:.2f}%" if var_d > 0 else f"{var_d:.2f}%",
                        "TIR": f"{tir_final:.2f}%",
                        "TEM": f"{tem_final:.2f}%",
                        "Paridad": f"{par_v:.2f}%" if par_v else "92.50%",
                        "MD": round(dias / 365.0 * 0.92, 2)
                    })
        if len(filas) >= 4:
            filas.sort(key=lambda x: x["Dias"])
            return {"tabla": filas}
    except Exception:
        pass
    return None

CURVAS_FALLBACK_META = {
    "lecaps": [
        ("S30N6", "2026-11-30", 104.25), ("T15E7", "2027-01-15", 108.15), ("S29E7", "2027-01-29", 109.10),
        ("S26F7", "2027-02-26", 111.45), ("T30A7", "2027-04-30", 117.65), ("T31Y7", "2027-05-31", 120.40),
        ("T30J7", "2027-06-30", 123.10), ("TY30P", "2027-09-30", 132.85)
    ],
    "tamar": [
        ("M29E7", "2027-01-29", 108.40), ("M31M7", "2027-03-31", 113.90), ("M30A7", "2027-04-30", 116.40),
        ("M30J7", "2027-06-30", 121.50), ("M15S7", "2027-09-15", 128.90), ("M15D7", "2027-12-15", 137.60)
    ],
    "dollar_linked": [
        ("TZVD6", "2026-12-15", 1125.0), ("TZVM7", "2027-03-31", 1110.0), ("TZV27", "2027-06-30", 1085.0),
        ("TZVD7", "2027-12-15", 1052.0), ("TZV28", "2028-06-30", 1024.0)
    ],
    "soberanos": [
        ("AL29", "2029-07-09", 76450.0, "Local"), ("GD29", "2029-07-09", 78900.0, "NY"),
        ("AL30", "2030-07-09", 72150.0, "Local"), ("GD30", "2030-07-09", 74800.0, "NY"),
        ("AL35", "2035-07-09", 65400.0, "Local"), ("GD35", "2035-07-09", 67900.0, "NY"),
        ("AE38", "2038-01-09", 68200.0, "Local"), ("GD38", "2038-01-09", 71300.0, "NY"),
        ("AL41", "2041-07-09", 61200.0, "Local"), ("GD41", "2041-07-09", 63800.0, "NY"),
        ("GD46", "2046-07-09", 64900.0, "NY")
    ],
    "bopreales": [
        ("BPY26", "2026-11-30", 103400.0), ("BPOA7", "2027-10-31", 96500.0),
        ("BPOB7", "2027-10-31", 95800.0), ("BPOC7", "2027-10-31", 94900.0), ("BPOD7", "2027-10-31", 94200.0)
    ]
}

def _fetch_curva_rava_respaldo(clave: str) -> Optional[Dict[str, Any]]:
    ts_ms = int(time.time() * 1000)
    hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    rava_map: Dict[str, Dict[str, Any]] = {}

    for u in [f"https://www.rava.com/cotizaciones/bonos?_t={ts_ms}", f"https://www.rava.com/cotizaciones/letras?_t={ts_ms}"]:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers=hdrs, timeout=4.2)
            if r.status_code != 200:
                continue
            html_u = r.text.replace("&quot;", '"')
            for m_obj in re.finditer(r'\{[^{}]*"(?:simbolo|especie)"\s*:\s*"([A-Z0-9]{3,7})"[^{}]*\}', html_u):
                b = m_obj.group(0)
                sim = m_obj.group(1).upper().strip()
                m_u = re.search(r'"(?:ultimo|px_last|close|valor)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b)
                m_d = re.search(r'"(?:variacion|var_dia|dia|change|porc)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b)
                m_t = re.search(r'"(?:tir|tea|rendimiento)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b)
                m_p = re.search(r'"(?:paridad|parity)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b)
                px = normalizar_valor(m_u.group(1)) if m_u else None
                if px and px > 5:
                    rava_map[sim] = {
                        "precio": px,
                        "var": normalizar_valor(m_d.group(1)) if m_d else 0.0,
                        "tir": normalizar_valor(m_t.group(1)) if m_t else None,
                        "paridad": normalizar_valor(m_p.group(1)) if m_p else None
                    }
        except Exception:
            pass

    if not rava_map:
        return None

    mep_ref = 1584.0
    if "AL30" in rava_map and "AL30D" in rava_map and rava_map["AL30D"]["precio"] > 0:
        ratio = rava_map["AL30"]["precio"] / rava_map["AL30D"]["precio"]
        mep_ref = ratio * 100.0 if ratio < 15 else ratio

    hoy = datetime.utcnow().date()
    filas = []

    if clave in ("soberanos", "soberanos_mep", "soberanos_ars", "soberanos_ccl"):
        suf = "C" if clave == "soberanos_ccl" else ("D" if clave in ("soberanos", "soberanos_mep") else "")
        for tk_base, vto, def_ars, ley in CURVAS_FALLBACK_META["soberanos"]:
            tk = tk_base + suf
            info = rava_map.get(tk) or rava_map.get(tk_base)
            if not info:
                continue
            px = info["precio"]
            if suf and px > 500:
                px = round(px / mep_ref, 2)
            dias = max(1, (datetime.strptime(vto, "%Y-%m-%d").date() - hoy).days)
            vd = info["var"] or 0.0
            tir = info["tir"] if info["tir"] is not None else 12.10
            par = info["paridad"] if info["paridad"] is not None else 70.0
            filas.append({
                "Ticker": tk, "Ley": ley, "Vencimiento": vto, "Dias": dias,
                "Precio": round(px, 2), "Variacion": f"+{vd:.2f}%" if vd > 0 else f"{vd:.2f}%",
                "TIR": f"{tir:.2f}%", "Paridad": f"{par:.2f}%", "MD": round(dias / 365.0 * 0.65, 2)
            })

    elif clave in ("bopreales", "bopreales_mep", "bopreales_ars"):
        suf = "D" if clave in ("bopreales", "bopreales_mep") else ""
        for tk_base, vto, def_ars in CURVAS_FALLBACK_META["bopreales"]:
            tk = tk_base + suf
            info = rava_map.get(tk) or rava_map.get(tk_base)
            if not info:
                continue
            px = info["precio"]
            if suf and px > 500:
                px = round(px / mep_ref, 2)
            dias = max(1, (datetime.strptime(vto, "%Y-%m-%d").date() - hoy).days)
            vd = info["var"] or 0.0
            tir = info["tir"] if info["tir"] is not None else 11.20
            par = info["paridad"] if info["paridad"] is not None else 89.50
            filas.append({
                "Ticker": tk, "Vencimiento": vto, "Dias": dias,
                "Precio": round(px, 2), "Variacion": f"+{vd:.2f}%" if vd > 0 else f"{vd:.2f}%",
                "TIR": f"{tir:.2f}%", "Paridad": f"{par:.2f}%", "MD": round(dias / 365.0 * 0.90, 2)
            })

    elif clave in ("lecaps", "tamar", "dollar_linked"):
        for tk, vto, def_px in CURVAS_FALLBACK_META[clave]:
            info = rava_map.get(tk)
            if not info:
                continue
            px = info["precio"]
            dias = max(1, (datetime.strptime(vto, "%Y-%m-%d").date() - hoy).days)
            vd = info["var"] or 0.0
            tir = info["tir"] if info["tir"] is not None else (27.5 if clave == "lecaps" else (32.0 if clave == "tamar" else 6.8))
            tem = round(((1 + tir / 100.0) ** (1 / 12.0) - 1) * 100.0, 2)
            fila = {
                "Ticker": tk, "Vencimiento": vto, "Dias": dias,
                "Precio": round(px, 2), "Variacion": f"+{vd:.2f}%" if vd > 0 else f"{vd:.2f}%",
                "TIR": f"{tir:.2f}%", "TEM": f"{tem:.2f}%", "MD": round(dias / 365.0 * 0.95, 2)
            }
            if clave == "dollar_linked":
                fila["Paridad"] = f"{(info['paridad'] or 95.0):.2f}%"
            filas.append(fila)

    return {"tabla": filas} if len(filas) >= 2 else None

def fetch_una_curva_sjb(clave: str, url: str) -> tuple:
    ts_ms = int(time.time() * 1000)
    sep = "&" if "?" in url else "?"
    urls_try = [f"{url}{sep}_t={ts_ms}"]

    variantes_sjb = {
        "cer": ["BONOS%20CER", "CER", "Bonos%20CER"],
        "lecaps": ["LECAP%20y%20BONCAP", "LECAPS", "LECAP", "Lecaps%20y%20Boncaps"],
        "tamar": ["LETAM", "TAMAR", "BONOS%20TAMAR", "Letras%20TAMAR"],
        "dollar_linked": ["D%C3%B3lar%20Linked", "DOLLAR%20LINKED", "DOLAR%20LINKED", "Dollar%20Linked"],
        "soberanos": ["SOBERANOS&moneda=MEP", "Soberanos&moneda=MEP"],
        "soberanos_mep": ["SOBERANOS&moneda=MEP", "Soberanos&moneda=MEP"],
        "soberanos_ars": ["SOBERANOS&moneda=ARS", "Soberanos&moneda=ARS"],
        "bopreales": ["BOPREALES&moneda=MEP", "BOPREAL&moneda=MEP", "Bopreales&moneda=MEP"],
        "bopreales_mep": ["BOPREALES&moneda=MEP", "BOPREAL&moneda=MEP", "Bopreales&moneda=MEP"],
        "bopreales_ars": ["BOPREALES&moneda=ARS", "BOPREAL&moneda=ARS", "Bopreales&moneda=ARS"]
    }
    if clave in variantes_sjb:
        urls_try = []
        for cat_v in variantes_sjb[clave]:
            mon_part = "" if "moneda=" in cat_v else "&moneda=ARS"
            urls_try.append(f"https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria={cat_v}{mon_part}&_t={ts_ms}")

    for u_item in urls_try:
        try:
            r = GLOBAL_HTTP_SESSION.get(u_item, headers={"User-Agent": "Mozilla/5.0"}, timeout=5.0)
            if r.status_code == 200:
                norm = normalizar_respuesta_curva(r.json())
                if norm and norm.get("tabla"):
                    return clave, norm
        except Exception:
            pass

    if clave == "cer":
        fb_cer = fetch_bonos_cer_rava_fallback()
        if fb_cer:
            return clave, fb_cer
    else:
        fb_gen = _fetch_curva_rava_respaldo(clave)
        if fb_gen:
            return clave, fb_gen

    return clave, None

def fetch_inflacion_implicita_sjb():
    ts_ms = int(time.time() * 1000)
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://sjbdashboards.up.railway.app/"
    }
    for u in [
        f"https://sjbdashboards.up.railway.app/api/bonos/inflacion-implicita?_t={ts_ms}",
        f"https://sjbdashboards.up.railway.app/api/inflacion-implicita?_t={ts_ms}",
        f"https://sjbdashboards.up.railway.app/api/intradiario/inflacion-implicita?_t={ts_ms}",
        f"https://sjbdashboards.up.railway.app/api/intradiario/breakeven?_t={ts_ms}",
        f"https://sjbdashboards.up.railway.app/api/bonos/breakeven?_t={ts_ms}"
    ]:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers=hdrs, timeout=4.5)
            if r.status_code == 200 and r.text.strip():
                data_j = r.json()
                if isinstance(data_j, list) and len(data_j) >= 5:
                    return {"series": data_j, "fuente": "SJB_LIVE"}
                if isinstance(data_j, dict) and any(
                    isinstance(data_j.get(k), list) and len(data_j.get(k)) >= 5
                    for k in ("series", "chart", "data", "tabla", "filas", "puntos", "meses", "labels")
                ):
                    data_j["fuente"] = "SJB_LIVE"
                    return data_j
        except Exception:
            pass

    # Respaldo dinámico sincronizado con las curvas vivas LECAP/BONCAP y CER de SJB
    lecaps = (CACHE_CURVAS_SJB.get("lecaps") or {}).get("tabla") or []
    cer = (CACHE_CURVAS_SJB.get("cer") or {}).get("tabla") or []
    if lecaps and cer:
        from datetime import timezone, timedelta
        hoy_ar = datetime.now(timezone(timedelta(hours=-3))).date()
        meses_en = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        def _extraer_pts_tem(tabla):
            pts = []
            for row in tabla:
                d = normalizar_valor(row.get("Dias") or row.get("dias") or row.get("plazo"))
                if d is None:
                    vto_s = str(row.get("Vencimiento") or row.get("vencimiento") or "").strip()
                    if len(vto_s) >= 10:
                        try:
                            d = (datetime.strptime(vto_s[:10], "%Y-%m-%d").date() - hoy_ar).days
                        except Exception:
                            pass
                tem = normalizar_valor(row.get("TEM") or row.get("tem"))
                if tem is None:
                    tir = normalizar_valor(row.get("TIR") or row.get("tir") or row.get("TEA") or row.get("tea"))
                    if tir is not None:
                        tem = ((1.0 + tir / 100.0) ** (1.0 / 12.0) - 1.0) * 100.0
                if d is not None and d > 0 and tem is not None and -15.0 <= tem <= 25.0:
                    pts.append((float(d), float(tem)))
            pts.sort(key=lambda x: x[0])
            return pts

        def _interpolar_lineal(pts, dias_obj):
            if not pts:
                return None
            if dias_obj <= pts[0][0]:
                return round(pts[0][1], 2)
            if dias_obj >= pts[-1][0]:
                return round(pts[-1][1], 2)
            for i in range(len(pts) - 1):
                d0, t0 = pts[i]
                d1, t1 = pts[i + 1]
                if d0 <= dias_obj <= d1:
                    if d1 == d0:
                        return round(t0, 2)
                    w = (dias_obj - d0) / (d1 - d0)
                    return round(t0 + w * (t1 - t0), 2)
            return round(pts[-1][1], 2)

        pts_nom = _extraer_pts_tem(lecaps)
        pts_cer = _extraer_pts_tem(cer)

        serie = [
            {"mes": "Jun 2026", "tipo": "real", "ipc_real": 1.89, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Jul 2026", "tipo": "real", "ipc_real": 2.11, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Aug 2026", "tipo": "real", "ipc_real": 1.66, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Sep 2026", "tipo": "real", "ipc_real": 1.93, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Oct 2026", "tipo": "real", "ipc_real": 2.10, "inf_implicita": None, "tem_nominal": None, "tem_cer": None}
        ]

        for offset_m in range(0, 9):
            yr = hoy_ar.year + ((hoy_ar.month - 1 + offset_m) // 12)
            mo = ((hoy_ar.month - 1 + offset_m) % 12) + 1
            if mo == 12:
                fin_mes = datetime(yr, 12, 31).date()
            else:
                fin_mes = (datetime(yr, mo + 1, 1) - timedelta(days=1)).date()
            dias_m = max(5, (fin_mes - hoy_ar).days)
            lbl_m = f"{meses_en[mo - 1]} {yr}"
            t_nom = _interpolar_lineal(pts_nom, dias_m)
            t_cer = _interpolar_lineal(pts_cer, dias_m)
            if t_nom is not None and t_cer is not None:
                be = round((((1.0 + t_nom / 100.0) / (1.0 + t_cer / 100.0)) - 1.0) * 100.0, 2)
                serie.append({
                    "mes": lbl_m, "tipo": "implicita", "ipc_real": None,
                    "inf_implicita": be, "tem_nominal": t_nom, "tem_cer": t_cer
                })
        if len(serie) >= 8:
            return {"series": serie, "fuente": "CURVAS_SJB_CALC"}
    return None

BANDAS_BCRA_TABLA_BASE = {
    "2026-11-16": (704.72, 1971.72), "2026-11-17": (704.27, 1972.96), "2026-11-18": (703.82, 1974.20),
    "2026-11-19": (703.37, 1975.44), "2026-11-20": (702.92, 1976.68), "2026-11-24": (701.12, 1981.64),
    "2026-11-25": (700.68, 1982.89), "2026-11-26": (700.23, 1984.13), "2026-11-27": (699.78, 1985.38),
    "2026-11-30": (698.44, 1989.12), "2026-12-01": (697.94, 1990.51), "2026-12-02": (697.44, 1991.91),
    "2026-12-03": (696.94, 1993.31), "2026-12-04": (696.44, 1994.71), "2026-12-09": (693.94, 2001.72),
    "2026-12-10": (693.45, 2003.13), "2026-12-11": (692.95, 2004.54), "2026-12-14": (691.46, 2008.76),
    "2026-12-15": (690.96, 2010.17), "2026-12-16": (690.47, 2011.58), "2026-12-17": (689.97, 2013.00),
    "2026-12-18": (689.48, 2014.41), "2026-12-21": (687.99, 2018.66), "2026-12-22": (687.50, 2020.08),
    "2026-12-23": (687.51, 2021.49), "2026-12-24": (687.52, 2022.91), "2026-12-28": (687.53, 2028.60),
    "2026-12-29": (687.54, 2030.03), "2026-12-30": (687.55, 2031.45), "2026-12-31": (687.56, 2032.88),
}

_BCRA_BANDAS_CACHE = {"ts": 0.0, "tabla": dict(BANDAS_BCRA_TABLA_BASE)}
_BCRA_BANDAS_LOCK = threading.Lock()

def _actualizar_tabla_bandas_bcra() -> Dict[str, tuple]:
    now = time.time()
    # Refresca cada 3 horas para capturar tanto el cambio diario como las publicaciones mensuales del BCRA
    if now - _BCRA_BANDAS_CACHE["ts"] < 10800.0 and _BCRA_BANDAS_CACHE["tabla"]:
        return _BCRA_BANDAS_CACHE["tabla"]
    if not _BCRA_BANDAS_LOCK.acquire(blocking=False):
        return _BCRA_BANDAS_CACHE["tabla"]
    try:
        r = GLOBAL_HTTP_SESSION.get(
            f"https://www.bcra.gob.ar/regimen-de-bandas-cambiarias/?_t={int(now)}",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=4.5,
            verify=False,
        )
        if r.status_code == 200 and r.text:
            soup = BeautifulSoup(r.text, "html.parser")
            nueva_tabla = dict(_BCRA_BANDAS_CACHE["tabla"])
            for tr in soup.find_all("tr"):
                celdas = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(celdas) >= 3:
                    m_fec = re.search(r"(\d{1,2})/(\d{1,2})/(20\d{2})", celdas[0])
                    if m_fec:
                        f_iso = f"{m_fec.group(3)}-{int(m_fec.group(2)):02d}-{int(m_fec.group(1)):02d}"
                        b_inf = normalizar_valor(celdas[1])
                        b_sup = normalizar_valor(celdas[2])
                        if b_inf and b_sup and 400 <= b_inf <= 1500 and 1200 <= b_sup <= 5000:
                            nueva_tabla[f_iso] = (round(b_inf, 2), round(b_sup, 2))
            if len(nueva_tabla) > 0:
                _BCRA_BANDAS_CACHE["tabla"] = nueva_tabla
        _BCRA_BANDAS_CACHE["ts"] = time.time()
    except Exception:
        _BCRA_BANDAS_CACHE["ts"] = time.time() - 9900.0
    finally:
        _BCRA_BANDAS_LOCK.release()
    return _BCRA_BANDAS_CACHE["tabla"]

def fetch_bandas_dolarito() -> Dict[str, Any]:
    from datetime import timezone, timedelta
    tz_ar = timezone(timedelta(hours=-3))
    ahora_ar = datetime.now(tz_ar)
    hoy_iso = ahora_ar.strftime("%Y-%m-%d")

    tabla = _actualizar_tabla_bandas_bcra()
    fechas_ord = sorted(tabla.keys())
    fecha_vigente = hoy_iso

    if hoy_iso in tabla:
        b_inf, b_sup = tabla[hoy_iso]
    else:
        # Si es fin de semana o feriado, tomar el último día hábil vigente anterior (o el más cercano)
        anteriores = [f for f in fechas_ord if f <= hoy_iso]
        if anteriores:
            fecha_vigente = anteriores[-1]
            b_inf, b_sup = tabla[fecha_vigente]
        elif fechas_ord:
            fecha_vigente = fechas_ord[0]
            b_inf, b_sup = tabla[fecha_vigente]
        else:
            b_inf, b_sup = 702.92, 1976.68

    d_may = 1544.00
    try:
        puntas_dl = obtener_puntas_dolarito()
        if "DOLAR_A3500" in puntas_dl and puntas_dl["DOLAR_A3500"].get("venta"):
            d_may = float(puntas_dl["DOLAR_A3500"]["venta"])
    except Exception:
        pass

    return {
        "banda_inferior": round(b_inf, 2),
        "banda_superior": round(b_sup, 2),
        "dolar_mayorista": round(d_may, 2),
        "fecha_banda": fecha_vigente
    }

def fetch_finviz_sectores(res_usa: Optional[List[Dict[str, Any]]] = None) -> Optional[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    tickers_map = {
        "basic materials": "XLB", "communication services": "XLC", "consumer cyclical": "XLY",
        "consumer defensive": "XLP", "energy": "XLE", "financial": "XLF", "healthcare": "XLV",
        "industrials": "XLI", "real estate": "XLRE", "technology": "XLK", "utilities": "XLU"
    }
    nombres_spdr = {
        "SPDR_XLK": ("Technology", "XLK"),
        "SPDR_XLF": ("Financial", "XLF"),
        "SPDR_XLE": ("Energy", "XLE"),
        "SPDR_XLV": ("Healthcare", "XLV"),
        "SPDR_XLI": ("Industrials", "XLI"),
        "SPDR_XLY": ("Consumer Cyclical", "XLY"),
        "SPDR_XLP": ("Consumer Defensive", "XLP"),
        "SPDR_XLU": ("Utilities", "XLU"),
        "SPDR_XLB": ("Basic Materials", "XLB"),
        "SPDR_XLRE": ("Real Estate", "XLRE"),
        "SPDR_XLC": ("Communication Services", "XLC")
    }

    data_res: Dict[str, Any] = {"1D": [], "1W": [], "1M": [], "1Y": []}

    # Calcular 1D en tiempo real desde las variaciones de los 11 ETFs sectoriales en vivo
    if res_usa:
        for item in res_usa:
            cod = item.get("variable", "")
            if cod in nombres_spdr:
                nom_en, tk = nombres_spdr[cod]
                chg_d = item.get("chg_pct")
                if chg_d is None:
                    chg_d = normalizar_valor(item.get("variacion", "0")) or 0.0
                data_res["1D"].append({"name": nom_en, "ticker": tk, "perf": round(float(chg_d), 2)})

    try:
        r = GLOBAL_HTTP_SESSION.get("https://finviz.com/groups?g=sector&v=210&o=name", headers=headers, timeout=5.0)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 10:
                    nom = tds[1].get_text(strip=True)
                    tk = tickers_map.get(nom.lower())
                    if tk:
                        p_w = normalizar_valor(tds[3].get_text(strip=True))
                        p_m = normalizar_valor(tds[4].get_text(strip=True))
                        p_y = normalizar_valor(tds[7].get_text(strip=True))
                        p_d = normalizar_valor(tds[-2].get_text(strip=True))
                        if p_w is not None: data_res["1W"].append({"name": nom, "ticker": tk, "perf": p_w})
                        if p_m is not None: data_res["1M"].append({"name": nom, "ticker": tk, "perf": p_m})
                        if p_y is not None: data_res["1Y"].append({"name": nom, "ticker": tk, "perf": p_y})
                        if not data_res["1D"] and p_d is not None:
                            data_res["1D"].append({"name": nom, "ticker": tk, "perf": p_d})
    except Exception:
        pass

    # Completar con caché previo si algún período no vino en este ciclo
    for k in ["1D", "1W", "1M", "1Y"]:
        if len(data_res[k]) < 8:
            data_res[k] = list(CACHE_FINVIZ_SECTORES.get(k, []))
        else:
            data_res[k].sort(key=lambda x: x["perf"], reverse=True)

    data_res["updated"] = datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
    return data_res


CACHE_ESTADO_MAE: Dict[str, Any] = {"estado": "ABIERTO", "descripcion": "A3 Mercados - Market Data"}

LICITACIONES_MAE_BASE = [
    {"fecha": "2026-11-04", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES YPF S.A. CLASE XXXII - NUEVA"},
    {"fecha": "2026-11-05", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES PAMPA ENERGÍA S.A. CLASE XXIII"},
    {"fecha": "2026-11-06", "horario": "10:00 - 15:00", "titulo": "LETRAS DE TESORERÍA DE LA PROVINCIA DE BUENOS AIRES - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "BONO DEL TESORO NACIONAL CAPITALIZABLE EN PESOS CON VTO. 30/06/2027 (T30J7) - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "BONO DEL TESORO NACIONAL EN PESOS CERO CUPÓN CON AJUSTE POR CER VTO. 30/06/2028 (TZX28) - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "LETRA DEL TESORO NACIONAL CAPITALIZABLE EN PESOS CON VTO. 29/01/2027 (S29E7) - REAPERTURA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "LETRA DEL TESORO NACIONAL EN PESOS A TASA TAMAR CON VTO. 30/04/2027 (M30A7) - NUEVA"},
    {"fecha": "2026-11-11", "horario": "10:00 - 15:00", "titulo": "LETRA DEL TESORO NACIONAL VINCULADA AL DÓLAR ESTADOUNIDENSE CERO CUPÓN CON VTO. 15/01/2027 (D15E7) - REAPERTURA"},
    {"fecha": "2026-11-12", "horario": "10:00 - 16:00", "titulo": "ON BAJO IMPACTO MADERO HARBOUR SERIE II"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 15 (REAPERTURA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 18 (REAPERTURA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 21 (NUEVA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 22 (NUEVA)"},
    {"fecha": "2026-11-12", "horario": "11:00 - 16:00", "titulo": "LETRAS DE TESORERIA DE LA PROVINCIA DEL CHACO CLASE 23 (NUEVA)"},
    {"fecha": "2026-11-13", "horario": "10:00 - 16:00", "titulo": "ON BAJO IMPACTO INGACOT COMERCIAL SERIE I"},
    {"fecha": "2026-11-18", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES VISTA ENERGY ARGENTINA S.A.U. CLASE XXVI"},
    {"fecha": "2026-11-19", "horario": "10:00 - 16:00", "titulo": "OBLIGACIONES NEGOCIABLES TELECOM ARGENTINA S.A. CLASE XXI"},
    {"fecha": "2026-11-25", "horario": "10:00 - 15:00", "titulo": "LICITACIÓN QUINCENAL SECRETARÍA DE FINANZAS — LECAP / BONCAP / CER"}
]

def headers_mae_a3() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Origin": "https://marketdata.mae.com.ar",
        "Referer": "https://marketdata.mae.com.ar/calendario-licitaciones",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site"
    }

def normalizar_item_licitacion(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    f_raw = str(raw.get("fecha") or raw.get("fechaLicitacion") or raw.get("start") or raw.get("date") or raw.get("fechaInicio") or raw.get("dia") or "").strip()
    h_ini = str(raw.get("horaInicio") or raw.get("desde") or raw.get("startTime") or "").strip()
    h_fin = str(raw.get("horaFin") or raw.get("hasta") or raw.get("endTime") or "").strip()
    horario = str(raw.get("horario") or raw.get("hora") or "").strip()

    if not horario:
        if h_ini and h_fin:
            horario = f"{h_ini[:5]} - {h_fin[:5]}"
        elif "T" in f_raw:
            partes_t = f_raw.split("T")
            h_ext = partes_t[1][:5]
            f_end = str(raw.get("end") or raw.get("fechaFin") or "")
            h_end_ext = f_end.split("T")[1][:5] if "T" in f_end else "16:00"
            horario = f"{h_ext} - {h_end_ext}" if h_ext != "00:00" else "10:00 - 15:00"
        else:
            horario = "10:00 - 15:00"

    fecha_iso = ""
    m_iso = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", f_raw)
    if m_iso:
        fecha_iso = f"{m_iso.group(1)}-{int(m_iso.group(2)):02d}-{int(m_iso.group(3)):02d}"
    else:
        m_lat = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](20\d{2})", f_raw)
        if m_lat:
            fecha_iso = f"{m_lat.group(3)}-{int(m_lat.group(2)):02d}-{int(m_lat.group(1)):02d}"

    titulo = str(
        raw.get("titulo") or raw.get("title") or raw.get("descripcion") or raw.get("especie")
        or raw.get("instrumento") or raw.get("nombre") or raw.get("emisor") or raw.get("summary") or ""
    ).strip().upper()

    if not fecha_iso or not titulo:
        return None
    return {"fecha": fecha_iso, "horario": horario, "titulo": titulo}

def fetch_estado_mae() -> Dict[str, Any]:
    global CACHE_ESTADO_MAE
    try:
        r = GLOBAL_HTTP_SESSION.get("https://api.marketdata.mae.com.ar/api/mercado/estado", headers=headers_mae_a3(), timeout=2.5, verify=False)
        if r.status_code == 200 and r.json():
            d = r.json()
            CACHE_ESTADO_MAE = d if isinstance(d, dict) else {"estado": str(d)}
    except Exception:
        pass
    return CACHE_ESTADO_MAE

def fetch_licitaciones_mae_rango(desde: Optional[str] = None, hasta: Optional[str] = None) -> List[Dict[str, Any]]:
    base_url = "https://api.marketdata.mae.com.ar/api/mercado/licitaciones"
    urls_intento = []
    if desde and hasta:
        urls_intento.append(f"{base_url}?fechaDesde={desde}&fechaHasta={hasta}")
    urls_intento.append(base_url)

    extraidos: List[Dict[str, Any]] = []
    for u in urls_intento:
        data = consultar_url_mae_con_puentes(u)
        if data:
            lista_raw = data if isinstance(data, list) else (data.get("licitaciones") or data.get("data") or data.get("items") or data.get("eventos") or [])
            if isinstance(lista_raw, list):
                for item in lista_raw:
                    norm = normalizar_item_licitacion(item)
                    if norm:
                        extraidos.append(norm)
            if extraidos:
                break

    mapa_unico = {}
    for b in LICITACIONES_AGENDA_COMPLETA + CACHE_LICITACIONES_MAE + extraidos:
        norm_b = normalizar_item_licitacion(b) if "horario" not in b else b
        if norm_b and norm_b.get("fecha") and norm_b.get("titulo"):
            clave = f"{norm_b['fecha']}|{norm_b['titulo']}"
            mapa_unico[clave] = norm_b

    resultado = list(mapa_unico.values())
    resultado.sort(key=lambda x: (x["fecha"], x["horario"]))
    return resultado



CALENDARIO_INDEC_ANUAL = [
    # SEPTIEMBRE 2026
    {"fecha": "2026-09-10", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica alimentaria y total. Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-15", "hora": "16:00hs", "publicacion": "Sistema de índices de precios mayoristas (SIPM). Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-16", "hora": "16:00hs", "publicacion": "Informe de avance del nivel de actividad (PIB). Segundo trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-17", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-18", "hora": "16:00hs", "publicacion": "Mercado de trabajo. Tasas e indicadores socioeconómicos (EPH). Segundo trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-24", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Julio de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-26", "hora": "16:00hs", "publicacion": "Incidencia de la pobreza y la indigencia en 31 aglomerados urbanos. Primer semestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-09-30", "hora": "16:00hs", "publicacion": "Índice de salarios. Julio de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    # OCTUBRE 2026
    {"fecha": "2026-10-07", "hora": "16:00hs", "publicacion": "Índice de producción industrial manufacturero (IPI manufacturero). Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-07", "hora": "16:00hs", "publicacion": "Indicadores de coyuntura de la actividad de la construcción (ISAC). Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-14", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica alimentaria y total. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-15", "hora": "16:00hs", "publicacion": "Sistema de índices de precios mayoristas (SIPM). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-15", "hora": "16:00hs", "publicacion": "Índice del costo de la construcción en el Gran Buenos Aires (ICC). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-16", "hora": "16:00hs", "publicacion": "Utilización de la capacidad instalada en la industria. Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-21", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-23", "hora": "16:00hs", "publicacion": "Encuesta de supermercados y autoservicios mayoristas. Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-23", "hora": "16:00hs", "publicacion": "Encuesta de centros de compras. Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-28", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-10-30", "hora": "16:00hs", "publicacion": "Índice de salarios. Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    # NOVIEMBRE 2026
    {"fecha": "2026-11-03", "hora": "16:00hs", "publicacion": "Índices de precios y cantidades del comercio exterior. Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-04", "hora": "16:00hs", "publicacion": "Indicadores de condiciones de vida de los hogares (EPH). Primer semestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-04", "hora": "16:00hs", "publicacion": "Bases de microdatos de la Encuesta Permanente de Hogares. Segundo trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-05", "hora": "16:00hs", "publicacion": "Encuesta Nacional de Empresas (ENE). Resultados preliminares 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-06", "hora": "16:00hs", "publicacion": "Índice de producción industrial minero (IPI minero). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-06", "hora": "16:00hs", "publicacion": "Indicadores de coyuntura de la actividad de la construcción (ISAC). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-06", "hora": "16:00hs", "publicacion": "Índice de producción industrial manufacturero (IPI manufacturero). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-11", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica alimentaria y total. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-12", "hora": "16:00hs", "publicacion": "Utilización de la capacidad instalada en la industria. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-12", "hora": "16:00hs", "publicacion": "Índice de salarios. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-13", "hora": "16:00hs", "publicacion": "Sistema de índices de precios mayoristas (SIPM). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-13", "hora": "16:00hs", "publicacion": "Índice del costo de la construcción en el Gran Buenos Aires (ICC). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-16", "hora": "16:00hs", "publicacion": "Encuesta de turismo internacional (ETI). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-17", "hora": "16:00hs", "publicacion": "Valorización mensual de la canasta de crianza. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-18", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-19", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-24", "hora": "16:00hs", "publicacion": "Encuesta de supermercados y autoservicios mayoristas. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-24", "hora": "16:00hs", "publicacion": "Encuesta de centros de compras. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-25", "hora": "16:00hs", "publicacion": "Generación de energía eléctrica. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-27", "hora": "16:00hs", "publicacion": "Dotación de personal de la administración pública nacional, empresas y sociedades. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-30", "hora": "16:00hs", "publicacion": "Cuentas internacionales de la Argentina. Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    # DICIEMBRE 2026
    {"fecha": "2026-12-03", "hora": "16:00hs", "publicacion": "Índice de producción industrial minero (IPI minero). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-04", "hora": "16:00hs", "publicacion": "Índice de producción industrial manufacturero (IPI manufacturero). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-04", "hora": "16:00hs", "publicacion": "Indicadores de coyuntura de la actividad de la construcción (ISAC). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-10", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica alimentaria y total. Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-11", "hora": "16:00hs", "publicacion": "Utilización de la capacidad instalada en la industria. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-15", "hora": "16:00hs", "publicacion": "Sistema de índices de precios mayoristas (SIPM). Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-15", "hora": "16:00hs", "publicacion": "Índice del costo de la construcción en el Gran Buenos Aires (ICC). Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-16", "hora": "16:00hs", "publicacion": "Informe de avance del nivel de actividad (PIB). Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-17", "hora": "16:00hs", "publicacion": "Mercado de trabajo. Tasas e indicadores socioeconómicos (EPH). Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-18", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-22", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-23", "hora": "16:00hs", "publicacion": "Encuesta de supermercados y centros de compras. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-29", "hora": "16:00hs", "publicacion": "Índice de salarios. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    # ENERO 2027
    {"fecha": "2027-01-07", "hora": "16:00hs", "publicacion": "Índice de producción industrial manufacturero (IPI) e ISAC (Construcción). Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2027-01-13", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica alimentaria y total. Diciembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2027-01-15", "hora": "16:00hs", "publicacion": "Sistema de índices de precios mayoristas (SIPM) e ICC. Diciembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2027-01-20", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Diciembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2027-01-26", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2027-01-29", "hora": "16:00hs", "publicacion": "Índice de salarios. Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"}
]

def parsear_js_indec_completo(js_text: str) -> List[Dict[str, Any]]:
    eventos = []
    # Captura bloques de objetos JS en CalendarioFull.js aunque tengan new Date(y, m, d) o saltos de línea
    bloques = re.findall(r"\{[^{}]*?\}", js_text, re.DOTALL)
    for b in bloques:
        f_iso = None
        # Caso 1: new Date(2026, 10, 11) -> mes en JS es 0-indexed (0=Ene, 10=Nov, 11=Dic)
        m_newdate = re.search(r"new\s+Date\s*\(\s*(20\d{2})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})", b)
        if m_newdate:
            yr = int(m_newdate.group(1))
            mo = int(m_newdate.group(2)) + 1
            dy = int(m_newdate.group(3))
            if 1 <= mo <= 12 and 1 <= dy <= 31:
                f_iso = f"{yr}-{mo:02d}-{dy:02d}"
        if not f_iso:
            # Caso 2: YYYY-MM-DD o YYYY/MM/DD
            m_iso = re.search(r"(?:start|fecha|date|dia|f)\s*[:=]\s*['\"](20\d{2})[-/](\d{1,2})[-/](\d{1,2})", b, re.IGNORECASE)
            if m_iso:
                f_iso = f"{int(m_iso.group(1))}-{int(m_iso.group(2)):02d}-{int(m_iso.group(3)):02d}"
        if not f_iso:
            # Caso 3: DD/MM/YYYY o DD-MM-YYYY
            m_lat = re.search(r"(?:start|fecha|date|dia|f)\s*[:=]\s*['\"](\d{1,2})[-/](\d{1,2})[-/](20\d{2})", b, re.IGNORECASE)
            if m_lat:
                f_iso = f"{int(m_lat.group(3))}-{int(m_lat.group(2)):02d}-{int(m_lat.group(1)):02d}"

        m_tit = re.search(r"(?:title|titulo|descripcion|nombre|text|t)\s*[:=]\s*['\"]([^'\"]{6,})['\"]", b, re.IGNORECASE)
        if f_iso and m_tit:
            pub = BeautifulSoup(m_tit.group(1), "html.parser").get_text(" ", strip=True)
            m_url = re.search(r"(?:url|link|href)\s*[:=]\s*['\"]([^'\"]+)['\"]", b, re.IGNORECASE)
            url_ev = m_url.group(1) if m_url else "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"
            if url_ev.startswith("/"):
                url_ev = "https://www.indec.gob.ar" + url_ev
            eventos.append({"fecha": f_iso, "hora": "16:00hs", "publicacion": pub, "url": url_ev})
    return eventos

CALENDARIO_INDEC_BASE = [
    {"fecha": "2026-10-14", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-5-31"},
    {"fecha": "2026-10-21", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Nivel3-Tema-3-2"},
    {"fecha": "2026-10-28", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Agosto de 2026", "url": "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-48"},
    {"fecha": "2026-11-03", "hora": "16:00hs", "publicacion": "Índices de precios y cantidades del comercio exterior. Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-04", "hora": "16:00hs", "publicacion": "Indicadores de condiciones de vida de los hogares (EPH). Primer semestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-04", "hora": "16:00hs", "publicacion": "Bases de microdatos de la Encuesta Permanente de Hogares. Segundo trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-05", "hora": "16:00hs", "publicacion": "Encuesta Nacional de Empresas (ENE). Resultados preliminares 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-06", "hora": "16:00hs", "publicacion": "Índice de producción industrial minero (IPI minero). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-06", "hora": "16:00hs", "publicacion": "Indicadores de coyuntura de la actividad de la construcción (ISAC). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-06", "hora": "16:00hs", "publicacion": "Índice de producción industrial manufacturero (IPI manufacturero). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-11", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica alimentaria y total. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-12", "hora": "16:00hs", "publicacion": "Utilización de la capacidad instalada en la industria. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-12", "hora": "16:00hs", "publicacion": "Índice de salarios. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-13", "hora": "16:00hs", "publicacion": "Sistema de índices de precios mayoristas (SIPM). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-13", "hora": "16:00hs", "publicacion": "Índice del costo de la construcción en el Gran Buenos Aires (ICC). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-16", "hora": "16:00hs", "publicacion": "Encuesta de turismo internacional (ETI). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-17", "hora": "16:00hs", "publicacion": "Valorización mensual de la canasta de crianza. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-18", "hora": "16:00hs", "publicacion": "Intercambio comercial argentino (ICA). Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-19", "hora": "16:00hs", "publicacion": "Estimador mensual de actividad económica (EMAE). Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-24", "hora": "16:00hs", "publicacion": "Encuesta de supermercados y autoservicios mayoristas. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-24", "hora": "16:00hs", "publicacion": "Encuesta de centros de compras. Septiembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-25", "hora": "16:00hs", "publicacion": "Generación de energía eléctrica. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-27", "hora": "16:00hs", "publicacion": "Dotación de personal de la administración pública nacional, empresas y sociedades. Octubre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-11-30", "hora": "16:00hs", "publicacion": "Cuentas internacionales de la Argentina. Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-10", "hora": "16:00hs", "publicacion": "Índice de precios al consumidor (IPC) y Canasta básica. Noviembre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-16", "hora": "16:00hs", "publicacion": "Informe de avance del nivel de actividad (PIB). Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"},
    {"fecha": "2026-12-17", "hora": "16:00hs", "publicacion": "Mercado de trabajo. Tasas e indicadores socioeconómicos (EPH). Tercer trimestre de 2026", "url": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"}
]

CALENDARIO_USA_BASE = [
    {"fecha": "2026-11-16", "hora": "14:00", "div": "US", "evento": "Declaraciones de Warsh, del Consejo de Gobierno de la Fed", "importancia": 3, "actual": "-", "prevision": "-", "anterior": "-"},
    {"fecha": "2026-11-17", "hora": "10:30", "div": "US", "evento": "Ventas minoristas (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,3%", "anterior": "0,4%"},
    {"fecha": "2026-11-17", "hora": "10:30", "div": "US", "evento": "Ventas minoristas subyacentes (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,3%", "anterior": "0,5%"},
    {"fecha": "2026-11-17", "hora": "11:15", "div": "US", "evento": "Producción industrial (Mensual) (Oct)", "importancia": 2, "actual": "-", "prevision": "-0,2%", "anterior": "-0,3%"},
    {"fecha": "2026-11-17", "hora": "12:00", "div": "US", "evento": "Índice NAHB del mercado de la vivienda (Nov)", "importancia": 2, "actual": "-", "prevision": "43", "anterior": "43"},
    {"fecha": "2026-11-18", "hora": "09:00", "div": "US", "evento": "Solicitudes de hipoteca MBA (Semanal)", "importancia": 2, "actual": "-", "prevision": "-", "anterior": "0,5%"},
    {"fecha": "2026-11-18", "hora": "10:30", "div": "US", "evento": "Permisos de construcción (Oct)", "importancia": 3, "actual": "-", "prevision": "1,440M", "anterior": "1,425M"},
    {"fecha": "2026-11-18", "hora": "10:30", "div": "US", "evento": "Inicios de construcción de viviendas (Oct)", "importancia": 2, "actual": "-", "prevision": "1,340M", "anterior": "1,354M"},
    {"fecha": "2026-11-18", "hora": "12:30", "div": "US", "evento": "Inventarios de petróleo crudo de la AIE", "importancia": 3, "actual": "-", "prevision": "1,200M", "anterior": "2,149M"},
    {"fecha": "2026-11-19", "hora": "10:30", "div": "US", "evento": "Nuevas peticiones de subsidio por desempleo", "importancia": 3, "actual": "-", "prevision": "222K", "anterior": "220K"},
    {"fecha": "2026-11-19", "hora": "10:30", "div": "US", "evento": "Índice manufacturero de la Fed de Filadelfia (Nov)", "importancia": 3, "actual": "-", "prevision": "8,0", "anterior": "10,3"},
    {"fecha": "2026-11-19", "hora": "12:00", "div": "US", "evento": "Ventas de viviendas de segunda mano (Oct)", "importancia": 3, "actual": "-", "prevision": "3,93M", "anterior": "3,84M"},
    {"fecha": "2026-11-20", "hora": "11:45", "div": "US", "evento": "PMI manufacturero de S&P Global (Nov) (Preliminar)", "importancia": 3, "actual": "-", "prevision": "48,8", "anterior": "48,5"},
    {"fecha": "2026-11-20", "hora": "11:45", "div": "US", "evento": "PMI de servicios de S&P Global (Nov) (Preliminar)", "importancia": 3, "actual": "-", "prevision": "55,2", "anterior": "55,0"},
    {"fecha": "2026-11-20", "hora": "12:00", "div": "US", "evento": "Confianza del consumidor de la Universidad de Michigan (Nov)", "importancia": 3, "actual": "-", "prevision": "73,0", "anterior": "70,5"},
    {"fecha": "2026-11-24", "hora": "12:00", "div": "US", "evento": "Confianza del consumidor de The Conference Board (Nov)", "importancia": 3, "actual": "-", "prevision": "111,8", "anterior": "108,7"},
    {"fecha": "2026-11-24", "hora": "12:00", "div": "US", "evento": "Ventas de viviendas nuevas (Oct)", "importancia": 3, "actual": "-", "prevision": "725K", "anterior": "738K"},
    {"fecha": "2026-11-25", "hora": "10:30", "div": "US", "evento": "PIB (Trimestral) (3T) (Segunda estimación)", "importancia": 3, "actual": "-", "prevision": "2,8%", "anterior": "2,8%"},
    {"fecha": "2026-11-25", "hora": "10:30", "div": "US", "evento": "Pedidos de bienes duraderos (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,5%", "anterior": "-0,7%"},
    {"fecha": "2026-11-25", "hora": "12:00", "div": "US", "evento": "Índice de precios PCE subyacente (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,3%", "anterior": "0,3%"},
    {"fecha": "2026-11-25", "hora": "16:00", "div": "US", "evento": "Actas de la reunión del FOMC de la Reserva Federal", "importancia": 3, "actual": "-", "prevision": "-", "anterior": "-"}
]

CACHE_CALENDARIO_INDEC: List[Dict[str, Any]] = list(CALENDARIO_INDEC_BASE)
CACHE_CALENDARIO_USA: List[Dict[str, Any]] = list(CALENDARIO_USA_BASE)

def extraer_calendario_indec() -> List[Dict[str, Any]]:
    global CACHE_CALENDARIO_INDEC
    url_js = "https://www.indec.gob.ar/Scripts/CalendarioFull.js"
    extraidos = []
    sess = MAE_HTTP_SESSION
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "https://www.indec.gob.ar/indec/web/Calendario-Fecha-0"
    }
    for u in [
        url_js,
        f"https://api.allorigins.win/raw?url={quote(url_js, safe='')}",
        f"https://api.codetabs.com/v1/proxy?quest={quote(url_js, safe='')}",
        f"https://corsproxy.io/?{quote(url_js, safe='')}"
    ]:
        try:
            r = sess.get(u, headers=hdrs, timeout=2.5, verify=False)
            if r.status_code == 200 and len(r.text) > 100:
                extraidos = parsear_js_indec_completo(r.text)
                if len(extraidos) > 5:
                    break
        except Exception:
            pass

    mapa = {}
    for item in CALENDARIO_INDEC_ANUAL + CALENDARIO_INDEC_BASE + CACHE_CALENDARIO_INDEC + extraidos:
        if item.get("fecha") and item.get("publicacion"):
            mapa[f"{item['fecha']}|{item['publicacion']}"] = item
    res = sorted(list(mapa.values()), key=lambda x: x["fecha"])
    CACHE_CALENDARIO_INDEC = res
    return res

TRADUCCIONES_EVENTOS_USA = {
    "Core CPI m/m": "IPC subyacente (Mensual)",
    "CPI m/m": "IPC (Mensual)",
    "CPI y/y": "IPC (Anual)",
    "Core PPI m/m": "IPP subyacente (Mensual)",
    "PPI m/m": "IPP (Mensual)",
    "Unemployment Claims": "Nuevas peticiones de subsidio por desempleo",
    "Core Retail Sales m/m": "Ventas minoristas subyacentes (Mensual)",
    "Retail Sales m/m": "Ventas minoristas (Mensual)",
    "Empire State Manufacturing Index": "Índice manufacturero Empire State",
    "Industrial Production m/m": "Producción industrial (Mensual)",
    "Fed Chair Powell Speaks": "Comparecencia de Jerome Powell, presidente de la Fed",
    "Crude Oil Inventories": "Inventarios de petróleo crudo de la AIE",
    "Philly Fed Manufacturing Index": "Índice manufacturero de la Fed de Filadelfia",
    "Existing Home Sales": "Ventas de viviendas de segunda mano",
    "Flash Manufacturing PMI": "PMI manufacturero de S&P Global (Preliminar)",
    "Flash Services PMI": "PMI de servicios de S&P Global (Preliminar)",
    "Revised UoM Consumer Sentiment": "Confianza del consumidor de la Universidad de Michigan",
    "Prelim UoM Consumer Sentiment": "Confianza del consumidor de la Universidad de Michigan",
    "Building Permits": "Permisos de construcción",
    "Housing Starts": "Inicios de construcción de viviendas",
    "10-y Bond Auction": "Subasta de bonos del Tesoro a 10 años (UST 10Y)",
    "30-y Bond Auction": "Subasta de bonos del Tesoro a 30 años (UST 30Y)",
    "Federal Budget Balance": "Balance presupuestario federal",
    "Prelim GDP q/q": "PIB (Trimestral) (Segunda estimación)",
    "Core PCE Price Index m/m": "Índice de precios PCE subyacente (Mensual)",
    "FOMC Meeting Minutes": "Actas de la reunión del FOMC de la Reserva Federal"
}

def extraer_calendario_usa() -> List[Dict[str, Any]]:
    global CACHE_CALENDARIO_USA
    from datetime import timezone, timedelta
    tz_ar = timezone(timedelta(hours=-3))
    ahora_ar = datetime.now(tz_ar)
    d_desde = (ahora_ar - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    d_hasta = (ahora_ar + timedelta(days=14)).strftime("%Y-%m-%dT23:59:59Z")
    ts_now = int(time.time() * 1000)
    extraidos = []

    hdrs_std = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9"
    }

    # Fuente 1: FXStreet Calendar API en español (multi-semana abierta sin Cloudflare)
    try:
        url_fxs = f"https://calendar-api.fxstreet.com/en/api/v1/eventDates/{d_desde}/{d_hasta}?&volatilities=LOW&volatilities=MEDIUM&volatilities=HIGH&countries=US&rows=250"
        r_fxs = GLOBAL_HTTP_SESSION.get(url_fxs, headers={**hdrs_std, "Origin": "https://www.fxstreet.es", "Referer": "https://www.fxstreet.es/calendario-economico"}, timeout=5.0)
        if r_fxs.status_code == 200:
            lista_fxs = r_fxs.json()
            if isinstance(lista_fxs, list):
                for ev in lista_fxs:
                    dt_str = str(ev.get("dateUtc") or "")
                    if not dt_str:
                        continue
                    try:
                        dt_ar = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(tz_ar)
                        f_iso = dt_ar.strftime("%Y-%m-%d")
                        hora = dt_ar.strftime("%H:%M")
                    except Exception:
                        continue
                    nom_raw = str(ev.get("name") or "").strip()
                    if not nom_raw:
                        continue
                    nom = TRADUCCIONES_EVENTOS_USA.get(nom_raw, nom_raw)
                    vol = str(ev.get("volatility") or "MEDIUM").upper()
                    imp = 3 if vol == "HIGH" else (1 if vol == "LOW" else 2)
                    un = str(ev.get("unit") or "")
                    def _fv(v):
                        if v is None or v == "":
                            return "-"
                        s = str(v).replace(".", ",")
                        return s + un if un and un not in s else s
                    extraidos.append({
                        "fecha": f_iso,
                        "hora": hora,
                        "div": "US",
                        "evento": nom,
                        "importancia": imp,
                        "actual": _fv(ev.get("actual")),
                        "prevision": _fv(ev.get("consensus")),
                        "anterior": _fv(ev.get("previous"))
                    })
    except Exception:
        pass

    # Fuente 2: Investing AWS en español para los eventos y resultados en vivo de hoy
    try:
        r_aws = GLOBAL_HTTP_SESSION.get(
            f"https://e1-0850197d5e074be58352ddc860645af6.ecs.us-east-1.on.aws/events?cee=no&lang=es&_t={ts_now}",
            headers={**hdrs_std, "Origin": "https://es.investing.com", "Referer": "https://es.investing.com/economic-calendar/"},
            timeout=4.0
        )
        if r_aws.status_code == 200:
            data = r_aws.json()
            lista = data if isinstance(data, list) else (data.get("events") or data.get("data") or [])
            for ev in lista:
                cur = str(ev.get("country") or ev.get("currency") or "").upper()
                if cur not in ("USD", "US"):
                    continue
                f_raw = str(ev.get("occurrence_time") or ev.get("date") or "")
                try:
                    dt_ar = datetime.fromisoformat(f_raw.replace("Z", "+00:00")).astimezone(tz_ar)
                    f_iso = dt_ar.strftime("%Y-%m-%d")
                    hora = dt_ar.strftime("%H:%M")
                except Exception:
                    continue
                nom = str(ev.get("title") or ev.get("event") or ev.get("name") or "").strip()
                if not nom:
                    continue
                imp_raw = str(ev.get("importance") or ev.get("impact") or "2").lower()
                imp = 3 if imp_raw in ("3", "high") else (1 if imp_raw in ("1", "low") else 2)
                extraidos.append({
                    "fecha": f_iso,
                    "hora": hora,
                    "div": "US",
                    "evento": nom,
                    "importancia": imp,
                    "actual": str(ev.get("actual") if ev.get("actual") not in (None, "") else "-").replace(".", ","),
                    "prevision": str(ev.get("forecast") or ev.get("prevision") or "-").replace(".", ","),
                    "anterior": str(ev.get("previous") or ev.get("anterior") or "-").replace(".", ",")
                })
    except Exception:
        pass

    mapa = {}
    for item in CALENDARIO_USA_BASE + CACHE_CALENDARIO_USA + extraidos:
        if item.get("fecha") and item.get("evento"):
            clave = f"{item['fecha']}|{item['hora']}|{item['evento']}"
            if clave in mapa and item.get("actual") in ("-", "", None) and mapa[clave].get("actual") not in ("-", "", None):
                continue
            mapa[clave] = item

    res = sorted(list(mapa.values()), key=lambda x: (x["fecha"], x["hora"]))
    CACHE_CALENDARIO_USA = res
    return res

def fetch_licitaciones_mae() -> List[Dict[str, Any]]:
    fetch_estado_mae()
    return fetch_licitaciones_mae_rango()

def _safe_res(fut, default):
    if fut is None:
        return default
    try:
        return fut.result(timeout=18)
    except Exception:
        return default

def worker_refresco_continuo():
    global CACHE_VARIABLES, CACHE_CURVAS_SJB, CACHE_INFLACION_IMPLICITA, CACHE_BANDAS_CAMBIARIAS, CACHE_LICITACIONES_MAE, CACHE_FINVIZ_SECTORES, ULTIMA_ACTUALIZACION

    ciclo_n = 0
    while True:
        try:
            es_ciclo_lento = (ciclo_n % 15 == 0)
            with ThreadPoolExecutor(max_workers=16) as executor:
                f_ind = executor.submit(obtener_datos_indicadores)
                f_acc_rava = executor.submit(obtener_acciones_rava_en_vivo)
                f_bcra_inf = executor.submit(obtener_inflacion_oficial_bcra) if es_ciclo_lento else None
                f_tasas = executor.submit(obtener_datos_tasas_sjb)
                f_monedas = executor.submit(obtener_datos_monedas_y_dolar)
                f_bonos = executor.submit(obtener_datos_bonos_sjb)
                f_usa = executor.submit(obtener_datos_mercado_usa)
                f_inf_imp = executor.submit(fetch_inflacion_implicita_sjb)
                f_bandas = executor.submit(fetch_bandas_dolarito)
                f_futuro = executor.submit(obtener_datos_dolar_futuro)
                f_metales = executor.submit(obtener_datos_metales_en_vivo)
                f_petroleo = executor.submit(obtener_datos_petroleo_en_vivo)
                f_licit = executor.submit(fetch_licitaciones_mae) if es_ciclo_lento else None
                f_ust = executor.submit(fetch_curva_us_treasuries)
                f_cal_usa = executor.submit(extraer_calendario_usa) if es_ciclo_lento else None

                futures_curvas = [executor.submit(fetch_una_curva_sjb, k, url) for k, url in SJB_CURVAS_URLS.items()]

                res_ind = _safe_res(f_ind, [])
                _safe_res(f_acc_rava, None)
                res_bcra_inf = _safe_res(f_bcra_inf, {})
                res_tasas = _safe_res(f_tasas, list(CACHE_TASAS_PERSISTENTE.values()))
                res_monedas = _safe_res(f_monedas, [])
                res_bonos = _safe_res(f_bonos, [])
                res_usa = _safe_res(f_usa, [])
                res_inf_imp = _safe_res(f_inf_imp, None)
                res_bandas = _safe_res(f_bandas, None)
                res_futuro = _safe_res(f_futuro, None)
                res_metales = _safe_res(f_metales, [])
                res_petroleo = _safe_res(f_petroleo, [])
                res_licit = _safe_res(f_licit, None)
                res_ust = _safe_res(f_ust, None)
                _safe_res(f_cal_usa, None)

                if res_ust:
                    CACHE_CURVAS_SJB['us_treasuries'] = res_ust

                for fut in futures_curvas:
                    par = _safe_res(fut, (None, None))
                    if par and par[0] and par[1]:
                        CACHE_CURVAS_SJB[par[0]] = par[1]
                res_inf_fresca = fetch_inflacion_implicita_sjb()
                if res_inf_fresca:
                    res_inf_imp = res_inf_fresca

            res_finviz = fetch_finviz_sectores(res_usa) if (es_ciclo_lento or not CACHE_FINVIZ_SECTORES.get("1D")) else None
            ciclo_n += 1

            if res_inf_imp: CACHE_INFLACION_IMPLICITA = res_inf_imp
            if res_bandas: CACHE_BANDAS_CAMBIARIAS = res_bandas
            if res_licit is not None: CACHE_LICITACIONES_MAE = res_licit
            if res_finviz: CACHE_FINVIZ_SECTORES = res_finviz

            indices_map = {item["variable"]: dict(item) for item in INDICES_BASE_MEMORIA}
            for item in res_usa:
                indices_map[item["variable"]] = item
            for item in res_ind:
                if item.get("categoria") == "indices" and item.get("variable") not in ("MERVAL_ARS", "MERVAL_USD", "RIESGO_PAIS"):
                    indices_map[item["variable"]] = item

            ccl_cache_obj = (_DOLARITO_CICLO_CACHE.get("data") or {}).get("DOLAR_CCL") or BASE_DOLARITO_EXACTA.get("DOLAR_CCL") or {}
            ccl_actual = float(ccl_cache_obj.get("venta") or 1749.47)
            msci_vivo = obtener_msci_merval_en_vivo(ccl_actual)
            indices_map["MERVAL_ARS"] = msci_vivo["MERVAL_ARS"]
            indices_map["MERVAL_USD"] = msci_vivo["MERVAL_USD"]
            indices_map["RIESGO_PAIS"] = msci_vivo["RIESGO_PAIS"]

            nuevo_agrupado = {
                "tasas_y_monetario": [],
                "dolar_y_cambiario": [],
                "monedas": [],
                "cripto": [],
                "cotizacion_bonos": res_bonos,
                "indices": list(indices_map.values()),
                "inflacion_y_ajuste": [],
                "metales": res_metales if res_metales else [],
                "petroleo": res_petroleo if res_petroleo else [],
                "macro_y_fiscal": [],
                "micro_y_sectores": [],
                "commodities_agro_energia": []
            }

            for item in res_monedas:
                if item.get("categoria") == "dolar_y_cambiario":
                    nuevo_agrupado["dolar_y_cambiario"].append(item)
                else:
                    nuevo_agrupado["monedas"].append(item)

            for item in res_ind:
                cat = item.get("categoria", "macro_y_fiscal")
                if cat != "indices" and cat in nuevo_agrupado:
                    idx_exist = next((i for i, x in enumerate(nuevo_agrupado[cat]) if x.get("variable") == item.get("variable")), None)
                    if idx_exist is not None:
                        existente = nuevo_agrupado[cat][idx_exist]
                        if item.get("variacion") and existente.get("variacion") in ("0,0%", "0.0%", None):
                            existente["variacion"] = item["variacion"]
                    else:
                        nuevo_agrupado[cat].append(item)

            if res_bcra_inf:
                for cod_inf, obj_inf in res_bcra_inf.items():
                    reemplazado = False
                    for idx_i, existente in enumerate(nuevo_agrupado["inflacion_y_ajuste"]):
                        if existente.get("variable") == cod_inf:
                            nuevo_agrupado["inflacion_y_ajuste"][idx_i] = obj_inf
                            reemplazado = True
                            break
                    if not reemplazado:
                        nuevo_agrupado["inflacion_y_ajuste"].append(obj_inf)

            orden_inflacion = ["INFLACION_MENSUAL", "INFLACION_INTERANUAL", "INFLACION_REM", "INDICE_CER", "INDICE_UVA", "INDICE_ICL"]
            nuevo_agrupado["inflacion_y_ajuste"].sort(
                key=lambda x: orden_inflacion.index(x.get("variable")) if x.get("variable") in orden_inflacion else 99
            )

            mapa_tasas = {t["variable"]: t for t in res_tasas}
            for k_pers, obj_pers in CACHE_TASAS_PERSISTENTE.items():
                if k_pers not in mapa_tasas:
                    mapa_tasas[k_pers] = obj_pers

            existentes_tasas = {x.get("variable") for x in nuevo_agrupado["tasas_y_monetario"]}
            for k_t, item_t in mapa_tasas.items():
                if k_t not in existentes_tasas:
                    nuevo_agrupado["tasas_y_monetario"].append(item_t)

            if res_futuro:
                nuevo_agrupado["dolar_y_cambiario"].append(res_futuro)

            puntas_dl = obtener_puntas_dolarito()
            definicion_9_dolares = [
                ("DOLAR_OFICIAL", "Dólar Oficial"),
                ("DOLAR_BLUE", "Dólar Blue"),
                ("DOLAR_NETFLIX", "Dólar Netflix"),
                ("DOLAR_MEP", "Dólar MEP"),
                ("DOLAR_CCL", "Dólar CCL"),
                ("DOLAR_CRIPTO", "Dólar Cripto"),
                ("DOLAR_TARJETA", "Dólar Tarjeta"),
                ("DOLAR_A3500", "Dólar Mayorista"),
                ("DOLAR_FUTURO", "Dólar Futuro")
            ]
            lista_dolar_limpia = []
            for cod_p, desc_p in definicion_9_dolares:
                info_p = puntas_dl.get(cod_p) or BASE_DOLARITO_EXACTA[cod_p]
                c_v = info_p.get("compra")
                v_v = float(info_p.get("venta") or 0)
                var_v = info_p.get("variacion") or "0,00%"
                fmt_v = f"${v_v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                lista_dolar_limpia.append({
                    "variable": cod_p,
                    "descripcion": desc_p,
                    "categoria": "dolar_y_cambiario",
                    "compra": c_v,
                    "venta": v_v,
                    "valor": v_v,
                    "valor_formateado": fmt_v,
                    "variacion": var_v,
                    "var_anio": info_p.get("var_anio"),
                    "fecha": info_p.get("fecha", "En vivo")
                })
            nuevo_agrupado["dolar_y_cambiario"] = lista_dolar_limpia
            tc_agro = CACHE_BANDAS_CAMBIARIAS.get("dolar_mayorista", 1545.50)
            for it_d in nuevo_agrupado["dolar_y_cambiario"]:
                if it_d.get("variable") == "DOLAR_A3500" and it_d.get("valor", 0) > 800:
                    tc_agro = float(it_d["valor"])
                    break
            agro_vivo = obtener_agro_en_vivo(tc_agro)
            existentes_agro = [x for x in nuevo_agrupado["commodities_agro_energia"] if x.get("variable") not in agro_vivo]
            nuevo_agrupado["commodities_agro_energia"] = list(agro_vivo.values()) + existentes_agro

            with CACHE_LOCK:
                for k, v in nuevo_agrupado.items():
                    if len(v) > 0:
                        CACHE_VARIABLES[k] = v
                ULTIMA_ACTUALIZACION = datetime.utcnow().strftime("%H:%M:%S UTC")

        except Exception as e:
            print(f"[Worker Error] {e}")

        time.sleep(8)

hilo_worker = threading.Thread(target=worker_refresco_continuo, daemon=True)
hilo_worker.start()

@app.get("/")
def inicio():
    if os.path.exists("static/index.html"):
        return FileResponse(
            "static/index.html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}
        )
    return {"mensaje": "Servidor activo", "ultima_actualizacion": ULTIMA_ACTUALIZACION}

@app.get("/api/salud")
def ver_salud_sincronizacion():
    with CACHE_LOCK:
        conteos = {k: len(v) for k, v in CACHE_VARIABLES.items()}
        rp_obj = next((x for x in CACHE_VARIABLES.get("indices", []) if x.get("variable") == "RIESGO_PAIS"), {})
        mep_obj = next((x for x in CACHE_VARIABLES.get("dolar_y_cambiario", []) if x.get("variable") == "DOLAR_MEP"), {})
        pf_obj = next((x for x in CACHE_VARIABLES.get("tasas_y_monetario", []) if x.get("variable") == "TASA_PLAZO_FIJO"), {})
        soja_obj = next((x for x in CACHE_VARIABLES.get("commodities_agro_energia", []) if x.get("variable") == "AGRO_SOJA"), {})
        return {
            "ok": bool(ULTIMA_ACTUALIZACION),
            "ultima_actualizacion": ULTIMA_ACTUALIZACION or "Iniciando...",
            "conteos": conteos,
            "muestras": {
                "riesgo_pais": rp_obj.get("valor_formateado"),
                "dolar_mep": mep_obj.get("valor_formateado"),
                "plazo_fijo": pf_obj.get("valor_formateado"),
                "soja_rosario": soja_obj.get("precio_ars")
            }
        }

@app.get("/bcra/variables")
def ver_bcra(formato: str = Query("agrupado")):
    with CACHE_LOCK:
        if formato == "lista":
            lista_plana = []
            for v in CACHE_VARIABLES.values():
                lista_plana.extend(v)
            return lista_plana
        return CACHE_VARIABLES

@app.get("/api/acciones/argentina")
def ver_acciones_argentina():
    with CACHE_LOCK:
        return CACHE_ACCIONES_ARGENTINA

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

@app.get("/api/riesgo-pais")
def ver_riesgo_pais_vivo():
    msci = obtener_msci_merval_en_vivo()
    rp = msci.get("RIESGO_PAIS", {})
    with CACHE_LOCK:
        idx_list = CACHE_VARIABLES.get("indices", [])
        for i, item in enumerate(idx_list):
            if item.get("variable") == "RIESGO_PAIS":
                idx_list[i] = rp
                break
    return rp

@app.get("/api/dolar/dolarito")
def ver_puntas_dolarito():
    return obtener_puntas_dolarito()

@app.get("/api/dolar/bandas")
def ver_bandas():
    with CACHE_LOCK:
        return CACHE_BANDAS_CAMBIARIAS

@app.get("/api/licitaciones/mae")
def ver_licitaciones_mae(desde: Optional[str] = Query(None), hasta: Optional[str] = Query(None)):
    datos = fetch_licitaciones_mae_rango(desde, hasta)
    with CACHE_LOCK:
        if datos:
            CACHE_LICITACIONES_MAE.clear()
            CACHE_LICITACIONES_MAE.extend(datos)
        return {"licitaciones": datos, "estado_mercado": CACHE_ESTADO_MAE, "updated": ULTIMA_ACTUALIZACION}

@app.get("/api/mercado/estado")
def ver_estado_mae():
    return fetch_estado_mae()




CACHE_ACEPTACION_GOBIERNO: Dict[str, Any] = {
    "icg": {
        "mes_label": "Nov 2026",
        "mes_mandato": 35,
        "valor": 2.04,
        "valor_formateado": "2,04 pts",
        "variacion_mensual": "+4,1%",
        "variacion_interanual": "-17,5%",
        "comparativa_anteriores": [
            {"presidente": "Mauricio Macri", "periodo": "Nov 2018 (Mes 35)", "valor": 1.77, "valor_formateado": "1,77 pts", "diferencia_pct": 15.2, "diferencia_formateada": "+15,2%", "estado": "Más alto"},
            {"presidente": "Alberto Fernández", "periodo": "Nov 2022 (Mes 35)", "valor": 1.19, "valor_formateado": "1,19 pts", "diferencia_pct": 71.1, "diferencia_formateada": "+71,1%", "estado": "Más alto"},
            {"presidente": "Cristina Fernández (II)", "periodo": "Nov 2014 (Mes 35)", "valor": 1.76, "valor_formateado": "1,76 pts", "diferencia_pct": 15.9, "diferencia_formateada": "+15,9%", "estado": "Más alto"}
        ],
        "historico_3_gobiernos": {
            "icg_promedio": 1.57,
            "icc_promedio": 40.68,
            "riesgo_pais_promedio": 1298.0,
            "emae_ia_promedio": -0.9,
            "inflacion_ia_promedio": 59.2,
            "desempleo_promedio": 8.1
        },
        "componentes": [
            {"nombre": "Honestidad de los funcionarios", "valor": 2.41, "variacion": "+1,4%"},
            {"nombre": "Capacidad para resolver problemas", "valor": 2.40, "variacion": "+5,6%"},
            {"nombre": "Eficiencia en el gasto público", "valor": 2.08, "variacion": "+4,5%"},
            {"nombre": "Evaluación general del gobierno", "valor": 1.69, "variacion": "+3,6%"},
            {"nombre": "Preocupación por el interés general", "valor": 1.60, "variacion": "+6,4%"}
        ]
    },
    "icc": {
        "mes_label": "Nov 2026",
        "valor": 40.99,
        "valor_formateado": "40,99 pts",
        "variacion_mensual": "-1,27%",
        "variacion_interanual": "-10,98%"
    }
}

_UTDT_CACHE_TS = 0.0
_UTDT_LOCK = threading.Lock()

def actualizar_aceptacion_utdt() -> Dict[str, Any]:
    global _UTDT_CACHE_TS, CACHE_ACEPTACION_GOBIERNO
    now = time.time()
    if now - _UTDT_CACHE_TS < 21600.0:
        return CACHE_ACEPTACION_GOBIERNO
    if not _UTDT_LOCK.acquire(blocking=False):
        return CACHE_ACEPTACION_GOBIERNO
    try:
        hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        r_icg = GLOBAL_HTTP_SESSION.get("https://www.utdt.edu/ver_contenido.php?id_contenido=1439&id_item_menu=2964", headers=hdrs, timeout=5.0, verify=False)
        if r_icg.status_code == 200 and r_icg.text:
            plano = BeautifulSoup(r_icg.text, "html.parser").get_text(" ", strip=True)
            m_icg = re.search(
                r"El ICG de ([a-záéíóú]+) fue de (\d+,\d+) puntos,\s*nivel que representa (?:un aumento|una disminuci[oó]n|una ca[ií]da) del (\d+,\d+)% respecto del mes anterior\.\s*El nivel de confianza en el mes (\d+) de la presidencia de Javier Milei es (\d+,\d+)% (mayor|menor) que el de la presidencia de Mauricio Macri,\s*y (\d+,\d+)% (mayor|menor) que el de la de Alberto Fern[aá]ndez",
                plano,
                re.IGNORECASE
            )
            if m_icg:
                mes_nom = m_icg.group(1).capitalize()
                val_icg = normalizar_valor(m_icg.group(2))
                es_baja = "disminuci" in m_icg.group(0).lower() or "ca" in m_icg.group(0).lower()
                var_m_str = f"{'-' if es_baja else '+'}{m_icg.group(3)}%"
                mes_mandato = int(m_icg.group(4))
                pct_macri = normalizar_valor(m_icg.group(5))
                dir_macri = m_icg.group(6).lower()
                pct_alberto = normalizar_valor(m_icg.group(7))
                dir_alberto = m_icg.group(8).lower()
                if val_icg and pct_macri is not None and pct_alberto is not None:
                    s_macri = pct_macri if dir_macri == "mayor" else -pct_macri
                    s_alberto = pct_alberto if dir_alberto == "mayor" else -pct_alberto
                    v_macri = round(val_icg / (1.0 + s_macri / 100.0), 2)
                    v_alberto = round(val_icg / (1.0 + s_alberto / 100.0), 2)
                    v_cfk = 1.76
                    s_cfk = round(((val_icg / v_cfk) - 1.0) * 100.0, 1)
                    CACHE_ACEPTACION_GOBIERNO["icg"].update({
                        "mes_label": f"{mes_nom[:3]} 2026",
                        "mes_mandato": mes_mandato,
                        "valor": val_icg,
                        "valor_formateado": f"{m_icg.group(2)} pts",
                        "variacion_mensual": var_m_str,
                        "comparativa_anteriores": [
                            {"presidente": "Mauricio Macri", "periodo": f"Mes {mes_mandato}", "valor": v_macri, "valor_formateado": f"{v_macri:.2f} pts".replace(".", ","), "diferencia_pct": s_macri, "diferencia_formateada": f"{'+' if s_macri >= 0 else ''}{s_macri:.1f}%".replace(".", ","), "estado": "Más alto" if s_macri >= 0 else "Más bajo"},
                            {"presidente": "Alberto Fernández", "periodo": f"Mes {mes_mandato}", "valor": v_alberto, "valor_formateado": f"{v_alberto:.2f} pts".replace(".", ","), "diferencia_pct": s_alberto, "diferencia_formateada": f"{'+' if s_alberto >= 0 else ''}{s_alberto:.1f}%".replace(".", ","), "estado": "Más alto" if s_alberto >= 0 else "Más bajo"},
                            {"presidente": "Cristina Fernández (II)", "periodo": f"Mes {mes_mandato}", "valor": v_cfk, "valor_formateado": f"{v_cfk:.2f} pts".replace(".", ","), "diferencia_pct": s_cfk, "diferencia_formateada": f"{'+' if s_cfk >= 0 else ''}{s_cfk:.1f}%".replace(".", ","), "estado": "Más alto" if s_cfk >= 0 else "Más bajo"}
                        ]
                    })
        r_icc = GLOBAL_HTTP_SESSION.get("https://www.utdt.edu/ver_contenido.php?id_contenido=2575&id_item_menu=4982", headers=hdrs, timeout=5.0, verify=False)
        if r_icc.status_code == 200 and r_icc.text:
            plano_icc = BeautifulSoup(r_icc.text, "html.parser").get_text(" ", strip=True)
            m_icc = re.search(r"([A-ZÁÉÍÓÚ][a-záéíóú]+ 20\d{2})\s+LA CONFIANZA DE LOS CONSUMIDORES (SUBE|CAE|BAJA) (\d+(?:,\d+)?)%", plano_icc)
            if m_icc:
                mes_icc = m_icc.group(1)
                sube_icc = m_icc.group(2).upper() == "SUBE"
                CACHE_ACEPTACION_GOBIERNO["icc"]["mes_label"] = mes_icc
                CACHE_ACEPTACION_GOBIERNO["icc"]["variacion_mensual"] = f"{'+' if sube_icc else '-'}{m_icc.group(3)}%"
        _UTDT_CACHE_TS = time.time()
    except Exception:
        _UTDT_CACHE_TS = time.time() - 18000.0
    finally:
        _UTDT_LOCK.release()
    return CACHE_ACEPTACION_GOBIERNO

@app.get("/api/aceptacion-gobierno")
def ver_aceptacion_gobierno():
    datos_utdt = actualizar_aceptacion_utdt()
    with CACHE_LOCK:
        rp_obj = next((x for x in CACHE_VARIABLES.get("indices", []) if x.get("variable") == "RIESGO_PAIS"), None)
        emae_obj = next((x for x in CACHE_VARIABLES.get("macro_y_fiscal", []) if x.get("variable") == "EMAE_ACTIVIDAD"), None)
        inf_ia_obj = next((x for x in CACHE_VARIABLES.get("inflacion_y_ajuste", []) if x.get("variable") == "INFLACION_INTERANUAL"), None)
        desempleo_obj = next((x for x in CACHE_VARIABLES.get("macro_y_fiscal", []) if x.get("variable") == "DESEMPLEO"), None)
        return {
            "riesgo_pais": rp_obj,
            "emae": emae_obj,
            "inflacion_interanual": inf_ia_obj,
            "desempleo": desempleo_obj,
            "icg": datos_utdt.get("icg"),
            "icc": datos_utdt.get("icc"),
            "updated": ULTIMA_ACTUALIZACION
        }







CACHE_NOTICIAS_DRIVERS: Dict[str, Any] = {
    "top5": [
        {
            "titulo": "Wall Street rebota con fuerza: Nasdaq +6,51% y S&P 500 +4,73% impulsados por el rally de Nvidia (+31,48%) tras el regreso de Jensen Huang",
            "url": "https://www.ambito.com/finanzas/wall-street-rebota-67-el-nasdaq-al-frente-el-nuevo-nombramiento-jensen-huang-nvidia-n6348272",
            "fuente": "Wall Street • Tech & IA",
            "mercado": "EE.UU.",
            "impacto": "Alcista Tech / Riesgo",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Dólar en Argentina: el oficial se ubica en $1.600, el MEP en $1.696,42 y el CCL en $1.851,48 con foco en reservas del BCRA",
            "url": "https://tn.com.ar/economia/2026/11/24/dolar-oficial-hoy-y-dolar-blue-a-cuanto-cotizan-este-martes-24-de-noviembre/",
            "fuente": "Mercado Cambiario AR",
            "mercado": "Argentina",
            "impacto": "Divisas & Brechas",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "La Reserva Federal mantiene tono hawkish de cara a la reunión de diciembre con el bono del Tesoro a 10 años en 4,92%",
            "url": "https://es.dailyforex.com/stock-market-today/2026/11/23-a-24-semana-sp500-y-7-magnificas-atraviesa-fase-de-correccion/252080",
            "fuente": "Fed & Tasas UST",
            "mercado": "EE.UU.",
            "impacto": "Tasas & Liquidez",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Bonos soberanos argentinos y S&P Merval operan atentos a la dinámica del Riesgo País y las tasas en pesos",
            "url": "https://www.cronista.com/finanzas-mercados/",
            "fuente": "Renta Fija & Merval",
            "mercado": "Argentina",
            "impacto": "Bonos & Acciones",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Agenda clave en EE.UU.: inversores aguardan Confianza del Consumidor hoy y el índice de inflación PCE junto al PIB del 3T",
            "url": "https://cincodias.elpais.com/mercados-financieros/2026-11-24/la-bolsa-y-el-ibex-35.html",
            "fuente": "Macro EE.UU.",
            "mercado": "EE.UU.",
            "impacto": "Driver Macro",
            "fecha": "Hoy • En vivo"
        }
    ],
    "argentina": [
        {
            "titulo": "Dólar oficial en $1.600 y dólares financieros (MEP $1.696,42 / CCL $1.851,48) marcan el pulso cambiario de la rueda",
            "url": "https://tn.com.ar/economia/2026/11/24/dolar-oficial-hoy-y-dolar-blue-a-cuanto-cotizan-este-martes-24-de-noviembre/",
            "fuente": "Cambiario",
            "impacto": "Dólar & Brechas",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Dólar Blue opera en $1.675 y mantiene una brecha del 5% frente al tipo de cambio oficial minorista",
            "url": "https://www.lanacion.com.ar/economia/dolar/dolar-blue-hoy-a-cuanto-cotiza-el-martes-24-de-noviembre-nid24112026/",
            "fuente": "Mercado Paralelo",
            "impacto": "Divisas",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Deuda soberana en dólares y Riesgo País EMBI+: el mercado evalúa paridades de Bonares y Globales",
            "url": "https://www.ambito.com/contenidos/finanzas.html",
            "fuente": "Bonos Soberanos",
            "impacto": "Renta Fija USD",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Curva en pesos (LECAP, BONCAP y CER) y tasas de caución bursátil bajo la lupa de la mesa de dinero",
            "url": "https://www.cronista.com/finanzas-mercados/",
            "fuente": "Tasas & Pesos",
            "impacto": "Renta Fija ARS",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "S&P Merval y ADRs argentinos en Nueva York acompañan el rebote de los índices estadounidenses",
            "url": "https://www.infobae.com/economia/",
            "fuente": "Renta Variable",
            "impacto": "Acciones & ADRs",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Acumulación de reservas del BCRA, superávit fiscal y expectativas de inflación REM marcan el horizonte macro",
            "url": "https://www.iprofesional.com/finanzas",
            "fuente": "Macro & BCRA",
            "impacto": "Fundamentales",
            "fecha": "Hoy • En vivo"
        }
    ],
    "estados_unidos": [
        {
            "titulo": "Nasdaq Composite salta +6,51% y S&P 500 +4,73% en su mejor rueda desde abril tras el regreso de Jensen Huang a Nvidia (+31,48%)",
            "url": "https://www.ambito.com/finanzas/wall-street-rebota-67-el-nasdaq-al-frente-el-nuevo-nombramiento-jensen-huang-nvidia-n6348272",
            "fuente": "Wall Street",
            "impacto": "Tecnología & IA",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Semiconductores lideran la recuperación global: el ETF SMHX trepa +13,92% con fuertes alzas en Micron (+19%) e Intel (+16,3%)",
            "url": "https://www.cnbc.com/markets/",
            "fuente": "Semiconductores",
            "impacto": "Chips & Hardware",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "La Fed sostiene un discurso restrictivo: Anna Paulson (Fed Filadelfia) advierte que la política monetaria debe seguir firme hacia el 2%",
            "url": "https://es.dailyforex.com/stock-market-today/2026/11/23-a-24-semana-sp500-y-7-magnificas-atraviesa-fase-de-correccion/252080",
            "fuente": "Reserva Federal",
            "impacto": "Política Monetaria",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Rendimiento del Bono del Tesoro de EE.UU. a 10 años (UST 10Y) se sostiene en torno al 4,92% antes de la reunión del FOMC de diciembre",
            "url": "https://www.marketwatch.com/markets",
            "fuente": "US Treasuries",
            "impacto": "Curva Soberana",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Expectativa por el dato de Confianza del Consumidor (Conference Board), revisión del PIB del 3T e inflación PCE subyacente",
            "url": "https://cincodias.elpais.com/mercados-financieros/2026-11-24/la-bolsa-y-el-ibex-35.html",
            "fuente": "Calendario Macro",
            "impacto": "Datos Clave EE.UU.",
            "fecha": "Hoy • En vivo"
        },
        {
            "titulo": "Oro (US$ 3.651/oz, +3,87%) y Bitcoin (US$ 61.175, +4,91%) rebotan junto con el apetito por riesgo global",
            "url": "https://finance.yahoo.com/",
            "fuente": "Commodities & Cripto",
            "impacto": "Activos Globales",
            "fecha": "Hoy • En vivo"
        }
    ],
    "updated": ""
}

_NOTICIAS_CACHE_TS = 0.0
_NOTICIAS_LOCK = threading.Lock()

KW_DRIVERS_AR = (
    "dolar", "dólar", "bcra", "bono", "bonos", "merval", "riesgo pais", "riesgo país",
    "tasa", "tasas", "inflacion", "inflación", "reserva", "reservas", "lecap", "cer",
    "mep", "ccl", "licitacion", "licitación", "deuda", "ypf", "rigi", "caputo", "bausili",
    "adr", "adrs", "byma", "cedear", "emae", "fmi", "banco central", "acciones"
)

KW_DRIVERS_USA = (
    "wall street", "fed", "reserva federal", "s&p", "nasdaq", "dow jones", "nvidia",
    "treasury", "bonos del tesoro", "powell", "inflacion", "inflación", "pce", "pib",
    "gdp", "tasas", "types", "tipos", "petroleo", "petróleo", "oro", "bitcoin", "ia",
    "semiconductores", "chips", "tecnolog", "apple", "microsoft", "tesla", "fomc"
)

def _traducir_titular_es(texto: str) -> str:
    """Traduce titulares financieros de ingles a espanol con fallback doble."""
    if not texto or len(texto.strip()) < 5:
        return texto
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }
    for u in (
        f"https://clients5.google.com/translate_a/t?client=dict-chrome-ex&sl=en&tl=es&q={quote(texto)}",
        f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=es&dt=t&q={quote(texto)}"
    ):
        try:
            r = requests.get(u, headers=hdrs, timeout=3.0)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    if isinstance(data[0], str) and data[0].strip():
                        return data[0].strip()
                    if isinstance(data[0], list):
                        partes = [seg[0] for seg in data[0] if isinstance(seg, list) and seg and seg[0]]
                        trad = "".join(partes).strip()
                        if trad:
                            return trad
        except Exception:
            pass
    try:
        u_mm = f"https://api.mymemory.translated.net/get?q={quote(texto[:450])}&langpair=en|es"
        r2 = requests.get(u_mm, headers=hdrs, timeout=3.0)
        if r2.status_code == 200:
            trad2 = (r2.json().get("responseData") or {}).get("translatedText", "").strip()
            if trad2 and trad2.lower() != texto.lower():
                return trad2
    except Exception:
        pass
    return texto

def _parsear_feed_rss(url_rss: str, mercado_lbl: str, tz_ar, hoy_iso: str, fuente_default: str = "") -> List[Dict[str, Any]]:
    salida: List[Dict[str, Any]] = []
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*"
    }
    try:
        r = GLOBAL_HTTP_SESSION.get(url_rss, headers=hdrs, timeout=3.0, verify=False)
        xml_txt = r.content.decode("utf-8", errors="replace")
        if r.status_code != 200 or "<item" not in xml_txt.lower():
            return salida
        soup = BeautifulSoup(xml_txt, "html.parser")
        from email.utils import parsedate_to_datetime
        ruido_editorial = (
            "401(k)", "retirement", "personal finance", "how to ", "why ", "opinion:",
            "review:", "best credit cards", "mortgage rates today", "lottery", "powerball"
        )
        for it in soup.find_all("item"):
            t_el = it.find("title")
            l_el = it.find("link")
            p_el = it.find("pubdate")
            s_el = it.find("source")
            d_el = it.find("description")
            raw_tit = BeautifulSoup(t_el.get_text(" ", strip=True), "html.parser").get_text(" ", strip=True) if t_el else ""
            raw_tit = re.sub(r"\s+", " ", raw_tit).strip()
            fuente_nom = s_el.get_text(strip=True) if s_el else (fuente_default or mercado_lbl)
            if " - " in raw_tit:
                partes = raw_tit.rsplit(" - ", 1)
                raw_tit = partes[0].strip()
                if not s_el and not fuente_default:
                    fuente_nom = partes[1].strip()

            link = l_el.get_text(strip=True) if l_el else (str(l_el.next_sibling).strip() if (l_el and l_el.next_sibling) else "")
            if not link.startswith("http") and it.find("guid"):
                g_txt = it.find("guid").get_text(strip=True)
                if g_txt.startswith("http"):
                    link = g_txt
            if len(raw_tit) < 22 or not link.startswith("http"):
                continue

            t_low = raw_tit.lower()
            if any(bad in t_low for bad in ruido_editorial):
                continue

            # Si el titulo de EE.UU. no trae cifras/datos duros, enriquecer con la 1ra oracion de la bajada
            if mercado_lbl == "EE.UU." and d_el and not re.search(r"\d+[%\.]|\$\d+|fed|fomc|cpi|pce|gdp|yield|s&p|nasdaq|dow", t_low):
                desc_txt = BeautifulSoup(d_el.get_text(" ", strip=True), "html.parser").get_text(" ", strip=True)
                desc_txt = re.sub(r"\s+", " ", desc_txt).strip()
                primera_oracion = desc_txt.split(". ")[0].strip()
                if 25 <= len(primera_oracion) <= 130 and primera_oracion.lower() not in t_low:
                    raw_tit = f"{raw_tit}: {primera_oracion}"

            ts_val = 0.0
            fec_lbl = "Hoy • En vivo"
            if p_el:
                try:
                    dt_p = parsedate_to_datetime(p_el.get_text(strip=True)).astimezone(tz_ar)
                    ts_val = dt_p.timestamp()
                    if dt_p.strftime("%Y-%m-%d") == hoy_iso:
                        fec_lbl = f"Hoy • {dt_p.strftime('%H:%M')} hs"
                    else:
                        mes_n = MESES_CORTOS_ES.get(dt_p.month, str(dt_p.month))
                        fec_lbl = f"{dt_p.day} {mes_n} • {dt_p.strftime('%H:%M')} hs"
                except Exception:
                    pass

            if any(k in t_low for k in ("dolar", "dólar", "mep", "ccl", "divisa", "cambi", "blue", "dollar", "fx")):
                imp_tag = "Dólar & Divisas"
            elif any(k in t_low for k in ("fed", "tasa", "treasury", "fomc", "bcra", "tamar", "lecap", "powell", "rates", "yield")):
                imp_tag = "Tasas & Bancos Centrales"
            elif any(k in t_low for k in ("bono", "deuda", "riesgo pais", "riesgo país", "cer", "bond", "credit")):
                imp_tag = "Renta Fija & Deuda"
            elif any(k in t_low for k in ("nvidia", "nasdaq", "s&p", "merval", "accion", "wall street", "ia", "stocks", "dow", "chips", "tech")):
                imp_tag = "Renta Variable"
            elif any(k in t_low for k in ("inflacion", "inflación", "pce", "pib", "emae", "empleo", "cpi", "gdp", "jobs", "oil", "gold")):
                imp_tag = "Macroeconomía"
            else:
                imp_tag = "Driver de Mercado"

            salida.append({
                "ts": ts_val,
                "titulo": raw_tit,
                "url": link,
                "fuente": fuente_nom[:28],
                "mercado": mercado_lbl,
                "impacto": imp_tag,
                "fecha": fec_lbl
            })
    except Exception:
        pass
    salida.sort(key=lambda x: x["ts"], reverse=True)
    return salida

def actualizar_noticias_drivers(forzar: bool = False) -> Dict[str, Any]:
    global _NOTICIAS_CACHE_TS, CACHE_NOTICIAS_DRIVERS
    from datetime import timezone, timedelta
    tz_ar = timezone(timedelta(hours=-3))
    ahora_ar = datetime.now(tz_ar)
    hoy_iso = ahora_ar.strftime("%Y-%m-%d")

    now = time.time()
    if not forzar and (now - _NOTICIAS_CACHE_TS < 300.0) and CACHE_NOTICIAS_DRIVERS.get("updated"):
        return CACHE_NOTICIAS_DRIVERS
    if not _NOTICIAS_LOCK.acquire(blocking=False):
        return CACHE_NOTICIAS_DRIVERS
    try:
        q_ar = quote("dolar bonos merval bcra economia argentina")
        feeds_ar = [
            ("https://www.ambito.com/rss/pages/finanzas.xml", "Ámbito Finanzas"),
            ("https://www.cronista.com/files/rss/finanzas-mercados.xml", "El Cronista"),
            ("https://www.infobae.com/arc/outboundfeeds/rss/category/economia/", "Infobae Economía"),
            (f"https://news.google.com/rss/search?q={q_ar}&hl=es-419&gl=AR&ceid=AR:es-419", "")
        ]
        feeds_us = [
            ("https://www.investing.com/rss/news_25.rss", "Investing Markets"),
            ("https://www.investing.com/rss/news_95.rss", "Investing Macro"),
            ("https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC,%5EIXIC,%5EDJI,NVDA,TLT,GC=F&region=US&lang=en-US", "Yahoo Finance"),
            ("https://www.cnbc.com/id/15839069/device/rss/rss.html", "CNBC Investing"),
            ("https://www.cnbc.com/id/20910258/device/rss/rss.html", "CNBC Economy")
        ]

        res_ar: List[Dict[str, Any]] = []
        res_us: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=9) as ex_n:
            futs_ar = [ex_n.submit(_parsear_feed_rss, u, "Argentina", tz_ar, hoy_iso, f_nom) for u, f_nom in feeds_ar]
            futs_us = [ex_n.submit(_parsear_feed_rss, u, "EE.UU.", tz_ar, hoy_iso, f_nom) for u, f_nom in feeds_us]
            for f in futs_ar:
                try: res_ar.extend(f.result(timeout=3.2))
                except Exception: pass
            for f in futs_us:
                try: res_us.extend(f.result(timeout=3.2))
                except Exception: pass

        res_ar.sort(key=lambda x: x["ts"], reverse=True)
        res_us.sort(key=lambda x: x["ts"], reverse=True)

        vistos = set()
        lista_ar_limpia = []
        for it in res_ar:
            k = it["titulo"].lower()[:55]
            if k not in vistos:
                vistos.add(k)
                lista_ar_limpia.append(it)
            if len(lista_ar_limpia) >= 6:
                break

        lista_us_limpia = []
        for it in res_us:
            k = it["titulo"].lower()[:55]
            if k not in vistos:
                vistos.add(k)
                lista_us_limpia.append(it)
            if len(lista_us_limpia) >= 6:
                break

        if len(lista_ar_limpia) >= 1:
            CACHE_NOTICIAS_DRIVERS["argentina"] = [
                {k: v for k, v in x.items() if k != "ts"} for x in lista_ar_limpia[:6]
            ]
        if len(lista_us_limpia) >= 1:
            us_traducidos = []
            for x in lista_us_limpia[:6]:
                item_clean = {k: v for k, v in x.items() if k != "ts"}
                item_clean["titulo"] = _traducir_titular_es(item_clean["titulo"])
                us_traducidos.append(item_clean)
            CACHE_NOTICIAS_DRIVERS["estados_unidos"] = us_traducidos

        combinados = []
        ar_cur = CACHE_NOTICIAS_DRIVERS.get("argentina", [])
        us_cur = CACHE_NOTICIAS_DRIVERS.get("estados_unidos", [])
        for i in range(3):
            if i < len(us_cur):
                item_u = dict(us_cur[i])
                item_u["mercado"] = "EE.UU."
                combinados.append(item_u)
            if i < len(ar_cur):
                item_a = dict(ar_cur[i])
                item_a["mercado"] = "Argentina"
                combinados.append(item_a)
        if combinados:
            CACHE_NOTICIAS_DRIVERS["top5"] = combinados[:5]

        CACHE_NOTICIAS_DRIVERS["updated"] = ahora_ar.strftime("%H:%M:%S")
        _NOTICIAS_CACHE_TS = time.time()
    except Exception:
        _NOTICIAS_CACHE_TS = time.time() - 240.0
    finally:
        _NOTICIAS_LOCK.release()
    return CACHE_NOTICIAS_DRIVERS

@app.get("/api/calendario/economico")
def ver_calendario_economico(desde: Optional[str] = Query(None), hasta: Optional[str] = Query(None)):
    indec = extraer_calendario_indec()
    usa = extraer_calendario_usa()
    return {"argentina": indec, "estados_unidos": usa, "updated": ULTIMA_ACTUALIZACION}

@app.get("/dolar")
def ver_dolar():
    try:
        r = GLOBAL_HTTP_SESSION.get("https://dolarapi.com/v1/dolares", timeout=3.5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"error": "Servicio de divisas ocupado"}

@app.get("/mercado/resumen")
def ver_resumen_mercado():
    try:
        r = GLOBAL_HTTP_SESSION.get("https://dolarapi.com/v1/dolares", timeout=3.5)
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
        r = GLOBAL_HTTP_SESSION.get("https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=6&sort=desc", timeout=3.5)
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
