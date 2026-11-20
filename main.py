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
            r = sess.get(u, headers=hdrs, timeout=4.0, verify=False)
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

app = FastAPI(title="API Mercado Argentino")
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
    {"variable": "MERVAL_ARS", "descripcion": "Merval", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "83.482 pts", "variacion": "-0,7%", "var_mes": "-9,3%", "var_anio": "-22,1%", "valor": 83482.0, "fecha": "Mes: -9,3% | Año: -22,1%"},
    {"variable": "MERVAL_USD", "descripcion": "Merval USD", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "50 pts", "variacion": "-1,3%", "var_mes": "-10,0%", "var_anio": "-28,7%", "valor": 50.0, "fecha": "Mes: -10,0% | Año: -28,7%"},
    {"variable": "RIESGO_PAIS", "descripcion": "Riesgo País EMBI+", "categoria": "indices", "subcategoria": "argentina", "valor_formateado": "852 pb", "variacion": "-0,1%", "var_mes": "+8,3%", "var_anio": "+49,2%", "valor": 852.0, "fecha": "Mes: +8,3% | Año: +49,2%"},
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
    "monedas": [],
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

def obtener_msci_merval_en_vivo(ccl_ref: float = 1669.0) -> Dict[str, Dict[str, Any]]:
    datos = {
        "MERVAL_ARS": {"ult": 83482.0, "dia": -0.7, "mes": -9.3, "anio": -22.1},
        "MERVAL_USD": {"ult": 50.0, "dia": -1.3, "mes": -10.0, "anio": -28.7},
        "RIESGO_PAIS": {"ult": 750.0, "dia": 0.0, "mes": 0.0, "anio": 0.0}
    }
    rp_actualizado = False

    # 1. Fuente JSON directa para Riesgo País (ArgentinaDatos: último + serie histórica para variaciones)
    try:
        r_rp = GLOBAL_HTTP_SESSION.get(
            "https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=3.8
        )
        if r_rp.status_code == 200:
            serie_rp = r_rp.json()
            if isinstance(serie_rp, list) and len(serie_rp) >= 2:
                ult_item = serie_rp[-1]
                prev_item = serie_rp[-2]
                v_ult = float(ult_item.get("valor") or 0)
                v_prev = float(prev_item.get("valor") or 0)
                if 100 <= v_ult <= 5000:
                    datos["RIESGO_PAIS"]["ult"] = v_ult
                    rp_actualizado = True
                    if v_prev > 0:
                        datos["RIESGO_PAIS"]["dia"] = round(((v_ult - v_prev) / v_prev) * 100.0, 2)
                    # Variación mensual (~22 ruedas) y anual (inicio de año)
                    if len(serie_rp) >= 23:
                        v_mes = float(serie_rp[-23].get("valor") or 0)
                        if v_mes > 0:
                            datos["RIESGO_PAIS"]["mes"] = round(((v_ult - v_mes) / v_mes) * 100.0, 2)
                    anio_act = str(ult_item.get("fecha", ""))[:4]
                    inicio_anio = next((x for x in serie_rp if str(x.get("fecha", "")).startswith(anio_act)), None)
                    if inicio_anio and float(inicio_anio.get("valor") or 0) > 0:
                        v_ini = float(inicio_anio["valor"])
                        datos["RIESGO_PAIS"]["anio"] = round(((v_ult - v_ini) / v_ini) * 100.0, 2)
    except Exception:
        pass

    # 2. Rava HTML compartido + perfil específico de Riesgo País
    htmls_fuente = [obtener_html_rava_compartido()]
    if not rp_actualizado:
        try:
            rr_rp = GLOBAL_HTTP_SESSION.get(
                f"https://www.rava.com/perfil/riesgo%20pais?_t={int(time.time()*1000)}",
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
                timeout=3.5
            )
            if rr_rp.status_code == 200 and len(rr_rp.text) > 400:
                htmls_fuente.append(rr_rp.text)
        except Exception:
            pass

    for raw_rava_html in htmls_fuente:
        if not raw_rava_html:
            continue
        try:
            html_u = raw_rava_html.replace("&quot;", '"')
            for m_obj in re.finditer(r'\{[^{}]*"(?:simbolo|especie|nombre)"\s*:\s*"(MERVAL(?:\s*USD|\s*CCL)?|IMV|RIESGO\s*PA[IÍ]S)"[^{}]*\}', html_u, re.IGNORECASE):
                b = m_obj.group(0)
                sim = m_obj.group(1).upper().strip()
                k_target = "RIESGO_PAIS" if "RIESGO" in sim else ("MERVAL_USD" if ("USD" in sim or "CCL" in sim) else "MERVAL_ARS")

                m_u = re.search(r'"ultimo"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_d = re.search(r'"(?:variacion|var_dia|dia|change)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_m = re.search(r'"(?:var_mes|mes|mensual|var_mtd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_a = re.search(r'"(?:var_anio|anio|anual|var_ytd|ytd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)

                vu = _parse_float_limpio(m_u.group(1)) if m_u else None
                vd = _parse_float_limpio(m_d.group(1)) if m_d else None
                vm = _parse_float_limpio(m_m.group(1)) if m_m else None
                va = _parse_float_limpio(m_a.group(1)) if m_a else None

                if k_target == "RIESGO_PAIS" and vu and 100 <= vu <= 5000:
                    datos["RIESGO_PAIS"]["ult"] = vu
                    rp_actualizado = True
                elif k_target == "MERVAL_USD" and vu and 20 <= vu <= 5000:
                    datos["MERVAL_USD"]["ult"] = vu
                elif k_target == "MERVAL_ARS" and vu and 40000 <= vu <= 10000000:
                    datos["MERVAL_ARS"]["ult"] = vu

                if vd is not None and -25.0 <= vd <= 25.0:
                    datos[k_target]["dia"] = vd
                if vm is not None and -60.0 <= vm <= 150.0:
                    datos[k_target]["mes"] = vm
                if va is not None and -80.0 <= va <= 300.0:
                    datos[k_target]["anio"] = va

            plano = BeautifulSoup(raw_rava_html, "html.parser").get_text(" ", strip=True)
            patrones_tabla = [
                ("MERVAL_ARS", r'\bMERVAL\b(?!\s*USD)\s+([\d]{1,3}(?:\.[\d]{3})+(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)'),
                ("MERVAL_USD", r'\bMERVAL\s+USD\b\s+([\d]{2,4}(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)'),
                ("RIESGO_PAIS", r'\bRIESGO\s+PA[IÍ]S\b\s+([\d]{3,4}(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)')
            ]
            for k_t, pat in patrones_tabla:
                mt = re.search(pat, plano, re.IGNORECASE)
                if mt:
                    u_v = _parse_float_limpio(mt.group(1))
                    d_v = _parse_float_limpio(mt.group(2))
                    m_v = _parse_float_limpio(mt.group(3))
                    a_v = _parse_float_limpio(mt.group(4))
                    if u_v:
                        datos[k_t]["ult"] = u_v
                        if k_t == "RIESGO_PAIS":
                            rp_actualizado = True
                    if d_v is not None: datos[k_t]["dia"] = d_v
                    if m_v is not None: datos[k_t]["mes"] = m_v
                    if a_v is not None: datos[k_t]["anio"] = a_v
        except Exception:
            pass

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
            "_live": rp_actualizado if cod == "RIESGO_PAIS" else True
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
            r = GLOBAL_HTTP_SESSION.get(url, headers=headers, timeout=4.5, verify=False)
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
    "DOLAR_OFICIAL": {"compra": 1520.00, "venta": 1570.00, "variacion": "0,00%", "fecha": "En vivo"},
    "DOLAR_BLUE": {"compra": 1545.00, "venta": 1565.00, "variacion": "+0,64%", "fecha": "En vivo"},
    "DOLAR_TARJETA": {"compra": None, "venta": 2041.00, "variacion": "0,00%", "fecha": "En vivo"},
    "DOLAR_NETFLIX": {"compra": None, "venta": 2370.70, "variacion": "0,00%", "fecha": "En vivo"},
    "DOLAR_MEP": {"compra": None, "venta": 1584.11, "variacion": "0,00%", "fecha": "AL30 / AL30D"},
    "DOLAR_CCL": {"compra": None, "venta": 1684.47, "variacion": "0,00%", "fecha": "AL30 / AL30C"},
    "DOLAR_CRIPTO": {"compra": 1654.73, "venta": 1658.43, "variacion": "+0,55%", "fecha": "En vivo"},
    "DOLAR_A3500": {"compra": 1535.00, "venta": 1544.00, "variacion": "-0,10%", "fecha": "En vivo"},
    "DOLAR_FUTURO": {"compra": 1563.00, "venta": 1563.50, "variacion": "0,00%", "fecha": "En vivo"}
}

def _fmt_var_dolar(val_raw: Any, default_str: str = "0,00%") -> str:
    if val_raw is None or val_raw == "":
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
        "Referer": "https://www.rava.com/"
    }

    bonos_px: Dict[str, float] = {}
    bonos_var: Dict[str, float] = {}
    bonos_ant: Dict[str, float] = {}
    rava_directo: Dict[str, Dict[str, Any]] = {}

    # A) Consultar Rava (Home + Dólares + Perfil Dólar CCL + Bonos)
    urls_rava = [
        obtener_html_rava_compartido(ttl=3.5),
        f"https://www.rava.com/cotizaciones/dolares?_t={ts_ms}",
        f"https://www.rava.com/cotizaciones/bonos?_t={ts_ms}",
        f"https://www.rava.com/perfil/dolar%20ccl?_t={ts_ms}",
        f"https://www.rava.com/perfil/dolar%20mep?_t={ts_ms}"
    ]

    for src in urls_rava:
        raw_html = ""
        if src and src.startswith("http"):
            if (
                all(k in bonos_px for k in ("AL30", "AL30D", "AL30C"))
                and "DOLAR_MEP" in rava_directo
                and "DOLAR_CCL" in rava_directo
            ):
                break
            try:
                rr = GLOBAL_HTTP_SESSION.get(src, headers=hdrs, timeout=3.2)
                if rr.status_code == 200:
                    raw_html = rr.text
            except Exception:
                pass
        else:
            raw_html = src or ""

        if not raw_html:
            continue

        html_u = raw_html.replace("&quot;", '"').replace("\\/", "/")

        # 1. Extraer objetos JSON de Rava (tanto con clave "simbolo"/"especie" en cualquier orden)
        for m_obj in re.finditer(r'\{[^{}]*\}', html_u):
            b = m_obj.group(0)
            m_sim = re.search(r'"(?:simbolo|especie|nombre|identificador|titulo|name)"\s*:\s*"([^"]+)"', b, re.IGNORECASE)
            if not m_sim:
                continue
            sim = m_sim.group(1).upper().strip()

            m_u = re.search(r'"(?:ultimo|venta|valor|close|cierre|px_last|last)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
            m_c = re.search(r'"(?:compra|buy|bid)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
            m_d = re.search(r'"(?:variacion|var_dia|dia|change|porc|var)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)
            m_ant = re.search(r'"(?:anterior|cierre_ant|prev)"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)

            vu = normalizar_valor(m_u.group(1)) if m_u else None
            vc = normalizar_valor(m_c.group(1)) if m_c else None
            vd_num = normalizar_valor(m_d.group(1)) if m_d else None
            vant = normalizar_valor(m_ant.group(1)) if m_ant else None

            sim_clean = re.sub(r'\s+(CI|24HS|48HS|SPOT)$', '', sim).strip()
            if sim_clean in ("AL30", "AL30D", "AL30C", "GD30", "GD30D", "GD30C") and vu and vu > 15:
                if sim_clean not in bonos_px or sim == sim_clean:
                    bonos_px[sim_clean] = vu
                    if vd_num is not None and -35 <= vd_num <= 35:
                        bonos_var[sim_clean] = vd_num
                    if vant and vant > 15:
                        bonos_ant[sim_clean] = vant

            if not vu or vu < 900 or vu > 5000:
                continue

            cod_t = None
            if any(k in sim for k in ("CCL", "LIQUI", "CONTADO CON LIQUI", "DOLAR CCL", "DÓLAR CCL")):
                cod_t = "DOLAR_CCL"
            elif any(k in sim for k in ("MEP", "BOLSA", "DOLAR MEP", "DÓLAR MEP")):
                cod_t = "DOLAR_MEP"
            elif any(k in sim for k in ("MAYORISTA", "A3500", "3500")):
                cod_t = "DOLAR_A3500"
            elif any(k in sim for k in ("BLUE", "INFORMAL", "PARALELO")):
                cod_t = "DOLAR_BLUE"
            elif any(k in sim for k in ("OFICIAL", "MINORISTA", "BNA")):
                cod_t = "DOLAR_OFICIAL"

            if cod_t and (cod_t not in rava_directo or rava_directo[cod_t].get("var_num") is None):
                rava_directo[cod_t] = {
                    "compra": None if cod_t in ("DOLAR_MEP", "DOLAR_CCL") else (round(vc, 2) if (vc and vc > 900) else None),
                    "venta": round(vu, 2),
                    "var_num": vd_num,
                    "variacion": _fmt_var_dolar(vd_num, "0,00%"),
                    "fecha": "AL30 / AL30D" if cod_t == "DOLAR_MEP" else ("AL30 / AL30C" if cod_t == "DOLAR_CCL" else "En vivo")
                }

        # 2. Extraer desde texto plano renderizado de Rava
        plano = BeautifulSoup(raw_html, "html.parser").get_text(" ", strip=True)
        for tk_b in ("AL30", "AL30D", "AL30C", "GD30", "GD30D", "GD30C"):
            if tk_b not in bonos_px:
                mb = re.search(r'\b' + tk_b + r'\b\s+([\d\.]+(?:,[\d]+)?)\s+([+\-−]?[\d]+[\.,][\d]+)', plano)
                if mb:
                    u_b = normalizar_valor(mb.group(1))
                    d_b = normalizar_valor(mb.group(2))
                    if u_b and u_b > 15:
                        bonos_px[tk_b] = u_b
                        if d_b is not None:
                            bonos_var[tk_b] = d_b

        patrones_rava = [
            ("DOLAR_CCL", r'D[ÓO]LAR\s+(?:CCL|CONTADO\s+CON\s+LIQUI)(?:\s+GD30|\s+AL30|\s+CEDEAR)?\b[^\d]{1,30}([\d]{1,2}\.[\d]{3}(?:,[\d]+)?|[\d]{3,4}(?:,[\d]+)?)\s+([+\-−]?[\d]+[\.,][\d]+)'),
            ("DOLAR_MEP", r'D[ÓO]LAR\s+(?:MEP|BOLSA)(?:\s+GD30|\s+AL30|\s+SOBERANO)?\b[^\d]{1,30}([\d]{1,2}\.[\d]{3}(?:,[\d]+)?|[\d]{3,4}(?:,[\d]+)?)\s+([+\-−]?[\d]+[\.,][\d]+)')
        ]
        for cod_k, pat in patrones_rava:
            if cod_k not in rava_directo:
                mt = re.search(pat, plano, re.IGNORECASE)
                if mt:
                    uv = normalizar_valor(mt.group(1))
                    dv = normalizar_valor(mt.group(2))
                    if uv and 900 < uv < 5000:
                        rava_directo[cod_k] = {
                            "compra": None,
                            "venta": round(uv, 2),
                            "var_num": dv,
                            "variacion": _fmt_var_dolar(dv, "0,00%"),
                            "fecha": "AL30 / AL30D" if cod_k == "DOLAR_MEP" else "AL30 / AL30C"
                        }

    # B) Complementar AL30, AL30D y AL30C desde las tablas intradiarias de bonos (ARS, MEP y CCL)
    try:
        urls_bonos_vivo = [
            ("soberanos_ars", "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=ARS", ""),
            ("soberanos_mep", "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=MEP", "D"),
            ("soberanos_ccl", "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=CCL", "C")
        ]
        for k_cache, url_b, sufijo_esp in urls_bonos_vivo:
            tabla_c = (CACHE_CURVAS_SJB.get(k_cache) or {}).get("tabla") or []
            if not tabla_c:
                try:
                    rb = GLOBAL_HTTP_SESSION.get(url_b, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.8)
                    if rb.status_code == 200:
                        norm_b = normalizar_respuesta_curva(rb.json())
                        if norm_b and norm_b.get("tabla"):
                            CACHE_CURVAS_SJB[k_cache] = norm_b
                            tabla_c = norm_b["tabla"]
                except Exception:
                    pass
            for row in tabla_c:
                tk_raw = str(row.get("Ticker") or row.get("ticker") or "").upper().strip()
                tk_base = re.sub(r'[DC]$', '', tk_raw)
                if tk_base in ("AL30", "GD30"):
                    tk_final = tk_base + sufijo_esp
                    pr_r = normalizar_valor(row.get("Precio") or row.get("precio"))
                    vr_r = normalizar_valor(row.get("Variacion") or row.get("variacion"))
                    if pr_r and pr_r > 15:
                        if tk_final not in bonos_px:
                            bonos_px[tk_final] = pr_r
                        if tk_final not in bonos_var and vr_r is not None:
                            bonos_var[tk_final] = vr_r
    except Exception:
        pass

    def _calcular_ratio_y_var(tk_pesos: str, tk_usd: str):
        p_ars = bonos_px.get(tk_pesos)
        p_usd = bonos_px.get(tk_usd)
        if not p_ars or not p_usd or p_usd <= 0:
            return None, None
        ratio = p_ars / p_usd
        if ratio < 15:
            ratio *= 100.0
        elif ratio > 50000:
            ratio /= 100.0
        if not (900 <= ratio <= 5000):
            return None, None

        v_ars = bonos_var.get(tk_pesos)
        v_usd = bonos_var.get(tk_usd)
        var_pct = None
        if v_ars is not None and v_usd is not None and (100.0 + v_usd) != 0:
            var_pct = (((100.0 + v_ars) / (100.0 + v_usd)) - 1.0) * 100.0
        elif tk_pesos in bonos_ant and tk_usd in bonos_ant and bonos_ant[tk_usd] > 0:
            ratio_ant = bonos_ant[tk_pesos] / bonos_ant[tk_usd]
            if ratio_ant < 15:
                ratio_ant *= 100.0
            if ratio_ant > 900:
                var_pct = ((ratio / ratio_ant) - 1.0) * 100.0
        return round(ratio, 2), (round(var_pct, 2) if var_pct is not None else None)

    # C) Armar DOLAR_MEP (AL30 / AL30D + contraste Rava, sin citar fuente)
    mep_px, mep_var = _calcular_ratio_y_var("AL30", "AL30D")
    if not mep_px:
        mep_px, mep_var = _calcular_ratio_y_var("GD30", "GD30D")

    rava_mep = rava_directo.get("DOLAR_MEP")
    if mep_px:
        if mep_var is None and rava_mep and rava_mep.get("var_num") is not None:
            mep_var = rava_mep["var_num"]
        res_rava["DOLAR_MEP"] = {
            "compra": None,
            "venta": mep_px,
            "variacion": _fmt_var_dolar(mep_var, "0,00%"),
            "fecha": "AL30 / AL30D"
        }
    elif rava_mep:
        res_rava["DOLAR_MEP"] = {
            "compra": None,
            "venta": rava_mep["venta"],
            "variacion": rava_mep["variacion"],
            "fecha": "AL30 / AL30D"
        }

    # D) Armar DOLAR_CCL (AL30 / AL30C + contraste Rava, sin citar fuente)
    ccl_px, ccl_var = _calcular_ratio_y_var("AL30", "AL30C")
    if not ccl_px:
        ccl_px, ccl_var = _calcular_ratio_y_var("GD30", "GD30C")

    rava_ccl = rava_directo.get("DOLAR_CCL")
    if ccl_px:
        if ccl_var is None and rava_ccl and rava_ccl.get("var_num") is not None:
            ccl_var = rava_ccl["var_num"]
        res_rava["DOLAR_CCL"] = {
            "compra": None,
            "venta": ccl_px,
            "variacion": _fmt_var_dolar(ccl_var, "0,00%"),
            "fecha": "AL30 / AL30C"
        }
    elif rava_ccl:
        res_rava["DOLAR_CCL"] = {
            "compra": None,
            "venta": rava_ccl["venta"],
            "variacion": rava_ccl["variacion"],
            "fecha": "AL30 / AL30C"
        }

    for k_other in ("DOLAR_A3500", "DOLAR_OFICIAL", "DOLAR_BLUE"):
        if k_other in rava_directo:
            res_rava[k_other] = {
                "compra": rava_directo[k_other].get("compra"),
                "venta": rava_directo[k_other]["venta"],
                "variacion": rava_directo[k_other]["variacion"],
                "fecha": "En vivo"
            }

    return res_rava

def obtener_puntas_dolarito() -> Dict[str, Dict[str, Any]]:
    if time.time() - _DOLARITO_CICLO_CACHE["ts"] < 6.5 and _DOLARITO_CICLO_CACHE["data"]:
        return {k: dict(v) for k, v in _DOLARITO_CICLO_CACHE["data"].items()}
    if not _DOLARITO_CACHE_LOCK.acquire(blocking=False):
        if _DOLARITO_CICLO_CACHE["data"]:
            return {k: dict(v) for k, v in _DOLARITO_CICLO_CACHE["data"].items()}
        _DOLARITO_CACHE_LOCK.acquire()
    try:
        if time.time() - _DOLARITO_CICLO_CACHE["ts"] < 6.5 and _DOLARITO_CICLO_CACHE["data"]:
            return {k: dict(v) for k, v in _DOLARITO_CICLO_CACHE["data"].items()}
        return _obtener_puntas_dolarito_interno()
    finally:
        _DOLARITO_CACHE_LOCK.release()

def _obtener_puntas_dolarito_interno() -> Dict[str, Dict[str, Any]]:
    res: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in BASE_DOLARITO_EXACTA.items()}
    ts_ms = int(time.time() * 1000)
    hora_vivo = datetime.utcnow().strftime("%H:%M:%S UTC")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Referer": "https://www.dolarito.ar/",
        "Origin": "https://www.dolarito.ar"
    }

    # 1. Obtener MEP y CCL en vivo desde AL30/AL30D, AL30/AL30C y Rava Bursátil
    datos_rava = obtener_dolares_rava_en_vivo()
    for k_r, v_r in datos_rava.items():
        res[k_r] = dict(v_r)
        BASE_DOLARITO_EXACTA[k_r] = dict(v_r)

    def _procesar_nodo_dolarito(k_hint: str, nd: Dict[str, Any]):
        nom = f"{k_hint} {nd.get('name', '')} {nd.get('nombre', '')} {nd.get('slug', '')} {nd.get('type', '')}".lower().strip()
        if any(sp in nom for sp in ["uala", "ualá", "brubank", "nexo", "youhodler", "arq", "prex"]):
            return
        # PROHIBIDO tomar MEP o CCL de Dolarito (evita variaciones viejas de ayer)
        if any(w in nom for w in ["mep", "bolsa", "ccl", "liqui", "contado con liqui"]):
            return

        c = normalizar_valor(nd.get("buy") or nd.get("compra") or nd.get("bid"))
        v = normalizar_valor(nd.get("sell") or nd.get("venta") or nd.get("value") or nd.get("valor") or nd.get("ask"))
        var_raw = nd.get("variation") if nd.get("variation") is not None else (nd.get("variacion") or nd.get("change") or nd.get("var"))

        if not v or v < 900 or v > 5000:
            return

        cod_dest = None
        es_un_valor = False
        if any(k in nom for k in ["mayorista", "a3500", "3500", "mulc", "wholesale"]):
            cod_dest = "DOLAR_A3500"
        elif any(k in nom for k in ["oficial", "minorista", "bna", "banco nacion"]) and "mejor" not in nom and "tarjeta" not in nom and "mayorista" not in nom:
            cod_dest = "DOLAR_OFICIAL"
        elif any(k in nom for k in ["blue", "informal", "paralelo"]):
            cod_dest = "DOLAR_BLUE"
        elif any(k in nom for k in ["cripto", "crypto", "usdt"]):
            cod_dest = "DOLAR_CRIPTO"
        elif "tarjeta" in nom or "turista" in nom:
            cod_dest = "DOLAR_TARJETA"
            es_un_valor = True
        elif "netflix" in nom or "streaming" in nom:
            cod_dest = "DOLAR_NETFLIX"
            es_un_valor = True
        elif "futuro" in nom or "rofex" in nom:
            cod_dest = "DOLAR_FUTURO"

        if cod_dest:
            var_def = res[cod_dest].get("variacion", "0,00%")
            res[cod_dest] = {
                "compra": None if es_un_valor else (round(c, 2) if (c and c > 900) else res[cod_dest].get("compra")),
                "venta": round(v, 2),
                "variacion": _fmt_var_dolar(var_raw, var_def),
                "fecha": hora_vivo
            }
            BASE_DOLARITO_EXACTA[cod_dest] = dict(res[cod_dest])

    # 2. Consultar Dolarito solo para el resto de los dólares (Blue, Oficial, Futuro, etc.)
    t1 = f"https://api.dolarito.ar/api/frontend/quotations?_t={ts_ms}"
    t2 = f"https://www.dolarito.ar/?_t={ts_ms}"
    urls_intento = [
        t1,
        f"https://corsproxy.io/?{quote(t1, safe='')}",
        f"https://api.codetabs.com/v1/proxy?quest={quote(t1, safe='')}",
        f"https://api.allorigins.win/raw?url={quote(t1, safe='')}&_t={ts_ms}",
        t2,
        f"https://corsproxy.io/?{quote(t2, safe='')}"
    ]

    dolarito_cripto_actualizado = False
    for u in urls_intento:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers=headers, timeout=3.2, verify=False)
            if r.status_code != 200 or not r.text.strip():
                continue
            txt = r.text.strip()
            data_json = None
            if txt.startswith("{") or txt.startswith("["):
                data_json = json.loads(txt)
            else:
                m_next = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', txt, re.DOTALL)
                if m_next:
                    data_json = json.loads(m_next.group(1))

            if data_json:
                prev_cripto_v = res["DOLAR_CRIPTO"]["venta"]
                def rec(o, k_padre=""):
                    if isinstance(o, dict):
                        if any(k in o for k in ("buy", "sell", "compra", "venta", "value", "valor")):
                            _procesar_nodo_dolarito(k_padre, o)
                        for kh, vh in o.items():
                            rec(vh, str(kh))
                    elif isinstance(o, list):
                        for el in o:
                            rec(el, k_padre)
                rec(data_json)
                if res["DOLAR_CRIPTO"]["venta"] != prev_cripto_v:
                    dolarito_cripto_actualizado = True
                break
        except Exception:
            pass

    # 2b. Sincronizar Blue, Oficial y Mayorista (A3500) en vivo desde DolarApi
    try:
        r_da = GLOBAL_HTTP_SESSION.get(f"https://dolarapi.com/v1/dolares?_t={ts_ms}", headers={"User-Agent": "Mozilla/5.0"}, timeout=3.0)
        if r_da.status_code == 200:
            for item_da in r_da.json():
                casa = str(item_da.get("casa", "")).lower().strip()
                c_da = normalizar_valor(item_da.get("compra"))
                v_da = normalizar_valor(item_da.get("venta"))
                if not v_da or v_da < 900 or v_da > 5000:
                    continue
                cod_da = None
                if casa == "blue":
                    cod_da = "DOLAR_BLUE"
                elif casa == "oficial":
                    cod_da = "DOLAR_OFICIAL"
                elif casa == "mayorista":
                    cod_da = "DOLAR_A3500"
                if cod_da:
                    prev_v = res[cod_da].get("venta") or v_da
                    var_da = res[cod_da].get("variacion", "0,00%")
                    if prev_v and prev_v > 0 and abs(v_da - prev_v) >= 0.01:
                        var_da = _fmt_var_dolar(((v_da - prev_v) / prev_v) * 100.0, var_da)
                    res[cod_da] = {
                        "compra": round(c_da, 2) if (c_da and c_da > 900) else (round(v_da, 2) if cod_da == "DOLAR_A3500" else res[cod_da].get("compra")),
                        "venta": round(v_da, 2),
                        "variacion": var_da,
                        "fecha": hora_vivo
                    }
                    BASE_DOLARITO_EXACTA[cod_da] = dict(res[cod_da])
    except Exception:
        pass

    # 3. Asegurar que MEP y CCL queden blindados con los valores de AL30/Rava
    for k_rv in ("DOLAR_MEP", "DOLAR_CCL"):
        if k_rv in datos_rava and datos_rava[k_rv].get("venta"):
            res[k_rv] = dict(datos_rava[k_rv])
            BASE_DOLARITO_EXACTA[k_rv] = dict(datos_rava[k_rv])

    # 4. Dólar Cripto dinámico
    try:
        r_cy = GLOBAL_HTTP_SESSION.get(f"https://criptoya.com/api/usdt/ars/0.5?_t={ts_ms}", headers=headers, timeout=2.5)
        if r_cy.status_code == 200:
            d_cy = r_cy.json()
            pares_validos = []
            for ex_k, ex_obj in d_cy.items():
                if isinstance(ex_obj, dict):
                    b_v = normalizar_valor(ex_obj.get("totalBid") or ex_obj.get("bid"))
                    a_v = normalizar_valor(ex_obj.get("totalAsk") or ex_obj.get("ask"))
                    if b_v and a_v and 1400 <= b_v <= 1900 and 1400 <= a_v <= 1900 and a_v >= b_v:
                        pares_validos.append((b_v, a_v))
            if pares_validos and not dolarito_cripto_actualizado:
                pares_validos.sort(key=lambda x: abs(x[1] - 1658.43))
                mejores = pares_validos[:4]
                c_vivo = round(sum(p[0] for p in mejores) / len(mejores), 2)
                v_vivo = round(sum(p[1] for p in mejores) / len(mejores), 2)
                if v_vivo <= c_vivo:
                    v_vivo = round(c_vivo + 3.70, 2)
                chg_c = ((v_vivo - 1649.35) / 1649.35) * 100
                res["DOLAR_CRIPTO"] = {
                    "compra": c_vivo,
                    "venta": v_vivo,
                    "variacion": _fmt_var_dolar(chg_c, "+0,55%"),
                    "fecha": hora_vivo
                }
                BASE_DOLARITO_EXACTA["DOLAR_CRIPTO"] = dict(res["DOLAR_CRIPTO"])
    except Exception:
        pass

    v_of = res["DOLAR_OFICIAL"]["venta"]
    if v_of and v_of > 1000:
        res["DOLAR_TARJETA"]["venta"] = round(v_of * 1.30, 2)
        res["DOLAR_NETFLIX"]["venta"] = round(v_of * 1.51, 2)

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
    puntas_init = obtener_puntas_dolarito()
    info_may = puntas_init.get("DOLAR_A3500") or {}
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
    pares_fx = [
        ("BRL=X", "usdbrl"),
        ("EURUSD=X", "eurusd"),
        ("CLP=X", "usdclp")
    ]
    with ThreadPoolExecutor(max_workers=3) as ex_fx:
        res_fx = dict(zip([p[0] for p in pares_fx], ex_fx.map(lambda p: fetch_quote_externa(p[0], p[1]), pares_fx)))

    q_brl = res_fx.get("BRL=X") or {}
    q_eur = res_fx.get("EURUSD=X") or {}
    q_clp = res_fx.get("CLP=X") or {}

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
        if cod_d in puntas_init:
            p_info = puntas_init[cod_d]
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

def fetch_una_curva_sjb(clave: str, url: str) -> tuple:
    ts_ms = int(time.time() * 1000)
    sep = "&" if "?" in url else "?"
    urls_try = [f"{url}{sep}_t={ts_ms}"]
    if clave == "cer":
        urls_try = [
            f"https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=BONOS%20CER&moneda=ARS&_t={ts_ms}",
            f"https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=CER&moneda=ARS&_t={ts_ms}",
            f"https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=Bonos%20CER&moneda=ARS&_t={ts_ms}"
        ]
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
    return clave, None

def fetch_inflacion_implicita_sjb():
    for u in [
        "https://sjbdashboards.up.railway.app/api/bonos/inflacion-implicita",
        "https://sjbdashboards.up.railway.app/api/inflacion-implicita",
        "https://sjbdashboards.up.railway.app/api/intradiario/inflacion-implicita"
    ]:
        try:
            r = GLOBAL_HTTP_SESSION.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=5.0)
            if r.status_code == 200 and r.json():
                return r.json()
        except Exception:
            pass
    # Cálculo dinámico de break-even en vivo desde curvas Lecaps y CER cacheadas
    lecaps = (CACHE_CURVAS_SJB.get("lecaps") or {}).get("tabla") or []
    cer = (CACHE_CURVAS_SJB.get("cer") or {}).get("tabla") or []
    if lecaps and cer:
        meses_proy = [
            ("Nov 2026", 15), ("Dec 2026", 45), ("Jan 2027", 75), ("Feb 2027", 105),
            ("Mar 2027", 135), ("Apr 2027", 165), ("May 2027", 195), ("Jun 2027", 225), ("Jul 2027", 255)
        ]
        def _interp_tem(tabla, dias_obj):
            pts = []
            for row in tabla:
                d = normalizar_valor(row.get("Dias") or row.get("dias"))
                tem = normalizar_valor(row.get("TEM") or row.get("tem"))
                if tem is None:
                    tir = normalizar_valor(row.get("TIR") or row.get("tir"))
                    if tir is not None:
                        tem = ((1 + tir / 100.0) ** (1 / 12.0) - 1) * 100.0
                if d is not None and tem is not None:
                    pts.append((d, tem))
            if not pts:
                return None
            pts.sort(key=lambda x: abs(x[0] - dias_obj))
            return round(pts[0][1], 2)

        serie = [
            {"mes": "Jun 2026", "tipo": "real", "ipc_real": 1.89, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Jul 2026", "tipo": "real", "ipc_real": 2.11, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Aug 2026", "tipo": "real", "ipc_real": 1.66, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Sep 2026", "tipo": "real", "ipc_real": 1.93, "inf_implicita": None, "tem_nominal": None, "tem_cer": None},
            {"mes": "Oct 2026", "tipo": "real", "ipc_real": 2.10, "inf_implicita": None, "tem_nominal": None, "tem_cer": None}
        ]
        for m_lbl, d_obj in meses_proy:
            t_nom = _interp_tem(lecaps, d_obj)
            t_cer = _interp_tem(cer, d_obj)
            if t_nom is not None and t_cer is not None:
                be = round((((1 + t_nom / 100.0) / (1 + t_cer / 100.0)) - 1) * 100.0, 2)
                serie.append({
                    "mes": m_lbl, "tipo": "implicita", "ipc_real": None,
                    "inf_implicita": be, "tem_nominal": t_nom, "tem_cer": t_cer
                })
        if len(serie) >= 8:
            return {"series": serie}
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
        r = GLOBAL_HTTP_SESSION.get("https://api.marketdata.mae.com.ar/api/mercado/estado", headers=headers_mae_a3(), timeout=4.0, verify=False)
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
            r = sess.get(u, headers=hdrs, timeout=4.5, verify=False)
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
        return fut.result(timeout=10)
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

            ccl_actual = 1671.58
            for it_ind in res_ind:
                if it_ind.get("variable") == "DOLAR_CCL" and it_ind.get("valor", 0) > 1000:
                    ccl_actual = float(it_ind["valor"])
                    break

            msci_vivo = obtener_msci_merval_en_vivo(ccl_actual)
            indices_map["MERVAL_ARS"] = msci_vivo["MERVAL_ARS"]
            indices_map["MERVAL_USD"] = msci_vivo["MERVAL_USD"]
            if msci_vivo["RIESGO_PAIS"].get("_live"):
                indices_map["RIESGO_PAIS"] = msci_vivo["RIESGO_PAIS"]
            else:
                rp_ind = next((x for x in res_ind if x.get("variable") == "RIESGO_PAIS" and x.get("valor", 0) > 100), None)
                if rp_ind:
                    rp_ind["subcategoria"] = "argentina"
                    rp_ind["valor_formateado"] = f"{int(round(float(rp_ind['valor']))):,} pb".replace(",", ".")
                    indices_map["RIESGO_PAIS"] = rp_ind
                else:
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
