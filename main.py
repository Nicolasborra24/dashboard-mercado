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
    "inflación rem": ("INFLACION_REM", "Inflación Esperada REM (12m)", "inflacion_y_ajuste"),
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
CACHE_BANDAS_CAMBIARIAS: Dict[str, Any] = {"banda_inferior": 703.82, "banda_superior": 1974.20, "dolar_mayorista": 1545.50}
CACHE_LICITACIONES_MAE: List[Dict[str, Any]] = []

CACHE_TASAS_PERSISTENTE: Dict[str, Dict[str, Any]] = {
    "TASA_CALL": {"variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "21,66% TNA", "variacion": "0,0%", "valor": 21.66, "fecha": "Mercado"},
    "TASA_TM20": {"variable": "TASA_TM20", "descripcion": "Tasa TM20 en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "22,00% TNA", "variacion": "0,0%", "valor": 22.0, "fecha": "Mercado"},
    "TASA_PASES": {"variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA", "categoria": "tasas_y_monetario", "valor_formateado": "22,06% TNA", "variacion": "0,0%", "valor": 22.06, "fecha": "Mercado"}
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
        r = requests.get(url, headers=h, timeout=2.8)
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
        r_s = requests.get(url_s, headers=h, timeout=2.5)
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
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
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
                                fecha_txt = c[3].get_text(strip=True)
                                mes_corte = extraer_mes_corte_legible(fecha_txt)
                                desc_final = desc
                                if cod == "INFLACION_MENSUAL" and mes_corte:
                                    desc_final = f"Inflación Mensual ({mes_corte})"
                                elif cod == "INFLACION_INTERANUAL" and mes_corte:
                                    desc_final = f"Inflación Interanual ({mes_corte})"

                                item_dict = {
                                    "variable": cod,
                                    "descripcion": desc_final,
                                    "categoria": cat,
                                    "valor_formateado": c[1].get_text(strip=True),
                                    "variacion": c[2].get_text(strip=True),
                                    "valor": v_num,
                                    "fecha": f"Corte: {mes_corte}" if mes_corte and cod in ("INFLACION_MENSUAL", "INFLACION_INTERANUAL") else fecha_txt
                                }
                                if cat == "indices":
                                    item_dict["subcategoria"] = "argentina"
                                items.append(item_dict)
                                cargadas.add(v_map[0])
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
            r = requests.get(url, headers=headers, timeout=4.5, verify=False)
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

def obtener_datos_tasas_sjb() -> List[Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = requests.get("https://sjbdashboards.up.railway.app/api/tasas/tablas", headers={"User-Agent": "Mozilla/5.0"}, timeout=4.5)
        if r.status_code == 200:
            d = r.json()
            for row in d.get("principales", []):
                if "TM20" in str(row.get("Tasa", "")).upper():
                    tna_val = row.get("TNA", "")
                    CACHE_TASAS_PERSISTENTE["TASA_TM20"] = {
                        "variable": "TASA_TM20", "descripcion": f"Tasa TM20 (TEA {row.get('TEA', '')})",
                        "categoria": "tasas_y_monetario", "valor_formateado": f"{tna_val} TNA" if "TNA" not in str(tna_val).upper() else str(tna_val),
                        "variacion": "0,0%", "valor": normalizar_valor(tna_val) or 22.0, "fecha": fec_hoy
                    }
            for row in d.get("otras", []):
                nom_t = str(row.get("Tasa", "")).lower()
                tna_val = row.get("TNA", "")
                v_n = normalizar_valor(tna_val)
                if "call" in nom_t and v_n is not None:
                    CACHE_TASAS_PERSISTENTE["TASA_CALL"] = {
                        "variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos",
                        "categoria": "tasas_y_monetario", "valor_formateado": f"{tna_val} TNA" if "TNA" not in str(tna_val).upper() else str(tna_val),
                        "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy
                    }
                elif "pase" in nom_t and v_n is not None:
                    CACHE_TASAS_PERSISTENTE["TASA_PASES"] = {
                        "variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA",
                        "categoria": "tasas_y_monetario", "valor_formateado": f"{tna_val} TNA" if "TNA" not in str(tna_val).upper() else str(tna_val),
                        "variacion": "0,0%", "valor": v_n, "fecha": fec_hoy
                    }
    except Exception:
        pass
    return list(CACHE_TASAS_PERSISTENTE.values())

def obtener_datos_dolar_futuro() -> Optional[Dict[str, Any]]:
    meses_es = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    now = datetime.utcnow()
    nombre_mes = f"{meses_es[now.month - 1]} {now.year}"
    precio_fut = 1570.50
    try:
        r = requests.get("https://www.dolarito.ar/", headers={"User-Agent": "Mozilla/5.0"}, timeout=3.0)
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
        "categoria": "dolar_y_cambiario", "valor_formateado": f"${precio_fut:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "variacion": "-0,19%", "valor": precio_fut, "fecha": f"Vto. {nombre_mes}"
    }

def obtener_puntas_dolarito() -> Dict[str, Dict[str, Any]]:
    res: Dict[str, Dict[str, Any]] = {}
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")

    # 1. Consulta directa a Dolarito.ar (API interna y __NEXT_DATA__)
    urls_dolarito = [
        "https://api.dolarito.ar/api/frontend/quotations",
        "https://www.dolarito.ar/api/frontend/quotations",
        "https://www.dolarito.ar/"
    ]
    for u in urls_dolarito:
        try:
            r = requests.get(u, headers=headers, timeout=3.5)
            if r.status_code != 200:
                continue
            data_json = None
            if "application/json" in r.headers.get("Content-Type", ""):
                data_json = r.json()
            else:
                m_next = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.DOTALL)
                if m_next:
                    data_json = json.loads(m_next.group(1))
            if data_json:
                nodos = []
                def buscar_nodos(obj):
                    if isinstance(obj, dict):
                        nodos.append(obj)
                        for v in obj.values():
                            buscar_nodos(v)
                    elif isinstance(obj, list):
                        for el in obj:
                            buscar_nodos(el)
                buscar_nodos(data_json)
                for nd in nodos:
                    nombre = str(nd.get("name") or nd.get("nombre") or nd.get("slug") or nd.get("id") or "").lower()
                    c = normalizar_valor(nd.get("buy") or nd.get("compra"))
                    v = normalizar_valor(nd.get("sell") or nd.get("venta"))
                    var = nd.get("variation") or nd.get("variacion")
                    if c and v and c > 500 and v > 500:
                        var_str = f"{float(var):+.2f}%".replace(".", ",") if isinstance(var, (int, float)) else (str(var) if var else "0,0%")
                        if any(k in nombre for k in ["oficial", "minorista", "bna", "banco nacion"]) and "DOLAR_OFICIAL" not in res:
                            res["DOLAR_OFICIAL"] = {"compra": round(c, 2), "venta": round(v, 2), "variacion": var_str, "fecha": fec_hoy}
                        elif any(k in nombre for k in ["blue", "informal", "libre"]) and "DOLAR_BLUE" not in res:
                            res["DOLAR_BLUE"] = {"compra": round(c, 2), "venta": round(v, 2), "variacion": var_str, "fecha": fec_hoy}
            if "DOLAR_OFICIAL" in res and "DOLAR_BLUE" in res:
                break
        except Exception:
            pass

    # 2. Respaldo en tiempo real con DolarApi si algún campo no vino en el HTML
    if "DOLAR_OFICIAL" not in res or "DOLAR_BLUE" not in res:
        try:
            r_api = requests.get("https://dolarapi.com/v1/dolares", headers=headers, timeout=3.5)
            if r_api.status_code == 200:
                for d in r_api.json():
                    casa = str(d.get("casa", "")).lower()
                    c = normalizar_valor(d.get("compra"))
                    v = normalizar_valor(d.get("venta"))
                    if c and v:
                        if casa == "oficial" and "DOLAR_OFICIAL" not in res:
                            res["DOLAR_OFICIAL"] = {"compra": round(c, 2), "venta": round(v, 2), "variacion": "0,0%", "fecha": fec_hoy}
                        elif casa == "blue" and "DOLAR_BLUE" not in res:
                            res["DOLAR_BLUE"] = {"compra": round(c, 2), "venta": round(v, 2), "variacion": "0,0%", "fecha": fec_hoy}
        except Exception:
            pass

    return res

def obtener_datos_monedas_y_dolar() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    usd_oficial = 1015.50
    try:
        r_a = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=3.0)
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
        r_fx = requests.get("https://open.er-api.com/v6/latest/USD", timeout=3.0)
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

    puntas = obtener_puntas_dolarito()
    for cod_d, desc_d in [("DOLAR_OFICIAL", "Dólar Oficial Minorista"), ("DOLAR_BLUE", "Dólar Blue")]:
        if cod_d in puntas:
            p_info = puntas[cod_d]
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
                "variacion": p_info.get("variacion", "0,0%"),
                "valor": v_val,
                "fecha": p_info.get("fecha", fec_hoy)
            })
    return items

def obtener_datos_bonos_sjb() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    try:
        r = requests.get("https://sjbdashboards.up.railway.app/api/bonos/precios-recientes", headers={"User-Agent": "Mozilla/5.0"}, timeout=3.0)
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

def fetch_una_curva_sjb(clave: str, url: str) -> tuple:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5.0)
        if r.status_code == 200:
            norm = normalizar_respuesta_curva(r.json())
            if norm:
                return clave, norm
    except Exception:
        pass
    return clave, None

def fetch_inflacion_implicita_sjb():
    for u in ["https://sjbdashboards.up.railway.app/api/bonos/inflacion-implicita", "https://sjbdashboards.up.railway.app/api/inflacion-implicita"]:
        try:
            r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=5.0)
            if r.status_code == 200 and r.json():
                return r.json()
        except Exception:
            pass
    return None

def fetch_bandas_dolarito() -> Dict[str, Any]:
    b_inf, b_sup, d_may = 703.82, 1974.20, 1545.50
    try:
        h = {"User-Agent": "Mozilla/5.0"}
        r1 = requests.get("https://api.dolarito.ar/api/frontend/indices/bandaInferior", headers=h, timeout=3.0)
        if r1.status_code == 200:
            b_inf = float(r1.json().get("value") or r1.json().get("valor") or b_inf)
        r2 = requests.get("https://api.dolarito.ar/api/frontend/indices/bandaSuperior", headers=h, timeout=3.0)
        if r2.status_code == 200:
            b_sup = float(r2.json().get("value") or r2.json().get("valor") or b_sup)
        r_m = requests.get("https://dolarapi.com/v1/dolares/mayorista", headers=h, timeout=3.0)
        if r_m.status_code == 200:
            d_may = float(r_m.json().get("venta") or r_m.json().get("compra") or d_may)
    except Exception:
        pass
    return {"banda_inferior": round(b_inf, 2), "banda_superior": round(b_sup, 2), "dolar_mayorista": round(d_may, 2)}

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
        r = requests.get("https://finviz.com/groups?g=sector&v=210&o=name", headers=headers, timeout=5.0)
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

def fetch_licitaciones_mae() -> List[Dict[str, Any]]:
    try:
        r = requests.get("https://api.marketdata.mae.com.ar/api/mercado/licitaciones", headers={"User-Agent": "Mozilla/5.0"}, timeout=4.0)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else (data.get("licitaciones") or data.get("data") or [data])
    except Exception:
        pass
    return []

def worker_refresco_continuo():
    global CACHE_VARIABLES, CACHE_CURVAS_SJB, CACHE_INFLACION_IMPLICITA, CACHE_BANDAS_CAMBIARIAS, CACHE_LICITACIONES_MAE, CACHE_FINVIZ_SECTORES, ULTIMA_ACTUALIZACION

    while True:
        try:
            with ThreadPoolExecutor(max_workers=16) as executor:
                f_ind = executor.submit(obtener_datos_indicadores)
                f_bcra_inf = executor.submit(obtener_inflacion_oficial_bcra)
                f_tasas = executor.submit(obtener_datos_tasas_sjb)
                f_monedas = executor.submit(obtener_datos_monedas_y_dolar)
                f_bonos = executor.submit(obtener_datos_bonos_sjb)
                f_usa = executor.submit(obtener_datos_mercado_usa)
                f_inf_imp = executor.submit(fetch_inflacion_implicita_sjb)
                f_bandas = executor.submit(fetch_bandas_dolarito)
                f_futuro = executor.submit(obtener_datos_dolar_futuro)
                f_metales = executor.submit(obtener_datos_metales_en_vivo)
                f_petroleo = executor.submit(obtener_datos_petroleo_en_vivo)
                f_licit = executor.submit(fetch_licitaciones_mae)

                futures_curvas = [executor.submit(fetch_una_curva_sjb, k, url) for k, url in SJB_CURVAS_URLS.items()]

                res_ind = f_ind.result()
                res_bcra_inf = f_bcra_inf.result()
                res_tasas = f_tasas.result()
                res_monedas = f_monedas.result()
                res_bonos = f_bonos.result()
                res_usa = f_usa.result()
                res_inf_imp = f_inf_imp.result()
                res_bandas = f_bandas.result()
                res_futuro = f_futuro.result()
                res_metales = f_metales.result()
                res_petroleo = f_petroleo.result()
                res_licit = f_licit.result()

                for fut in futures_curvas:
                    k, data = fut.result()
                    if data:
                        CACHE_CURVAS_SJB[k] = data

            res_finviz = fetch_finviz_sectores(res_usa)

            if res_inf_imp: CACHE_INFLACION_IMPLICITA = res_inf_imp
            if res_bandas: CACHE_BANDAS_CAMBIARIAS = res_bandas
            if res_licit is not None: CACHE_LICITACIONES_MAE = res_licit
            if res_finviz: CACHE_FINVIZ_SECTORES = res_finviz

            indices_map = {item["variable"]: dict(item) for item in INDICES_BASE_MEMORIA}
            for item in res_usa:
                indices_map[item["variable"]] = item
            for item in res_ind:
                if item.get("categoria") == "indices":
                    indices_map[item["variable"]] = item

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
            for cod_p, desc_p in [("DOLAR_OFICIAL", "Dólar Oficial Minorista"), ("DOLAR_BLUE", "Dólar Blue")]:
                if cod_p in puntas_dl:
                    c_v = puntas_dl[cod_p]["compra"]
                    v_v = puntas_dl[cod_p]["venta"]
                    fmt_v = f"${v_v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    encontrado_d = False
                    for item_d in nuevo_agrupado["dolar_y_cambiario"]:
                        if item_d.get("variable") == cod_p:
                            item_d["compra"] = c_v
                            item_d["venta"] = v_v
                            item_d["valor"] = v_v
                            item_d["valor_formateado"] = fmt_v
                            encontrado_d = True
                            break
                    if not encontrado_d:
                        nuevo_agrupado["dolar_y_cambiario"].append({
                            "variable": cod_p, "descripcion": desc_p, "categoria": "dolar_y_cambiario",
                            "compra": c_v, "venta": v_v, "valor": v_v, "valor_formateado": fmt_v,
                            "variacion": "0,0%", "fecha": puntas_dl[cod_p]["fecha"]
                        })

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

@app.get("/bcra/variables")
def ver_bcra(formato: str = Query("agrupado")):
    with CACHE_LOCK:
        if formato == "lista":
            lista_plana = []
            for v in CACHE_VARIABLES.values():
                lista_plana.extend(v)
            return lista_plana
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
        r = requests.get("https://dolarapi.com/v1/dolares", timeout=3.5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {"error": "Servicio de divisas ocupado"}

@app.get("/mercado/resumen")
def ver_resumen_mercado():
    try:
        r = requests.get("https://dolarapi.com/v1/dolares", timeout=3.5)
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
        r = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=6&sort=desc", timeout=3.5)
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
