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
    sess = requests.Session()
    sess.mount("https://", MAEAdapter())
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
    "cer": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=CER&moneda=ARS",
    "soberanos": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=MEP",
    "soberanos_mep": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=MEP",
    "soberanos_ars": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=SOBERANOS&moneda=ARS",
    "duales": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=DUALES&moneda=ARS&incluir_cer_tamar=true&incluir_dl_tamar=true",
    "tamar": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=LETAM&moneda=ARS",
    "dollar_linked": "https://sjbdashboards.up.railway.app/api/intradiario/tabla?categoria=D%C3%B3lar%20Linked&moneda=ARS"
}

CACHE_CURVAS_SJB: Dict[str, Any] = {}
CACHE_INFLACION_IMPLICITA: Any = None
CACHE_BANDAS_CAMBIARIAS: Dict[str, Any] = {"banda_inferior": 703.82, "banda_superior": 1974.20, "dolar_mayorista": 1544.00}
CACHE_LICITACIONES_MAE: List[Dict[str, Any]] = []

CACHE_TASAS_PERSISTENTE: Dict[str, Dict[str, Any]] = {
    "TASA_CALL": {"variable": "TASA_CALL", "descripcion": "Tasa Call Interbancaria en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "21,66% TNA", "variacion": "0,0%", "valor": 21.66, "fecha": "Mercado"},
    "TASA_TM20": {"variable": "TASA_TM20", "descripcion": "Tasa TM20 en Pesos", "categoria": "tasas_y_monetario", "valor_formateado": "22,00% TNA", "variacion": "0,0%", "valor": 22.0, "fecha": "Mercado"},
    "TASA_PASES": {"variable": "TASA_PASES", "descripcion": "Tasa de Pases Pasivos BCRA", "categoria": "tasas_y_monetario", "valor_formateado": "22,06% TNA", "variacion": "0,0%", "valor": 22.06, "fecha": "Mercado"},
    "TASA_CAUCION_1D": {"variable": "TASA_CAUCION_1D", "descripcion": "Caución Bursátil 1 Día (Pesos)", "categoria": "tasas_y_monetario", "valor_formateado": "9,50% TNA", "variacion": "-63,6%", "var_mes": "-47,2%", "var_anio": "-92,1%", "valor": 9.50, "fecha": "Mes: -47,2% | Año: -92,1%"},
    "TASA_CAUCION_7D": {"variable": "TASA_CAUCION_7D", "descripcion": "Caución Bursátil 7 Días (Pesos)", "categoria": "tasas_y_monetario", "valor_formateado": "19,00% TNA", "variacion": "-57,8%", "var_mes": "-2,1%", "var_anio": "-13,6%", "valor": 19.00, "fecha": "Mes: -2,1% | Año: -13,6%"},
    "TASA_CAUCION_14D": {"variable": "TASA_CAUCION_14D", "descripcion": "Caución Bursátil 14 Días (Pesos)", "categoria": "tasas_y_monetario", "valor_formateado": "20,90% TNA", "variacion": "-32,4%", "var_mes": "+8,9%", "var_anio": "-5,0%", "valor": 20.90, "fecha": "Mes: +8,9% | Año: -5,0%"}
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

def obtener_msci_merval_en_vivo(ccl_ref: float = 1669.0) -> Dict[str, Dict[str, Any]]:
    datos = {
        "MERVAL_ARS": {"ult": 83482.0, "dia": -0.7, "mes": -9.3, "anio": -22.1},
        "MERVAL_USD": {"ult": 50.0, "dia": -1.3, "mes": -10.0, "anio": -28.7},
        "RIESGO_PAIS": {"ult": 852.0, "dia": -0.1, "mes": 8.3, "anio": 49.2}
    }

    try:
        r = requests.get(
            "https://www.rava.com/",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"},
            timeout=4.5
        )
        if r.status_code == 200 and r.text:
            html_u = r.text.replace("&quot;", '"')
            # A) Extraer desde el JSON embebido (trae ultimo, variacion diaria, var_mes / mes y var_anio / anio)
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

                if k_target == "RIESGO_PAIS" and vu and 200 <= vu <= 5000:
                    datos["RIESGO_PAIS"]["ult"] = vu
                elif k_target == "MERVAL_USD" and vu and 20 <= vu <= 300:
                    datos["MERVAL_USD"]["ult"] = vu
                elif k_target == "MERVAL_ARS" and vu and 40000 <= vu <= 250000:
                    datos["MERVAL_ARS"]["ult"] = vu

                if vd is not None and -20.0 <= vd <= 20.0:
                    datos[k_target]["dia"] = vd
                if vm is not None and -60.0 <= vm <= 150.0:
                    datos[k_target]["mes"] = vm
                if va is not None and -80.0 <= va <= 300.0:
                    datos[k_target]["anio"] = va

            # B) Extraer desde las tablas renderizadas (ESPECIE | ÚLTIMO | % DÍA | % MES | % AÑO)
            plano = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            patrones_tabla = [
                ("MERVAL_ARS", r'\bMERVAL\b(?!\s*USD)\s+([\d]{2,3}\.[\d]{3}(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)'),
                ("MERVAL_USD", r'\bMERVAL\s+USD\b\s+([\d]{2,3}(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)'),
                ("RIESGO_PAIS", r'\bRIESGO\s+PA[IÍ]S\b\s+([\d]{3,4}(?:,[\d]+)?)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)')
            ]
            for k_t, pat in patrones_tabla:
                mt = re.search(pat, plano, re.IGNORECASE)
                if mt:
                    u_v = _parse_float_limpio(mt.group(1))
                    d_v = _parse_float_limpio(mt.group(2))
                    m_v = _parse_float_limpio(mt.group(3))
                    a_v = _parse_float_limpio(mt.group(4))
                    if u_v: datos[k_t]["ult"] = u_v
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
            "fecha": f"Mes: {var_mes} | Año: {var_anio}"
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
    ("AGRO_SOJA", "Soja (Pizarra Rosario / Matba)", "ZS=F", "zs.f", 0.367437, 0.72, 298.50),
    ("AGRO_MAIZ", "Maíz (Pizarra Rosario / Matba)", "ZC=F", "zc.f", 0.393680, 1.08, 186.00),
    ("AGRO_TRIGO", "Trigo (Pizarra Rosario / Matba)", "ZW=F", "zw.f", 0.367437, 0.96, 208.50),
    ("AGRO_GIRASOL", "Girasol (Pizarra Rosario / Matba)", None, None, 1.0, 1.0, 365.00),
    ("AGRO_SORGO", "Sorgo (Pizarra Rosario / Matba)", "ZC=F", "zc.f", 0.393680, 0.92, 162.00),
]

def obtener_agro_en_vivo(tc_mayorista: float = 1545.50) -> Dict[str, Dict[str, Any]]:
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    tc = tc_mayorista if tc_mayorista and tc_mayorista > 800 else 1545.50

    pizarras_ars = {}
    pizarras_var = {}
    try:
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers={"User-Agent": "Mozilla/5.0"}, timeout=4.0)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            mapa_nom = {"soja": "AGRO_SOJA", "maiz": "AGRO_MAIZ", "trigo": "AGRO_TRIGO", "girasol": "AGRO_GIRASOL", "sorgo": "AGRO_SORGO"}
            for tr in soup.find_all("tr"):
                c = tr.find_all(["td", "th"])
                if len(c) >= 4:
                    nom = c[0].get_text(strip=True).lower().replace("í", "i")
                    if nom in mapa_nom:
                        cod_g = mapa_nom[nom]
                        v_n = normalizar_valor(c[1].get_text(strip=True))
                        if v_n and v_n > 100:
                            pizarras_ars[cod_g] = v_n
                            pizarras_var[cod_g] = c[2].get_text(strip=True)
    except Exception:
        pass

    def worker_grano(info):
        cod, desc, sym_y, sym_s, factor_tn, ratio_local, def_usd = info
        chg_pct = 0.0
        usd_cbot = None
        if sym_y:
            q = fetch_quote_externa(sym_y, sym_s)
            if q and q.get("price"):
                p_raw = float(q["price"])
                if p_raw > 150:
                    usd_cbot = round((p_raw / 100.0) / factor_tn * ratio_local, 2)
                chg_pct = float(q.get("change_pct", 0.0))

        v_raw = pizarras_ars.get(cod)
        if v_raw:
            if v_raw > 50000:
                precio_ars = round(v_raw, 2)
                precio_usd = round(precio_ars / tc, 2)
            else:
                precio_usd = round(v_raw, 2)
                precio_ars = round(precio_usd * tc, 2)
        elif usd_cbot and usd_cbot > 100:
            precio_usd = usd_cbot
            precio_ars = round(precio_usd * tc, 2)
        else:
            precio_usd = def_usd
            precio_ars = round(precio_usd * tc, 2)

        fmt_ars = f"$ {precio_ars:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        fmt_usd = f"US$ {precio_usd:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        if chg_pct != 0.0:
            var_str = f"+{chg_pct:.2f}%".replace(".", ",") if chg_pct > 0 else f"{chg_pct:.2f}%".replace(".", ",")
        else:
            var_str = pizarras_var.get(cod) or "0,00%"

        return cod, {
            "variable": cod,
            "descripcion": desc,
            "categoria": "commodities_agro_energia",
            "precio_ars": fmt_ars,
            "precio_usd": fmt_usd,
            "valor_ars": precio_ars,
            "valor_usd": precio_usd,
            "valor_formateado": f"{fmt_ars} / {fmt_usd}",
            "variacion": var_str,
            "valor": precio_usd,
            "fecha": f"{fec_hoy} • En vivo"
        }

    with ThreadPoolExecutor(max_workers=5) as ex:
         pares = list(ex.map(worker_grano, AGRO_GRANOS_BASE))
    return {k: v for k, v in pares}

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

def _actualizar_cauciones_en_vivo():
    base_c = {
        "1D": {"desc": "Caución Bursátil 1 Día (Pesos)", "tasa": 9.50, "dia": -63.6, "mes": -47.2, "anio": -92.1},
        "7D": {"desc": "Caución Bursátil 7 Días (Pesos)", "tasa": 19.00, "dia": -57.8, "mes": -2.1, "anio": -13.6},
        "14D": {"desc": "Caución Bursátil 14 Días (Pesos)", "tasa": 20.90, "dia": -32.4, "mes": 8.9, "anio": -5.0}
    }
    try:
        r = requests.get(
            "https://www.rava.com/",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"},
            timeout=4.5
        )
        if r.status_code == 200 and r.text:
            html_u = r.text.replace("&quot;", '"')
            for m_obj in re.finditer(r'\{[^{}]*"(?:simbolo|especie|nombre)"\s*:\s*"CAUCION\s*(1D|7D|14D)"[^{}]*\}', html_u, re.IGNORECASE):
                b = m_obj.group(0)
                plz = m_obj.group(1).upper()
                m_u = re.search(r'"ultimo"\s*:\s*"?([+\-]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_d = re.search(r'"(?:variacion|var_dia|dia|change)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_m = re.search(r'"(?:var_mes|mes|mensual|var_mtd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)
                m_a = re.search(r'"(?:var_anio|anio|anual|var_ytd|ytd)"\s*:\s*"?([+\-−]?[\d\.,]+)"?', b, re.IGNORECASE)

                vu = normalizar_valor(m_u.group(1)) if m_u else None
                vd = normalizar_valor(m_d.group(1)) if m_d else None
                vm = normalizar_valor(m_m.group(1)) if m_m else None
                va = normalizar_valor(m_a.group(1)) if m_a else None

                if vu is not None and 0.5 <= vu <= 150.0:
                    base_c[plz]["tasa"] = vu
                if vd is not None and -99.0 <= vd <= 200.0:
                    base_c[plz]["dia"] = vd
                if vm is not None and -99.0 <= vm <= 200.0:
                    base_c[plz]["mes"] = vm
                if va is not None and -99.0 <= va <= 200.0:
                    base_c[plz]["anio"] = va

            plano = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            for plz in ("1D", "7D", "14D"):
                mt = re.search(
                    r'\bCAUCION\s+' + plz + r'\b\s+([\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)\s+([+\-−]?[\d]+,[\d]+)',
                    plano, re.IGNORECASE
                )
                if mt:
                    u_v = normalizar_valor(mt.group(1))
                    d_v = normalizar_valor(mt.group(2))
                    m_v = normalizar_valor(mt.group(3))
                    a_v = normalizar_valor(mt.group(4))
                    if u_v is not None: base_c[plz]["tasa"] = u_v
                    if d_v is not None: base_c[plz]["dia"] = d_v
                    if m_v is not None: base_c[plz]["mes"] = m_v
                    if a_v is not None: base_c[plz]["anio"] = a_v
    except Exception:
        pass

    def _f_pct(v):
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
    _actualizar_cauciones_en_vivo()
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

    # 1. Consulta en vivo a Dolarito.ar (quotations + __NEXT_DATA__)
    for u in ["https://api.dolarito.ar/api/frontend/quotations", "https://www.dolarito.ar/"]:
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
                def rec(o):
                    if isinstance(o, dict):
                        nodos.append(o)
                        for v in o.values(): rec(v)
                    elif isinstance(o, list):
                        for el in o: rec(el)
                rec(data_json)
                for nd in nodos:
                    nom = str(nd.get("name") or nd.get("nombre") or nd.get("slug") or "").lower()
                    c = normalizar_valor(nd.get("buy") or nd.get("compra"))
                    v = normalizar_valor(nd.get("sell") or nd.get("venta") or nd.get("value") or nd.get("valor"))
                    if v and v > 500:
                        if any(k in nom for k in ["oficial", "minorista", "bna", "nacion"]) and "DOLAR_OFICIAL" not in res and c:
                            res["DOLAR_OFICIAL"] = {"compra": round(c, 2), "venta": round(v, 2), "fecha": fec_hoy}
                        elif any(k in nom for k in ["blue", "informal", "libre"]) and "DOLAR_BLUE" not in res and c:
                            res["DOLAR_BLUE"] = {"compra": round(c, 2), "venta": round(v, 2), "fecha": fec_hoy}
                        elif "tarjeta" in nom and "DOLAR_TARJETA" not in res:
                            res["DOLAR_TARJETA"] = {"compra": round(c, 2) if c else None, "venta": round(v, 2), "fecha": fec_hoy}
                        elif any(k in nom for k in ["cripto", "crypto", "usdt"]) and "DOLAR_CRIPTO" not in res:
                            res["DOLAR_CRIPTO"] = {"compra": round(c or v, 2), "venta": round(v, 2), "fecha": fec_hoy}
                        elif any(k in nom for k in ["netflix", "streaming"]) and "DOLAR_NETFLIX" not in res:
                            res["DOLAR_NETFLIX"] = {"compra": None, "venta": round(v, 2), "fecha": fec_hoy}
                        elif any(k in nom for k in ["mayorista", "a3500", "comunicacion a 3500", "mayorista bna", "mulc"]) and "DOLAR_A3500" not in res:
                            res["DOLAR_A3500"] = {"compra": round(c or v, 2), "venta": round(v, 2), "fecha": fec_hoy}
        except Exception:
            pass

    # 2. Respaldo en tiempo real con DolarApi
    try:
        r_api = requests.get("https://dolarapi.com/v1/dolares", headers=headers, timeout=3.5)
        if r_api.status_code == 200:
            for d in r_api.json():
                casa = str(d.get("casa", "")).lower()
                c = normalizar_valor(d.get("compra"))
                v = normalizar_valor(d.get("venta"))
                if v:
                    if casa == "oficial" and "DOLAR_OFICIAL" not in res and c:
                        res["DOLAR_OFICIAL"] = {"compra": round(c, 2), "venta": round(v, 2), "fecha": fec_hoy}
                    elif casa == "blue" and "DOLAR_BLUE" not in res and c:
                        res["DOLAR_BLUE"] = {"compra": round(c, 2), "venta": round(v, 2), "fecha": fec_hoy}
                    elif casa == "tarjeta" and "DOLAR_TARJETA" not in res:
                        res["DOLAR_TARJETA"] = {"compra": round(c, 2) if c else None, "venta": round(v, 2), "fecha": fec_hoy}
                    elif casa == "cripto" and "DOLAR_CRIPTO" not in res:
                        res["DOLAR_CRIPTO"] = {"compra": round(c or v, 2), "venta": round(v, 2), "fecha": fec_hoy}
    except Exception:
        pass

    # 3. Asegurar DÓLAR NETFLIX en vivo (Dolarito: Oficial Minorista Venta + 51% [21% IVA + 30% Percepción])
    if "DOLAR_NETFLIX" not in res:
        v_of = (res.get("DOLAR_OFICIAL") or {}).get("venta") or 1570.00
        res["DOLAR_NETFLIX"] = {
            "compra": None,
            "venta": round(float(v_of) * 1.51, 2),
            "fecha": fec_hoy
        }

    if "DOLAR_A3500" not in res:
        try:
            r_html = requests.get("https://www.dolarito.ar/", headers=headers, timeout=3.5)
            if r_html.status_code == 200:
                m_may = re.search(r'mayorista[^\d]{1,60}(1[\.,]?\d{3}(?:[\.,]\d{1,2})?)', r_html.text, re.IGNORECASE)
                if m_may:
                    v_m = normalizar_valor(m_may.group(1))
                    if v_m and 1300 < v_m < 2500:
                        res["DOLAR_A3500"] = {"compra": round(v_m, 2), "venta": round(v_m, 2), "fecha": fec_hoy}
        except Exception:
            pass
    if "DOLAR_A3500" not in res:
        res["DOLAR_A3500"] = {"compra": 1544.00, "venta": 1544.00, "fecha": fec_hoy}

    return res

def obtener_datos_monedas_y_dolar() -> List[Dict[str, Any]]:
    items = []
    fec_hoy = datetime.utcnow().strftime("%d %b %Y")
    puntas_init = obtener_puntas_dolarito()
    usd_oficial = float((puntas_init.get("DOLAR_A3500") or {}).get("venta") or 1544.00)
    fmt_may = f"${usd_oficial:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    items.append({
        "variable": "DOLAR_A3500",
        "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
        "categoria": "dolar_y_cambiario",
        "compra": usd_oficial,
        "venta": usd_oficial,
        "valor_formateado": fmt_may,
        "variacion": "0,0%",
        "valor": usd_oficial,
        "fecha": fec_hoy
    })

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
        r = requests.get(url_cnbc, headers=headers, timeout=3.5)
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
    b_inf, b_sup, d_may = 703.82, 1974.20, 1544.00
    try:
        h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        r1 = requests.get("https://api.dolarito.ar/api/frontend/indices/bandaInferior", headers=h, timeout=3.0)
        if r1.status_code == 200:
            b_inf = float(r1.json().get("value") or r1.json().get("valor") or b_inf)
        r2 = requests.get("https://api.dolarito.ar/api/frontend/indices/bandaSuperior", headers=h, timeout=3.0)
        if r2.status_code == 200:
            b_sup = float(r2.json().get("value") or r2.json().get("valor") or b_sup)
        puntas_dl = obtener_puntas_dolarito()
        if "DOLAR_A3500" in puntas_dl and puntas_dl["DOLAR_A3500"].get("venta"):
            d_may = float(puntas_dl["DOLAR_A3500"]["venta"])
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
        r = requests.get("https://api.marketdata.mae.com.ar/api/mercado/estado", headers=headers_mae_a3(), timeout=4.0, verify=False)
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
    {"fecha": "2026-11-09", "hora": "13:00", "div": "US", "evento": "Expectativas de inflación del consumidor (Oct)", "importancia": 2, "actual": "2,9%", "prevision": "3,0%", "anterior": "3,0%"},
    {"fecha": "2026-11-09", "hora": "15:00", "div": "US", "evento": "Subasta de letras a 3 y 6 meses del Tesoro", "importancia": 2, "actual": "4,42%", "prevision": "-", "anterior": "4,45%"},
    {"fecha": "2026-11-10", "hora": "10:30", "div": "US", "evento": "IPC (Mensual) (Oct)", "importancia": 3, "actual": "0,4%", "prevision": "0,3%", "anterior": "0,6%"},
    {"fecha": "2026-11-10", "hora": "10:30", "div": "US", "evento": "IPC subyacente (Mensual) (Oct)", "importancia": 3, "actual": "0,3%", "prevision": "0,2%", "anterior": "0,3%"},
    {"fecha": "2026-11-10", "hora": "10:30", "div": "US", "evento": "IPC (Anual) (Oct)", "importancia": 3, "actual": "2,6%", "prevision": "2,6%", "anterior": "2,4%"},
    {"fecha": "2026-11-10", "hora": "10:30", "div": "US", "evento": "IPC subyacente (Anual) (Oct)", "importancia": 3, "actual": "3,3%", "prevision": "3,3%", "anterior": "3,3%"},
    {"fecha": "2026-11-10", "hora": "16:00", "div": "US", "evento": "Balance presupuestario federal (Oct)", "importancia": 2, "actual": "-", "prevision": "-225,0B", "anterior": "-64,3B"},
    {"fecha": "2026-11-11", "hora": "09:00", "div": "US", "evento": "Solicitudes de hipoteca MBA (Semanal)", "importancia": 2, "actual": "-", "prevision": "-", "anterior": "-1,8%"},
    {"fecha": "2026-11-11", "hora": "15:00", "div": "US", "evento": "Subasta de bonos del Tesoro a 10 años (UST 10Y)", "importancia": 3, "actual": "-", "prevision": "4,30%", "anterior": "4,26%"},
    {"fecha": "2026-11-12", "hora": "10:30", "div": "US", "evento": "IPP (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,2%", "anterior": "0,0%"},
    {"fecha": "2026-11-12", "hora": "10:30", "div": "US", "evento": "IPP subyacente (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,3%", "anterior": "0,2%"},
    {"fecha": "2026-11-12", "hora": "10:30", "div": "US", "evento": "Nuevas peticiones de subsidio por desempleo", "importancia": 3, "actual": "-", "prevision": "220K", "anterior": "221K"},
    {"fecha": "2026-11-12", "hora": "13:00", "div": "US", "evento": "Inventarios de petróleo crudo de la AIE", "importancia": 3, "actual": "-", "prevision": "1,100M", "anterior": "2,149M"},
    {"fecha": "2026-11-12", "hora": "17:00", "div": "US", "evento": "Comparecencia de Jerome Powell, presidente de la Fed", "importancia": 3, "actual": "-", "prevision": "-", "anterior": "-"},
    {"fecha": "2026-11-13", "hora": "10:30", "div": "US", "evento": "Ventas minoristas (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,3%", "anterior": "0,4%"},
    {"fecha": "2026-11-13", "hora": "10:30", "div": "US", "evento": "Ventas minoristas subyacentes (Mensual) (Oct)", "importancia": 3, "actual": "-", "prevision": "0,3%", "anterior": "0,5%"},
    {"fecha": "2026-11-13", "hora": "10:30", "div": "US", "evento": "Índice manufacturero Empire State (Nov)", "importancia": 2, "actual": "-", "prevision": "-0,50", "anterior": "-11,90"},
    {"fecha": "2026-11-13", "hora": "11:15", "div": "US", "evento": "Producción industrial (Mensual) (Oct)", "importancia": 2, "actual": "-", "prevision": "-0,3%", "anterior": "-0,3%"},
    {"fecha": "2026-11-17", "hora": "12:00", "div": "US", "evento": "Índice NAHB del mercado de la vivienda (Nov)", "importancia": 2, "actual": "-", "prevision": "43", "anterior": "43"},
    {"fecha": "2026-11-18", "hora": "10:30", "div": "US", "evento": "Permisos de construcción (Oct)", "importancia": 3, "actual": "-", "prevision": "1,440M", "anterior": "1,425M"},
    {"fecha": "2026-11-18", "hora": "10:30", "div": "US", "evento": "Inicios de construcción de viviendas (Oct)", "importancia": 2, "actual": "-", "prevision": "1,340M", "anterior": "1,354M"},
    {"fecha": "2026-11-19", "hora": "10:30", "div": "US", "evento": "Nuevas peticiones de subsidio por desempleo", "importancia": 3, "actual": "-", "prevision": "222K", "anterior": "220K"},
    {"fecha": "2026-11-19", "hora": "10:30", "div": "US", "evento": "Índice manufacturero de la Fed de Filadelfia (Nov)", "importancia": 3, "actual": "-", "prevision": "8,0", "anterior": "10,3"},
    {"fecha": "2026-11-19", "hora": "12:00", "div": "US", "evento": "Ventas de viviendas de segunda mano (Oct)", "importancia": 3, "actual": "-", "prevision": "3,93M", "anterior": "3,84M"},
    {"fecha": "2026-11-20", "hora": "11:45", "div": "US", "evento": "PMI manufacturero de S&P Global (Nov) (Preliminar)", "importancia": 3, "actual": "-", "prevision": "48,8", "anterior": "48,5"},
    {"fecha": "2026-11-20", "hora": "11:45", "div": "US", "evento": "PMI de servicios de S&P Global (Nov) (Preliminar)", "importancia": 3, "actual": "-", "prevision": "55,2", "anterior": "55,0"},
    {"fecha": "2026-11-20", "hora": "12:00", "div": "US", "evento": "Confianza del consumidor de la Universidad de Michigan (Nov)", "importancia": 3, "actual": "-", "prevision": "73,0", "anterior": "70,5"}
]

CACHE_CALENDARIO_INDEC: List[Dict[str, Any]] = list(CALENDARIO_INDEC_BASE)
CACHE_CALENDARIO_USA: List[Dict[str, Any]] = list(CALENDARIO_USA_BASE)

def extraer_calendario_indec() -> List[Dict[str, Any]]:
    global CACHE_CALENDARIO_INDEC
    url_js = "https://www.indec.gob.ar/Scripts/CalendarioFull.js"
    extraidos = []
    sess = requests.Session()
    sess.mount("https://", MAEAdapter())
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
            r = sess.get(u, headers=hdrs, timeout=5.0, verify=False)
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

def extraer_calendario_usa() -> List[Dict[str, Any]]:
    global CACHE_CALENDARIO_USA
    url_aws = "https://e1-0850197d5e074be58352ddc860645af6.ecs.us-east-1.on.aws/events?cee=no"
    candidatos = [
        url_aws,
        f"https://api.allorigins.win/raw?url={quote(url_aws, safe='')}",
        f"https://api.codetabs.com/v1/proxy?quest={quote(url_aws, safe='')}"
    ]
    extraidos = []
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://es.investing.com",
        "Referer": "https://es.investing.com/economic-calendar/"
    }
    for u in candidatos:
        try:
            r = requests.get(u, headers=hdrs, timeout=4.5, verify=False)
            if r.status_code == 200 and r.text.strip():
                data = r.json()
                lista = data if isinstance(data, list) else (data.get("events") or data.get("data") or data.get("items") or [])
                if isinstance(lista, list):
                    for ev in lista:
                        if not isinstance(ev, dict):
                            continue
                        cur = str(ev.get("currency") or ev.get("country") or ev.get("div") or "US").upper()
                        if cur not in ("USD", "US", "UNITED STATES", "EE.UU.", ""):
                            continue
                        f_raw = str(ev.get("date") or ev.get("fecha") or ev.get("time") or ev.get("datetime") or "")
                        m_f = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", f_raw)
                        if not m_f:
                            continue
                        f_iso = f"{m_f.group(1)}-{int(m_f.group(2)):02d}-{int(m_f.group(3)):02d}"
                        m_h = re.search(r"(\d{2}:\d{2})", f_raw)
                        hora = str(ev.get("hora") or (m_h.group(1) if m_h else "10:30"))
                        nom = str(ev.get("event") or ev.get("title") or ev.get("name") or ev.get("evento") or "").strip()
                        if not nom:
                            continue
                        imp_raw = ev.get("importance") or ev.get("impact") or ev.get("importancia") or 2
                        imp = 3 if str(imp_raw).lower() in ("3", "high", "alta", "bull3") else (1 if str(imp_raw).lower() in ("1", "low", "baja") else 2)
                        extraidos.append({
                            "fecha": f_iso,
                            "hora": hora,
                            "div": "US",
                            "evento": nom,
                            "importancia": imp,
                            "actual": str(ev.get("actual") if ev.get("actual") not in (None, "") else "-"),
                            "prevision": str(ev.get("forecast") or ev.get("prevision") or "-"),
                            "anterior": str(ev.get("previous") or ev.get("anterior") or "-")
                        })
                if extraidos:
                    break
        except Exception:
            pass

    mapa = {}
    for item in CALENDARIO_USA_BASE + CACHE_CALENDARIO_USA + extraidos:
        if item.get("fecha") and item.get("evento"):
            mapa[f"{item['fecha']}|{item['evento']}"] = item
    res = sorted(list(mapa.values()), key=lambda x: (x["fecha"], x["hora"]))
    CACHE_CALENDARIO_USA = res
    return res

def fetch_licitaciones_mae() -> List[Dict[str, Any]]:
    fetch_estado_mae()
    return fetch_licitaciones_mae_rango()

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
                f_ust = executor.submit(fetch_curva_us_treasuries)

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
                res_ust = f_ust.result()
                if res_ust:
                    CACHE_CURVAS_SJB['us_treasuries'] = res_ust

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
            for cod_p, desc_p in [
                ("DOLAR_OFICIAL", "Dólar Oficial Minorista"),
                ("DOLAR_BLUE", "Dólar Blue"),
                ("DOLAR_TARJETA", "Dólar Tarjeta"),
                ("DOLAR_CRIPTO", "Dólar Cripto (USDT)"),
                ("DOLAR_NETFLIX", "Dólar Netflix"),
                ("DOLAR_A3500", "Tipo de Cambio Mayorista Comunicación A3500")
            ]:
                if cod_p in puntas_dl:
                    info_p = puntas_dl[cod_p]
                    c_v = info_p.get("compra")
                    v_v = info_p.get("venta")
                    ent_c = info_p.get("entidad_compra")
                    ent_v = info_p.get("entidad_venta") or info_p.get("entidad")
                    fmt_v = f"${v_v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    encontrado_d = False
                    for item_d in nuevo_agrupado["dolar_y_cambiario"]:
                        if item_d.get("variable") == cod_p:
                            item_d["compra"] = c_v
                            item_d["venta"] = v_v
                            item_d["valor"] = v_v
                            item_d["valor_formateado"] = fmt_v
                            if ent_c: item_d["entidad_compra"] = ent_c
                            if ent_v: item_d["entidad_venta"] = ent_v
                            encontrado_d = True
                            break
                    if not encontrado_d:
                        nuevo_item_d = {
                            "variable": cod_p, "descripcion": desc_p, "categoria": "dolar_y_cambiario",
                            "compra": c_v, "venta": v_v, "valor": v_v, "valor_formateado": fmt_v,
                            "variacion": "0,0%", "fecha": info_p.get("fecha", "")
                        }
                        if ent_c: nuevo_item_d["entidad_compra"] = ent_c
                        if ent_v: nuevo_item_d["entidad_venta"] = ent_v
                        nuevo_agrupado["dolar_y_cambiario"].append(nuevo_item_d)
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
