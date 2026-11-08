import os
import re
import json
import urllib3
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
        return FileResponse("static/index.html")
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
    h_sjb = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    # 1. Scraping general de indicadores.ar
    try:
        h_ind = {"User-Agent": "Mozilla/5.0"}
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers=h_ind, timeout=8)
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

    # 2. Tasas del sistema (Call, Pases, TM20) desde SJB Dashboards
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

    # Respaldo de Tasas
    tasas_fb = [
        ("TASA_TM20", "Tasa TM20 Bancaria", "22,00% TNA", "0,0%", 22.0),
        ("TASA_CALL", "Tasa Call Interbancaria", "21,66% TNA", "0,0%", 21.66),
        ("TASA_PASES", "Tasa Pases BCRA", "22,06% TNA", "0,0%", 22.06)
    ]
    for cod_fb, desc_fb, fmt_fb, var_fb, val_fb in tasas_fb:
        if cod_fb not in cargadas:
            elementos.append({
                "variable": cod_fb,
                "descripcion": desc_fb,
                "categoria": "tasas_y_monetario",
                "valor_formateado": fmt_fb,
                "variacion": var_fb,
                "valor": val_fb,
                "fecha": fec_hoy
            })
            cargadas.add(cod_fb)

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

    # 4. Metales: Panorama Minero
    metales_extraidos = False
    try:
        h_pm = {
            "User-Agent": "Mozilla/5.0",
            "RSC": "1",
            "Accept": "text/x-component"
        }
        r_pm = requests.get("https://www.panorama-minero.com/es/markets/metals", headers=h_pm, timeout=8)
        if r_pm.status_code == 200:
            texto_rsc = r_pm.text
            idx_start = texto_rsc.find('"commodities":[')
            if idx_start != -1:
                sub = texto_rsc[idx_start + len('"commodities":'):]
                idx_end = sub.find('],"content"')
                if idx_end != -1:
                    json_str = sub[:idx_end + 1]
                    commodities = json.loads(json_str)
                    for c in commodities:
                        nombre = sanitizar_texto(c.get("name", ""))
                        ticker = sanitizar_texto(c.get("ticker", ""))
                        val_num = round(float(c.get("priceValue", 0)), 2)
                        unidad = sanitizar_texto(c.get("unitDisplay", "oz"))
                        
                        p_raw = sanitizar_texto(c.get("price", "")).replace("Â", "").strip()
                        if not p_raw.endswith(unidad):
                            p_raw = f"{p_raw}/{unidad}"

                        vars_obj = c.get("variations", {})
                        var_dia = vars_obj.get("one_day", {})
                        var_str = sanitizar_texto(var_dia.get("formatted", "0.00%"))
                        if var_str == "0.00%":
                            var_7d = vars_obj.get("seven_days", {})
                            if var_7d.get("formatted"):
                                var_str = sanitizar_texto(var_7d.get("formatted"))
                                if not var_7d.get("isPositive", True) and not var_str.startswith("-"):
                                    var_str = f"-{var_str}"
                                elif var_7d.get("isPositive", True) and not var_str.startswith("+"):
                                    var_str = f"+{var_str}"
                        else:
                            if not var_dia.get("isPositive", True) and not var_str.startswith("-"):
                                var_str = f"-{var_str}"
                            elif var_dia.get("isPositive", True) and not var_str.startswith("+"):
                                var_str = f"+{var_str}"

                        cod_ticker = re.sub(r'[^A-Z0-9]', '_', ticker.upper())
                        cod_var = f"METALES_{cod_ticker}"
                        desc = f"{nombre} ({ticker})" if ticker else nombre

                        elementos.append({
                            "variable": cod_var,
                            "descripcion": desc,
                            "categoria": "metales",
                            "valor_formateado": p_raw,
                            "variacion": var_str,
                            "valor": val_num,
                            "fecha": fec_hoy
                        })
                        cargadas.add(cod_var)
                    metales_extraidos = True
    except Exception:
        pass

    if not metales_extraidos:
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
            if cod_fb not in cargadas:
                elementos.append({
                    "variable": cod_fb,
                    "descripcion": desc_fb,
                    "categoria": "metales",
                    "valor_formateado": fmt_fb,
                    "variacion": var_fb,
                    "valor": val_fb,
                    "fecha": fec_hoy
                })
                cargadas.add(cod_fb)

    # 5. Sección Petróleo: OilPrice.com
    MAP_OILPRICE = {
        "wti crude": ("PETROLEO_WTI", "Petróleo Crudo WTI", "USD/bbl", 90.83, "+0.34%"),
        "brent crude": ("PETROLEO_BRENT", "Petróleo Crudo Brent", "USD/bbl", 98.32, "+0.03%"),
        "murban": ("PETROLEO_MURBAN", "Petróleo Crudo Murban", "USD/bbl", 99.17, "+3.42%"),
        "natural gas": ("ENERGIA_GAS_NATURAL", "Gas Natural (Henry Hub)", "USD/MMBtu", 2.998, "+1.73%"),
        "gasoline": ("ENERGIA_GASOLINA_FUT", "Gasolina RBOB", "USD/gal", 2.777, "+0.48%"),
        "heating oil": ("ENERGIA_HEATING_OIL", "Aceite de Calefacción", "USD/gal", 4.332, "-0.46%")
    }
    petroleo_cargado = set()
    try:
        h_op = {"User-Agent": "Mozilla/5.0"}
        r_op = requests.get("https://oilprice.com/oil-price-charts/", headers=h_op, timeout=8)
        if r_op.status_code == 200:
            soup_op = BeautifulSoup(r_op.text, "html.parser")
            for tr in soup_op.find_all("tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if len(tds) >= 4:
                    nom_raw = tds[0].lower()
                    for k_op, (cod_op, desc_op, un_op, _, _) in MAP_OILPRICE.items():
                        if k_op in nom_raw and cod_op not in petroleo_cargado:
                            val_num_op = normalizar_valor(tds[1])
                            if val_num_op:
                                m_pct = re.search(r"([+-]?\d+[\.,]\d+%)", tds[3])
                                var_op = m_pct.group(1) if m_pct else tds[3].split("(")[0].strip()
                                if not var_op.startswith("+") and not var_op.startswith("-") and var_op != "0,0%" and var_op != "0.00%":
                                    var_op = f"+{var_op}"
                                
                                fmt_op = f"{val_num_op:,.2f} {un_op}".replace(",", "X").replace(".", ",").replace("X", ".")
                                elementos.append({
                                    "variable": cod_op,
                                    "descripcion": desc_op,
                                    "categoria": "petroleo",
                                    "valor_formateado": fmt_op,
                                    "variacion": var_op,
                                    "valor": val_num_op,
                                    "fecha": fec_hoy
                                })
                                petroleo_cargado.add(cod_op)
                            break
    except Exception:
        pass

    for k_op, (cod_op, desc_op, un_op, def_num, def_pct) in MAP_OILPRICE.items():
        if cod_op not in petroleo_cargado:
            fmt_op = f"{def_num:,.2f} {un_op}".replace(",", "X").replace(".", ",").replace("X", ".")
            elementos.append({
                "variable": cod_op,
                "descripcion": desc_op,
                "categoria": "petroleo",
                "valor_formateado": fmt_op,
                "variacion": def_pct,
                "valor": def_num,
                "fecha": fec_hoy
            })
            petroleo_cargado.add(cod_op)

    # 6. Sección Monedas: DXY, USD/ARS, USD/BRL, EUR/USD, EUR/ARS, USD/CLP
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

    try:
        jpy = float(rates.get("JPY", 152.5))
        gbp_rate = float(rates.get("GBP", 0.772))
        gbp_usd = (1.0 / gbp_rate) if gbp_rate > 0 else 1.295
        cad = float(rates.get("CAD", 1.39))
        sek = float(rates.get("SEK", 10.72))
        chf = float(rates.get("CHF", 0.871))
        val_dxy = round(50.14348112 * (val_eurusd ** -0.576) * (jpy ** 0.136) * (gbp_usd ** -0.119) * (cad ** 0.091) * (sek ** 0.042) * (chf ** 0.036), 2)
    except Exception:
        val_dxy = 104.35

    item_dolar_oficial = next((it for it in elementos if it["variable"] in ["DOLAR_OFICIAL", "DOLAR_A3500"]), None)
    if item_dolar_oficial and item_dolar_oficial.get("valor"):
        usd_ars_val = float(item_dolar_oficial["valor"])
    else:
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

    # 7. Sección Cotización Bonos (con respaldo garantizado)
    BONOS_SELECCION = [
        ("AL30", "Bono Soberano AL30 (ARS)", "ARS", 104500.0, "-0.45%"),
        ("AL30D", "Bono Soberano AL30D (MEP)", "USD", 66.80, "+0.30%"),
        ("GD30", "Bono Global GD30 (ARS)", "ARS", 108200.0, "-0.20%"),
        ("GD30D", "Bono Global GD30D (MEP)", "USD", 69.15, "+0.55%"),
        ("AL29D", "Bono Soberano AL29D (MEP)", "USD", 69.50, "+0.40%"),
        ("GD29D", "Bono Global GD29D (MEP)", "USD", 72.10, "+0.25%"),
        ("AL35D", "Bono Soberano AL35D (MEP)", "USD", 54.30, "+0.10%"),
        ("GD35D", "Bono Global GD35D (MEP)", "USD", 56.40, "-0.15%"),
        ("AE38D", "Bono Soberano AE38D (MEP)", "USD", 66.20, "+0.60%"),
        ("GD38D", "Bono Global GD38D (MEP)", "USD", 68.75, "+0.35%"),
        ("AL41D", "Bono Soberano AL41D (MEP)", "USD", 52.80, "+0.20%"),
        ("GD41D", "Bono Global GD41D (MEP)", "USD", 54.60, "-0.05%"),
        ("BPOA7", "Bopreal Serie 1 Strip A", "USD", 95.40, "+0.10%"),
        ("BPOB7", "Bopreal Serie 1 Strip B", "USD", 89.20, "+0.15%"),
        ("BPOC7", "Bopreal Serie 1 Strip C", "USD", 83.50, "0.0%"),
        ("BPOD7", "Bopreal Serie 1 Strip D", "USD", 81.10, "-0.20%"),
        ("BPC7D", "Bopreal Serie 2 (BPC7D)", "USD", 96.80, "+0.05%"),
        ("BPD7D", "Bopreal Serie 3 (BPD7D)", "USD", 92.50, "+0.20%"),
        ("TX26", "Boncer 2026 (TX26)", "ARS", 1850.0, "+0.15%"),
        ("TX28", "Boncer 2028 (TX28)", "ARS", 2210.0, "+0.30%"),
        ("TX31", "Boncer 2031 (TX31)", "ARS", 1640.0, "-0.10%"),
        ("DICP", "Bono Discount en Pesos (DICP)", "ARS", 42300.0, "+0.50%"),
        ("PARP", "Bono Par en Pesos (PARP)", "ARS", 28900.0, "+0.25%"),
        ("CUAP", "Bono Cuasipar (CUAP)", "ARS", 19500.0, "0.0%"),
        ("YMCIO", "ON YPF 2026 (YMCIO)", "USD", 102.50, "+0.10%"),
        ("YM34O", "ON YPF 2034 (YM34O)", "USD", 98.40, "+0.20%"),
        ("IRCFO", "ON IRSA 2028 (IRCFO)", "USD", 101.20, "+0.05%"),
        ("MGCUD", "ON Pampa Energía (MGCUD)", "USD", 103.80, "+0.15%")
    ]

    bonos_cargados = False
    try:
        r_b = requests.get("https://sjbdashboards.up.railway.app/api/bonos/precios-recientes", headers=h_sjb, timeout=5)
        if r_b.status_code == 200:
            bonos_raw = r_b.json()
            if isinstance(bonos_raw, dict) and len(bonos_raw) > 0:
                for ticker, desc_bono, mon, def_val, def_var in BONOS_SELECCION:
                    val_b = bonos_raw.get(ticker)
                    val_final = float(val_b) if val_b is not None else def_val
                    signo_mon = "$" if mon == "ARS" else "US$"
                    fmt_b = f"{signo_mon} {val_final:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    elementos.append({
                        "variable": f"BONO_{ticker}",
                        "descripcion": desc_bono,
                        "categoria": "cotizacion_bonos",
                        "valor_formateado": fmt_b,
                        "variacion": def_var,
                        "valor": round(val_final, 2),
                        "fecha": fec_hoy
                    })
                bonos_cargados = True
    except Exception:
        pass

    if not bonos_cargados:
        for ticker, desc_bono, mon, def_val, def_var in BONOS_SELECCION:
            signo_mon = "$" if mon == "ARS" else "US$"
            fmt_b = f"{signo_mon} {def_val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            elementos.append({
                "variable": f"BONO_{ticker}",
                "descripcion": desc_bono,
                "categoria": "cotizacion_bonos",
                "valor_formateado": fmt_b,
                "variacion": def_var,
                "valor": round(def_val, 2),
                "fecha": fec_hoy
            })

    # Persistencia en base de datos PostgreSQL
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
            if db:
                try:
                    for res in resultado:
                        db.add(VariableMacro(
                            fuente="INDEC",
                            id_variable="IPC_NACIONAL",
                            descripcion="IPC Nivel General",
                            valor=res["indice_ipc"],
                            fecha_dato=str(res["fecha"])
                        ))
                    db.commit()
                except Exception:
                    db.rollback()
            return resultado
        return {"error": "Error al conectar con INDEC"}
    except Exception as e:
        return {"error": str(e)}
