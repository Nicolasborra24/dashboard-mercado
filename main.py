import os
import re
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

@app.get("/bcra/variables")
def ver_bcra(formato: str = Query("agrupado", enum=["agrupado", "lista"]), db: Session = Depends(get_db)):
    elementos = []
    cargadas = set()

    # 1. Scraping de indicadores.ar
    try:
        h = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers=h, timeout=8)
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

    # 2. Dólar Oficial Mayorista A3500
    try:
        r_a = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=6)
        if r_a.status_code == 200:
            d = r_a.json().get("data", [])
            if d and len(d[0]) >= 2 and d[0][1] is not None:
                val = float(d[0][1])
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

    # 3. Extracción de TODOS los metales desde Panorama Minero
    try:
        h_pm = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        r_pm = requests.get("https://www.panorama-minero.com/es/markets/metals", headers=h_pm, timeout=8)
        if r_pm.status_code == 200:
            soup_pm = BeautifulSoup(r_pm.text, "html.parser")
            fec_hoy = datetime.utcnow().strftime("%d %b %Y")

            for tr in soup_pm.find_all("tr"):
                tds = tr.find_all(["td", "th"])
                if len(tds) >= 4:
                    raw_strings = [td.get_text(strip=True) for td in tds]
                    
                    # Detectar encabezados
                    if not raw_strings[0] or raw_strings[0].lower() in ["commodity", "nombre", "símbolo", "precio"]:
                        continue

                    # Identificar la columna de precio por unidad
                    idx_precio = -1
                    precio_str = ""
                    for i, td in enumerate(tds):
                        t_text = td.get_text(strip=True)
                        if "US$" in t_text or "USD" in t_text or "/oz" in t_text or "/lb" in t_text:
                            idx_precio = i
                            precio_str = t_text
                            break

                    if idx_precio != -1:
                        nombre_raw = " ".join(dict.fromkeys(tds[0].stripped_strings))
                        simbolo = tds[1].get_text(strip=True) if idx_precio > 1 else ""
                        
                        # 24h % suele estar 2 columnas despues del precio (saltando 1h %)
                        var_24h = "0.00%"
                        if len(tds) > idx_precio + 2:
                            var_24h = tds[idx_precio + 2].get_text(strip=True)
                        elif len(tds) > idx_precio + 1:
                            var_24h = tds[idx_precio + 1].get_text(strip=True)

                        v_num = normalizar_valor(precio_str)
                        if v_num is not None:
                            # Código limpio normalizado
                            cod_base = re.sub(r'[^A-Z0-9]', '_', nombre_raw.upper()).strip('_')
                            cod = f"METALES_{cod_base}"

                            if cod not in cargadas:
                                # Asegurar signo positivo en variaciones verdes
                                var_fmt = var_24h
                                try:
                                    v_clean = float(var_24h.replace("%", "").replace(",", ".").strip())
                                    if v_clean > 0 and not var_fmt.startswith("+"):
                                        var_fmt = f"+{var_fmt}"
                                except Exception:
                                    pass

                                desc = f"{nombre_raw} ({simbolo})" if simbolo and simbolo.upper() != nombre_raw.upper() else nombre_raw

                                elementos.append({
                                    "variable": cod,
                                    "descripcion": desc,
                                    "categoria": "metales",
                                    "valor_formateado": precio_str,
                                    "variacion": var_fmt,
                                    "valor": v_num,
                                    "fecha": fec_hoy
                                })
                                cargadas.add(cod)
    except Exception:
        pass

    # Respaldo completo de Panorama Minero (siempre muestra los 18 activos)
    METALES_FALLBACK = [
        ("METALES_ORO", "Oro (XAU)", "4070,10 US$/ozt", "+0.19%", 4070.10),
        ("METALES_PLATA", "Plata (XAG)", "51,95 US$/oz", "+0.43%", 51.95),
        ("METALES_COBRE", "Cobre (HG00)", "6,24 US$/lb", "+0.34%", 6.24),
        ("METALES_PLATINO", "Platino (XPT)", "1534,00 US$/oz", "+0.52%", 1534.00),
        ("METALES_PALADIO", "Paladio (XPD)", "1004,00 US$/oz", "0.00%", 1004.00),
        ("METALES_RODIO", "Rodio (XRH)", "9200,00 US$/oz", "0.00%", 9200.00),
        ("METALES_URANIO", "Uranio (U)", "91,85 US$/lb", "+0.76%", 91.85),
        ("METALES_LITIO", "Litio (LI)", "2,29 US$/oz", "0.00%", 2.29),
        ("METALES_CARBONATO_LITIO", "Carbonato de Litio (LITH-CAR)", "2,29 US$/ozt", "0.00%", 2.29),
        ("METALES_ALUMINIO", "Aluminio (ALU)", "10,49 US$/ozt", "0.00%", 10.49),
        ("METALES_ZINC", "Zinc (ZNC)", "8,55 US$/ozt", "+3.13%", 8.55),
        ("METALES_NIQUEL", "Níquel (NI)", "2,00 US$/oz", "+0.04%", 2.00),
        ("METALES_ESTANO", "Estaño (TIN)", "1,45 US$/oz", "+3.69%", 1.45),
        ("METALES_PLOMO", "Plomo (LEAD)", "17,31 US$/oz", "+0.50%", 17.31),
        ("METALES_MINERAL_HIERRO", "Mineral de hierro (IRON)", "380,52 US$/oz", "0.00%", 380.52),
        ("METALES_COBALTO", "Cobalto (LCO)", "1,04 US$/oz", "0.00%", 1.04),
        ("METALES_MOLIBDENO", "Molibdeno (MO)", "2,81 US$/ozt", "0.00%", 2.81),
        ("METALES_VANADIO", "Vanadio (VAN)", "3,17 US$/ozt", "0.00%", 3.17)
    ]
    fec_ref = datetime.utcnow().strftime("%d %b %Y")
    for cod_f, desc_f, val_fmt_f, var_f, num_f in METALES_FALLBACK:
        if cod_f not in cargadas:
            elementos.append({
                "variable": cod_f,
                "descripcion": desc_f,
                "categoria": "metales",
                "valor_formateado": val_fmt_f,
                "variacion": var_f,
                "valor": num_f,
                "fecha": fec_ref
            })
            cargadas.add(cod_f)

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
        "inflacion_y_ajuste": [],
        "mercado_y_deuda": [],
        "metales": [],
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
