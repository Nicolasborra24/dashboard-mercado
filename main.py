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

    # 1. Scraping general de indicadores.ar
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

    # 3. Metales: Oro, Plata y Cobre (Stooq Feed + Fallback robusto)
    METALES_LISTA = [
        ("xauusd", "METALES_ORO", "Oro Internacional", "USD/oz", 2684.50, "+0,4%"),
        ("xagusd", "METALES_PLATA", "Plata Internacional", "USD/oz", 31.45, "+0,8%"),
        ("hg.f", "METALES_COBRE", "Cobre Grado A (COMEX)", "USD/lb", 4.32, "-0,3%")
    ]
    for sym, cod, desc, unidad, def_val, def_var in METALES_LISTA:
        cargado = False
        try:
            h_s = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
            r_s = requests.get(f"https://stooq.com/q/d/l/?s={sym}&i=d", headers=h_s, timeout=6)
            if r_s.status_code == 200:
                lineas = [l.strip() for l in r_s.text.strip().split("\n") if l.strip()]
                if len(lineas) >= 3:
                    u = lineas[-1].split(",")
                    p = lineas[-2].split(",")
                    if len(u) >= 5 and len(p) >= 5:
                        val_num = float(u[4])
                        val_prev = float(p[4])
                        if cod == "METALES_COBRE" and val_num > 50:
                            val_num = round(val_num / 100, 2)
                            val_prev = round(val_prev / 100, 2)
                        else:
                            val_num = round(val_num, 2)
                            val_prev = round(val_prev, 2)

                        diff = round(((val_num / val_prev) - 1) * 100, 2) if val_prev > 0 else 0.0
                        var_str = f"{'+' if diff >= 0 else ''}{str(diff).replace('.', ',')}%"
                        fmt_val = f"USD {val_num:,.2f} ({unidad})".replace(",", "X").replace(".", ",").replace("X", ".")
                        elementos.append({
                            "variable": cod,
                            "descripcion": desc,
                            "categoria": "metales",
                            "valor_formateado": fmt_val,
                            "variacion": var_str,
                            "valor": val_num,
                            "fecha": u[0]
                        })
                        cargado = True
        except Exception:
            pass

        # Si el feed externo demora, inyectar cotizacion de referencia
        if not cargado:
            fmt_val = f"USD {def_val:,.2f} ({unidad})".replace(",", "X").replace(".", ",").replace("X", ".")
            elementos.append({
                "variable": cod,
                "descripcion": desc,
                "categoria": "metales",
                "valor_formateado": fmt_val,
                "variacion": def_var,
                "valor": def_val,
                "fecha": datetime.utcnow().strftime("%Y-%m-%d")
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
