import os
import re
import urllib3
from datetime import datetime
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Depends
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
    return {"mensaje": "Servidor funcionando correctamente", "db_conectada": engine is not None}

@app.get("/dolar")
def ver_dolar(db: Session = Depends(get_db)):
    url = "https://dolarapi.com/v1/dolares"
    try:
        respuesta = requests.get(url, timeout=8)
        if respuesta.status_code == 200:
            datos = respuesta.json()
            if db:
                try:
                    for item in datos:
                        nueva_cotizacion = CotizacionDolar(
                            casa=item.get("casa"),
                            nombre=item.get("nombre"),
                            compra=float(item.get("compra", 0) or 0),
                            venta=float(item.get("venta", 0) or 0),
                            fecha_actualizacion=str(item.get("fechaActualizacion"))
                        )
                        db.add(nueva_cotizacion)
                    db.commit()
                except Exception:
                    db.rollback()
            return datos
        return {"error": "No se pudo conectar a la API de divisas"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/dolar/historial")
def ver_historial(limite: int = 20, db: Session = Depends(get_db)):
    if not db:
        return {"error": "Base de datos no disponible"}
    try:
        return db.query(CotizacionDolar).order_by(CotizacionDolar.id.desc()).limit(limite).all()
    except Exception as e:
        return {"error": str(e)}

@app.get("/bcra/variables")
def ver_bcra(db: Session = Depends(get_db)):
    resultado = []

    # 1. Scraping fila por fila sobre la tabla de indicadores.ar
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        r = requests.get("https://indicadores.ar/indicadores-economicos", headers=headers, timeout=8)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            
            for tr in soup.find_all("tr"):
                texto_tr = tr.get_text(" | ", strip=True)
                
                # Fila exclusiva de Reservas BCRA
                if "Reservas BCRA" in texto_tr and not any(x["variable"] == "RESERVAS_USD" for x in resultado):
                    match_res = re.search(r"USD\s*([0-9]+(?:[.,][0-9]+)?)\s*([BMKbmk])", texto_tr, re.IGNORECASE)
                    match_fecha = re.search(r"(\d{1,2}\s+[a-zA-Z]{3,4}\s+\d{4})", texto_tr)
                    if match_res:
                        num = float(match_res.group(1).replace(",", "."))
                        sufijo = match_res.group(2).upper()
                        val_m = round(num * 1000.0, 2) if sufijo == "B" else num
                        resultado.append({
                            "variable": "RESERVAS_USD",
                            "descripcion": "Reservas Internacionales BCRA",
                            "valor_formateado": f"USD {num}{sufijo}",
                            "valor": val_m,
                            "fecha": match_fecha.group(1) if match_fecha else datetime.utcnow().strftime("%Y-%m-%d")
                        })

                # Fila exclusiva de Base Monetaria
                elif "Base Monetaria" in texto_tr and not any(x["variable"] == "BASE_MONETARIA" for x in resultado):
                    match_bm = re.search(r"\$\s*([0-9]+(?:[.,][0-9]+)?)\s*([BMKbmk])", texto_tr, re.IGNORECASE)
                    match_fecha = re.search(r"(\d{1,2}\s+[a-zA-Z]{3,4}\s+\d{4})", texto_tr)
                    if match_bm:
                        num = float(match_bm.group(1).replace(",", "."))
                        sufijo = match_bm.group(2).upper()
                        resultado.append({
                            "variable": "BASE_MONETARIA",
                            "descripcion": "Base Monetaria BCRA",
                            "valor_formateado": f"${num}{sufijo}",
                            "valor": num,
                            "fecha": match_fecha.group(1) if match_fecha else datetime.utcnow().strftime("%Y-%m-%d")
                        })
    except Exception:
        pass

    # 2. Dólar Oficial Mayorista A3500 (datos.gob.ar)
    try:
        r_a3500 = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=6)
        if r_a3500.status_code == 200:
            d = r_a3500.json().get("data", [])
            if d and len(d[0]) >= 2 and d[0][1] is not None:
                val = float(d[0][1])
                resultado.append({
                    "variable": "DOLAR_A3500",
                    "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
                    "valor_formateado": f"${val:.2f}",
                    "valor": val,
                    "fecha": d[0][0]
                })
    except Exception:
        pass

    # Persistencia en base de datos
    if db and resultado:
        for item in resultado:
            try:
                db_record = VariableMacro(
                    fuente="BCRA",
                    id_variable=item["variable"],
                    descripcion=item["descripcion"],
                    valor=item["valor"],
                    fecha_dato=str(item["fecha"])
                )
                db.add(db_record)
            except Exception:
                db.rollback()
        try:
            db.commit()
        except Exception:
            db.rollback()

    return resultado

@app.get("/mercado/resumen")
def ver_resumen_mercado(db: Session = Depends(get_db)):
    try:
        r_dolar = requests.get("https://dolarapi.com/v1/dolares", timeout=8)
        dolares = r_dolar.json() if r_dolar.status_code == 200 else []
        
        precios = {d["casa"]: float(d.get("venta", 0) or 0) for d in dolares}
        mayorista = precios.get("mayorista") or precios.get("oficial", 1)
        
        brechas = {}
        if mayorista > 0:
            for casa in ["blue", "bolsa", "contadoconliqui", "tarjeta", "cripto"]:
                if casa in precios and precios[casa] > 0:
                    spread = round(((precios[casa] / mayorista) - 1) * 100, 2)
                    brechas[casa] = spread
                    
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
    url = "https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=6&sort=desc"
    try:
        respuesta = requests.get(url, timeout=8)
        if respuesta.status_code == 200:
            datos = respuesta.json()
            data_points = datos.get("data", [])
            
            resultado = []
            for punto in data_points:
                fecha, valor = punto[0], punto[1]
                if valor is not None:
                    resultado.append({"fecha": fecha, "indice_ipc": float(valor)})
                    if db:
                        try:
                            registro = VariableMacro(
                                fuente="INDEC",
                                id_variable="IPC_NACIONAL",
                                descripcion="Índice de Precios al Consumidor (Nivel General)",
                                valor=float(valor),
                                fecha_dato=str(fecha)
                            )
                            db.add(registro)
                        except Exception:
                            db.rollback()
            if db:
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            return resultado
        return {"error": f"INDEC API devolvió código {respuesta.status_code}"}
    except Exception as e:
        return {"error": str(e)}
