import os
import urllib3
from datetime import datetime
from typing import List, Optional
import requests
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
        respuesta = requests.get(url, timeout=10)
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

    # 1. Dólar A3500 Mayorista
    try:
        r_a3500 = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=1&sort=desc", timeout=8)
        if r_a3500.status_code == 200:
            d = r_a3500.json().get("data", [])
            if d and len(d[0]) >= 2 and d[0][1] is not None:
                resultado.append({
                    "variable": "DOLAR_A3500",
                    "descripcion": "Tipo de Cambio Mayorista Comunicación A3500",
                    "valor": float(d[0][1]),
                    "fecha": d[0][0]
                })
    except Exception:
        pass

    # 2. Reservas Internacionales
    try:
        r_res = requests.get("https://api.argentinadatos.com/v1/finanzas/tasas/depositos30Dias", timeout=8)
        # Endpoint alternativo directo para reservas del BCRA
        r_bcra = requests.get("https://api.argentinadatos.com/v1/finanzas/indices/inflacionInteranual", timeout=8)
    except Exception:
        pass

    # Consulta directa de Reservas en serie activa de balance semanal/mensual
    try:
        url_reservas = "https://apis.datos.gob.ar/series/api/series/?ids=174.1_RO_2016_0_21&limit=5&sort=desc"
        r_r = requests.get(url_reservas, timeout=8)
        if r_r.status_code == 200:
            d = r_r.json().get("data", [])
            for row in d:
                if len(row) >= 2 and row[1] is not None:
                    resultado.append({
                        "variable": "RESERVAS_USD",
                        "descripcion": "Reservas Internacionales (Millones USD)",
                        "valor": float(row[1]),
                        "fecha": row[0]
                    })
                    break
    except Exception:
        pass

    # Si la serie oficial 174.1 no trajo valor, consultar la serie monetaria 148
    if not any(x["variable"] == "RESERVAS_USD" for x in resultado):
        try:
            r_alt = requests.get("https://apis.datos.gob.ar/series/api/series/?ids=174.1_RO_M_0_0_21&limit=1&sort=desc", timeout=8)
            if r_alt.status_code == 200:
                d = r_alt.json().get("data", [])
                if d and len(d[0]) >= 2 and d[0][1] is not None:
                    resultado.append({
                        "variable": "RESERVAS_USD",
                        "descripcion": "Reservas Internacionales (Millones USD)",
                        "valor": float(d[0][1]),
                        "fecha": d[0][0]
                    })
        except Exception:
            pass

    # 3. Base Monetaria
    try:
        url_bm = "https://apis.datos.gob.ar/series/api/series/?ids=174.1_BMA_2016_0_15&limit=5&sort=desc"
        r_b = requests.get(url_bm, timeout=8)
        if r_b.status_code == 200:
            d = r_b.json().get("data", [])
            for row in d:
                if len(row) >= 2 and row[1] is not None:
                    resultado.append({
                        "variable": "BASE_MONETARIA",
                        "descripcion": "Base Monetaria (Millones ARS)",
                        "valor": float(row[1]),
                        "fecha": row[0]
                    })
                    break
    except Exception:
        pass

    # Si todavía faltan Reservas o Base Monetaria, consultar directamente el endpoint de variables de Ámbito/DolarApi
    if len(resultado) < 3:
        try:
            r_amb = requests.get("https://dolarapi.com/v1/ambito/dolares", timeout=8)
            # Respaldo de contingencia
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
        r_dolar = requests.get("https://dolarapi.com/v1/dolares", timeout=10)
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
        respuesta = requests.get(url, timeout=10)
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
