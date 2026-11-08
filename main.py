import os
from datetime import datetime
from typing import List, Optional
import requests
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Configuración de Base de Datos
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL) if DATABASE_URL else None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None
Base = declarative_base()

# Tablas en PostgreSQL
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
    __tablename__ = "variables_macro"

    id = Column(Integer, primary_key=True, index=True)
    fuente = Column(String, index=True) # BCRA o INDEC
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
    respuesta = requests.get(url, timeout=10)
    
    if respuesta.status_code == 200:
        datos = respuesta.json()
        if db:
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
        return datos
    return {"error": "No se pudo conectar a la API de divisas"}

@app.get("/dolar/historial")
def ver_historial(limite: int = 20, db: Session = Depends(get_db)):
    if not db:
        return {"error": "Base de datos no disponible"}
    return db.query(CotizacionDolar).order_by(CotizacionDolar.id.desc()).limit(limite).all()

@app.get("/bcra/variables")
def ver_bcra(db: Session = Depends(get_db)):
    # Fuente consolidada de series de tiempo oficiales de Argentina (BCRA)
    series_bcra = {
        "RESERVAS": {"id": "174.1_RO_2016_0_21", "desc": "Reservas Internacionales (Millones USD)"},
        "BASE_MONETARIA": {"id": "174.1_BMA_2016_0_15", "desc": "Base Monetaria (Millones ARS)"},
        "TIPO_CAMBIO_A3500": {"id": "168.1_T_CAMBIOR_D_0_0_26", "desc": "Tipo de Cambio Mayorista Comunicación A3500"}
    }
    
    ids_param = ",".join([item["id"] for item in series_bcra.values()])
    url = f"https://apis.datos.gob.ar/series/api/series/?ids={ids_param}&limit=1&sort=desc"
    
    try:
        respuesta = requests.get(url, timeout=15)
        if respuesta.status_code == 200:
            datos = respuesta.json()
            data_points = datos.get("data", [])
            
            if not data_points:
                return {"mensaje": "No se encontraron datos recientes"}
            
            ultimo_registro = data_points[0]
            fecha = ultimo_registro[0]
            
            resultado = []
            keys = list(series_bcra.keys())
            for idx, key in enumerate(keys):
                valor = ultimo_registro[idx + 1]
                info = series_bcra[key]
                item_data = {
                    "variable": key,
                    "descripcion": info["desc"],
                    "valor": valor,
                    "fecha": fecha
                }
                resultado.append(item_data)
                
                if db and valor is not None:
                    registro = VariableMacro(
                        fuente="BCRA",
                        id_variable=key,
                        descripcion=info["desc"],
                        valor=float(valor),
                        fecha_dato=str(fecha)
                    )
                    db.add(registro)
            
            if db:
                db.commit()
            return resultado
        return {"error": f"API de series devolvió código {respuesta.status_code}"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/indec/ipc")
def ver_indec_ipc(db: Session = Depends(get_db)):
    url = "https://apis.datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&limit=12&sort=desc"
    try:
        respuesta = requests.get(url, timeout=15)
        if respuesta.status_code == 200:
            datos = respuesta.json()
            data_points = datos.get("data", [])
            
            resultado = []
            for punto in data_points:
                fecha, valor = punto[0], punto[1]
                resultado.append({"fecha": fecha, "indice_ipc": valor})
                
                if db and valor is not None:
                    registro = VariableMacro(
                        fuente="INDEC",
                        id_variable="IPC_NACIONAL",
                        descripcion="Índice de Precios al Consumidor (Nivel General)",
                        valor=float(valor),
                        fecha_dato=str(fecha)
                    )
                    db.add(registro)
            if db:
                db.commit()
            return resultado
        return {"error": f"INDEC API devolvió código {respuesta.status_code}"}
    except Exception as e:
        return {"error": str(e)}
