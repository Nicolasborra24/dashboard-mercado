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
    id_variable = Column(Integer, nullable=True)
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
    # API Oficial BCRA - Estadísticas Monetarias
    url = "https://api.bcra.gob.ar/estadisticas/v3.0/monetarias"
    try:
        respuesta = requests.get(url, timeout=15, verify=False)
        if respuesta.status_code == 200:
            resultado = respuesta.json().get("results", [])
            
            # IDs de variables clave en BCRA:
            # 1: Reservas Internacionales
            # 4: Base Monetaria
            # 6: Tasa de Política Monetaria
            # 7: Badlar bancos privados
            claves = {1: "Reservas Internacionales", 4: "Base Monetaria", 6: "Tasa Política Monetaria", 7: "Tasa Badlar"}
            
            filtrados = [item for item in resultado if item.get("idVariable") in claves]
            
            if db:
                for item in filtrados:
                    registro = VariableMacro(
                        fuente="BCRA",
                        id_variable=item.get("idVariable"),
                        descripcion=claves.get(item.get("idVariable"), item.get("descripcion", "")),
                        valor=float(item.get("valor", 0) or 0),
                        fecha_dato=str(item.get("fecha"))
                    )
                    db.add(registro)
                db.commit()
                
            return filtrados
        return {"error": f"BCRA API devolvió código {respuesta.status_code}"}
    except Exception as e:
        return {"error": str(e)}
