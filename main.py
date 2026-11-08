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

# Corrección de protocolo para SQLAlchemy si viene como postgres://
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL) if DATABASE_URL else None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None
Base = declarative_base()

# Modelo de la Tabla en PostgreSQL
class CotizacionDolar(Base):
    __tablename__ = "cotizaciones_dolar"

    id = Column(Integer, primary_key=True, index=True)
    casa = Column(String, index=True)
    nombre = Column(String)
    compra = Column(Float)
    venta = Column(Float)
    fecha_actualizacion = Column(String)
    fecha_registro = Column(DateTime, default=datetime.utcnow)

# Crear tablas automáticamente al arrancar
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
        
        # Si la base de datos está conectada, guardamos el registro actual
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
    return {"error": "No se pudo conectar a la API"}

@app.get("/dolar/historial")
def ver_historial(limite: int = 20, db: Session = Depends(get_db)):
    if not db:
        return {"error": "Base de datos no disponible"}
    
    registros = db.query(CotizacionDolar).order_by(CotizacionDolar.id.desc()).limit(limite).all()
    return registros
