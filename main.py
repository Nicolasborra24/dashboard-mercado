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

def parsear_monto_celda(texto: str) -> Optional[float]:
    # Extrae el valor numérico puro respetando multiplicadores B (billones/billions) y M (millones)
    t = texto.strip().upper()
    multiplicador = 1.0
    if "B" in t:
        multiplicador = 1000.0 # Expresado en Miles de Millones
    elif "M" in t:
        multiplicador = 1.0

    numeros = re.findall(r"[\d]+(?:[.,]\d+)?", t)
    if numeros:
        num_str = numeros[0].replace(",", ".")
        try:
            return round(float(num_str) * multiplicador, 2)
        except Exception:
            pass
    return None

@app.get("/bcra/variables")
def ver_bcra(db: Session = Depends(get_db)):
    resultado = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # 1. Extracción precisa por posición de celda desde indicadores.ar
    try:
        r_ind = requests.get("https://indicadores.ar/indicadores-economicos", headers=headers, timeout=10)
        if r_ind.status_code == 200:
            soup = BeautifulSoup(r_ind.text, "html.parser")
            
            for fila in soup.find_all("tr"):
                celdas = fila.find_all(["td", "th"])
                if len(celdas) >= 4:
                    nombre = celdas[0].get_text(strip=True)
                    val_str = celdas[1].get_text(strip=True)
                    fecha_str = celdas[3].get_text(strip=True)

                    if "Reservas" in nombre and not any(x["variable"] == "RESERVAS_USD" for x in resultado):
                        val = parsear_monto_celda(val_str)
                        if val is not None:
                            resultado.append({
                                "variable": "RESERVAS_USD",
                                "descripcion": "Reservas Internacionales BCRA (Millones USD)",
                                "valor_formateado": val_str,
                                "valor": val,
                                "fecha": fecha_str
                            })

                    elif "Base" in nombre and "Monetaria" in nombre and
git add .
git commit -m "Extraccion fija por posicion de celdas para Reservas y Base Monetaria"
git push origin main
git status
