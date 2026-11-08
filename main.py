import os
from datetime import datetime
from typing import List, Optional
import requests
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

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

def consultar_serie_simple(serie_id: str):
    url = f"https://apis.datos.gob.ar/series/api/series/?ids={serie_id}&limit=1&sort=desc"
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data and len(data[0]) >= 2:
                return {"fecha": data[0][0], "valor": data[0][1]}
    except Exception:
        pass
    return None

@app.get("/bcra/variables")
def ver_bcra(db: Session = Depends(get_db)):
    series = [
        {"clave": "DOLAR_A3500", "id": "168.1_T_CAMBIOR_D_0_0_26", "desc": "Tipo de Cambio Mayorista Comunicación A3500"},
        {"clave": "RESERVAS_USD", "id": "174.1_RO_D_0_0_21", "desc": "Reservas Internacionales (Millones USD)"},
        {"clave": "BASE_MONETARIA", "id": "174.1_BMA_M_0_0_15", "desc": "Base Monetaria (Millones ARS)"},
        {"clave": "BADLAR", "id": "174.1_TB_D_0_0_19", "desc": "Tasa Badlar Bancos Privados (% TNA)"}
    ]
    
    resultado = []
    for item in series:
        dato = consultar_serie_simple(item["id"])
        if dato and dato["valor"] is not None:
            registro_dict = {
                "variable": item["clave"],
                "descripcion": item["desc"],
                "valor": float(dato["valor"]),
                "fecha": dato["fecha"]
            }
            resultado.append(registro_dict)
            
            if db:
                try:
                    db_record = VariableMacro(
                        fuente="BCRA",
                        id_variable=item["clave"],
                        descripcion=item["desc"],
                        valor=float(dato["valor"]),
                        fecha_dato=str(dato["fecha"])
                    )
                    db.add(db_record)
                except Exception:
                    db.rollback()
    
    if db and resultado:
        try:
            db.commit()
        except Exception:
            db.rollback()
        
    return resultado

@app.get("/mercado/resumen")
def ver_resumen_mercado(db: Session = Depends(get_db)):
    # Endpoint consolidado con cotizaciones, mayorista y brechas calculadas
    try:
        r_dolar = requests.get("https://dolarapi.com/v1/dolares", timeout=10)
        dolares = r_dolar.json() if r_dolar.status_code == 200 else []
        
        # Mapear precios de venta
        precios = {d["casa"]: float(d.get("venta", 0) or 0) for d in dolares}
        
        # Buscar mayorista (preferencia A3500 oficial o de dolarapi)
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
