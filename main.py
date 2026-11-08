import requests
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def inicio():
    return {"mensaje": "Servidor funcionando correctamente"}

@app.get("/dolar")
def ver_dolar():
    url = "https://dolarapi.com/v1/dolares"
    respuesta = requests.get(url, timeout=10)
    if respuesta.status_code == 200:
        return respuesta.json()
    return {"error": "No se pudo conectar a la API"}
