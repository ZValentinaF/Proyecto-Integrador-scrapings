import requests
from bs4 import BeautifulSoup
import json
import re
import os

# ----------------------------
# Configuración de rutas
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------
# Funciones de normalización
# ----------------------------
def normalizar_fecha(fecha_raw: str):
    if not fecha_raw or fecha_raw == "N/A":
        return None
    return fecha_raw.strip().title()

def limpiar_nombre(nombre_raw: str):
    return re.sub(r"\d+", "", nombre_raw).strip()

# ----------------------------
# Scraping
# ----------------------------
url = "https://teatroastorplaza.com"
response = requests.get(url, timeout=15)
response.encoding = "utf-8"
soup = BeautifulSoup(response.text, "html.parser")

eventos = []

nombres = soup.find_all("h2", class_="elementor-heading-title")
fechas = soup.find_all("span", style="vertical-align: inherit;")

for nombre_elem, fecha_elem in zip(nombres, fechas):
    nombre = limpiar_nombre(nombre_elem.get_text(strip=True))
    fecha = normalizar_fecha(fecha_elem.get_text(strip=True))

    evento = {
        "nombre": nombre,
        "fecha": fecha
    }
    eventos.append(evento)

# 👉 Guardar en archivo JSON dentro del proyecto
ruta_salida = os.path.join(BASE_DIR, "scraping_teatroplasa.json")
with open(ruta_salida, "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

print(json.dumps(eventos, indent=4, ensure_ascii=False))
print("✅ Archivo JSON guardado en carpeta del proyecto")
