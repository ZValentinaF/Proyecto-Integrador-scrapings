import requests
from bs4 import BeautifulSoup
import json
import re
import os
from datetime import datetime

# ----------------------------
# Configuración de rutas
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------
# Diccionario de meses
# ----------------------------
MESES = {
    "enero": "01", "febrero": "02", "marzo": "03",
    "abril": "04", "mayo": "05", "junio": "06",
    "julio": "07", "agosto": "08", "septiembre": "09",
    "octubre": "10", "noviembre": "11", "diciembre": "12"
}

# ----------------------------
# Funciones de normalización
# ----------------------------
def convertir_fecha_simple(fecha_txt: str, year: int):
    try:
        partes = fecha_txt.replace("de", "").split()
        if len(partes) >= 2:
            dia = partes[0]
            mes = MESES.get(partes[1].lower())
            if mes:
                return f"{year}-{mes.zfill(2)}-{dia.zfill(2)}"
    except Exception:
        return fecha_txt.title()
    return fecha_txt.title()

def normalizar_fecha_es(fecha_raw: str, year: int = datetime.now().year):
    if not fecha_raw or fecha_raw == "N/A":
        return {"fecha_inicio": None, "fecha_fin": None, "hora": None}
    fecha_raw = fecha_raw.strip().lower()
    if "-" in fecha_raw:
        partes = [p.strip() for p in fecha_raw.split("-")]
        fecha = convertir_fecha_simple(partes[0], year)
        hora = partes[1] if len(partes) > 1 else None
        return {"fecha_inicio": fecha, "fecha_fin": fecha, "hora": hora}
    return {"fecha_inicio": convertir_fecha_simple(fecha_raw, year), "fecha_fin": convertir_fecha_simple(fecha_raw, year), "hora": None}

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
    fecha = normalizar_fecha_es(fecha_elem.get_text(strip=True))

    evento = {
        "nombre": nombre,
        "fecha_inicio": fecha["fecha_inicio"],
        "fecha_fin": fecha["fecha_fin"],
        "hora": fecha["hora"]
    }
    eventos.append(evento)

ruta_salida = os.path.join(BASE_DIR, "scraping_teatroplasa.json")
with open(ruta_salida, "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

print(json.dumps(eventos, indent=4, ensure_ascii=False))
print(f"✅ {len(eventos)} eventos normalizados guardados en {ruta_salida}")
