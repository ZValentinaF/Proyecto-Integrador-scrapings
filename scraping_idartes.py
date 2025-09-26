import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MESES = {
    "enero": "01", "febrero": "02", "marzo": "03",
    "abril": "04", "mayo": "05", "junio": "06",
    "julio": "07", "agosto": "08", "septiembre": "09",
    "octubre": "10", "noviembre": "11", "diciembre": "12"
}

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

    fecha_raw = fecha_raw.lower()

    if "al" in fecha_raw:
        partes = [p.strip() for p in fecha_raw.split("al")]
        return {
            "fecha_inicio": convertir_fecha_simple(partes[0], year),
            "fecha_fin": convertir_fecha_simple(partes[1], year),
            "hora": None
        }

    if "-" in fecha_raw:
        partes = [p.strip() for p in fecha_raw.split("-")]
        fecha = convertir_fecha_simple(partes[0], year)
        hora = partes[1] if len(partes) > 1 else None
        return {"fecha_inicio": fecha, "fecha_fin": fecha, "hora": hora}

    fecha = convertir_fecha_simple(fecha_raw, year)
    return {"fecha_inicio": fecha, "fecha_fin": fecha, "hora": None}

def normalizar_ingreso(ingreso_raw: str):
    ingreso_raw = ingreso_raw.lower()
    if "libre" in ingreso_raw:
        return "LIBRE"
    elif "costo" in ingreso_raw:
        return "COSTO"
    elif "inscripción" in ingreso_raw:
        return "INSCRIPCION"
    return "OTRO"

def limpiar_nombre(nombre_raw: str):
    return re.sub(r"\d+", "", nombre_raw).strip()

url = "https://www.idartes.gov.co/es/agenda"
response = requests.get(url, timeout=15)
response.encoding = "utf-8"
soup = BeautifulSoup(response.text, "html.parser")

eventos = []
contenedores = soup.find_all("div", class_="cajashomeeventos")

for cont in contenedores:
    tipo_elem = cont.find("div", class_="ctg-ev-24 position-absolute bg-white")
    tipo = tipo_elem.get_text(strip=True) if tipo_elem else "N/A"

    nombre_elem = cont.select_one('a[hreflang="es"]')
    if nombre_elem:
        nombre = nombre_elem.get_text(strip=True)
        if not nombre:
            href = nombre_elem.get("href", "")
            if href:
                slug = href.strip("/").split("/")[-1]
                nombre = slug.replace("-", " ").title()
    else:
        nombre = "N/A"

    fecha_elem = cont.find("div", class_="fecha-ev24")
    fecha = fecha_elem.get_text(" ", strip=True) if fecha_elem else "N/A"

    ingreso_elem = cont.find("div", class_="tipo_cajashomeeventos font2")
    ingreso = ingreso_elem.get_text(strip=True) if ingreso_elem else "N/A"

    nombre = limpiar_nombre(nombre)
    ingreso = normalizar_ingreso(ingreso)
    fecha_normalizada = normalizar_fecha_es(fecha)

    evento = {
        "tipo": tipo,
        "nombre": nombre,
        "fecha_inicio": fecha_normalizada["fecha_inicio"],
        "fecha_fin": fecha_normalizada["fecha_fin"],
        "hora": fecha_normalizada["hora"],
        "ingreso": ingreso
    }

    eventos.append(evento)

ruta_salida = os.path.join(BASE_DIR, "scraping_idartes.json")
with open(ruta_salida, "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

print(json.dumps(eventos, indent=4, ensure_ascii=False))
print(f"✅ {len(eventos)} eventos normalizados guardados en {ruta_salida}")
