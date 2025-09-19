import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

# ----------------------------
# Funciones de normalización
# ----------------------------
def normalizar_fecha(fecha_raw: str):
    """
    Convierte fechas a un formato estándar.
    Si es un rango: devuelve inicio y fin.
    Si es una fecha con hora: separa ambos.
    """
    if not fecha_raw or fecha_raw == "N/A":
        return {"fecha_inicio": None, "fecha_fin": None, "hora": None}

    fecha_raw = fecha_raw.lower()

    # Detectar rango de fechas
    if "al" in fecha_raw:
        partes = fecha_raw.split("al")
        return {
            "fecha_inicio": partes[0].strip().title(),
            "fecha_fin": partes[1].strip().title(),
            "hora": None
        }
    else:
        # Separar fecha y hora si existen
        if "-" in fecha_raw:
            partes = fecha_raw.split("-")
            fecha = partes[0].strip().title()
            hora = partes[1].strip()
            return {"fecha_inicio": fecha, "fecha_fin": fecha, "hora": hora}
        else:
            return {"fecha_inicio": fecha_raw.title(), "fecha_fin": fecha_raw.title(), "hora": None}


def normalizar_ingreso(ingreso_raw: str):
    """
    Estandariza el tipo de ingreso.
    """
    ingreso_raw = ingreso_raw.lower()
    if "libre" in ingreso_raw:
        return "LIBRE"
    elif "costo" in ingreso_raw:
        return "COSTO"
    elif "inscripción" in ingreso_raw:
        return "INSCRIPCION"
    else:
        return "OTRO"


def limpiar_nombre(nombre_raw: str):
    """
    Limpia nombres eliminando números innecesarios o basura.
    """
    return re.sub(r"\d+", "", nombre_raw).strip()

# ----------------------------
# Scraping
# ----------------------------
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

    # ----------------------------
    # Aplicar normalización
    # ----------------------------
    nombre = limpiar_nombre(nombre)
    ingreso = normalizar_ingreso(ingreso)
    fecha_normalizada = normalizar_fecha(fecha)

    evento = {
        "tipo": tipo,
        "nombre": nombre,
        "fecha_inicio": fecha_normalizada["fecha_inicio"],
        "fecha_fin": fecha_normalizada["fecha_fin"],
        "hora": fecha_normalizada["hora"],
        "ingreso": ingreso
    }

    eventos.append(evento)

# Guardar en JSON
with open("scraping_idartes.json", "w", encoding="utf-8") as f: json.dump(eventos, f, indent=4, ensure_ascii=False)

# Imprimir salida para verificar
print(json.dumps(eventos, indent=4, ensure_ascii=False))
