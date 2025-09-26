import requests
from bs4 import BeautifulSoup
import re
import json
import os
from datetime import datetime

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
        return None
    return convertir_fecha_simple(fecha_raw, year)

def normalizar_ingreso(ingreso_raw: str):
    ingreso_raw = ingreso_raw.lower()
    if "libre" in ingreso_raw:
        return "LIBRE"
    elif "costo" in ingreso_raw or "$" in ingreso_raw:
        return "COSTO"
    elif "inscripción" in ingreso_raw:
        return "INSCRIPCION"
    return "OTRO"

def limpiar_nombre(nombre_raw: str):
    return re.sub(r"\d+", "", nombre_raw).strip()

def normalizar_tipo(tipo_raw: str):
    tipo_raw = tipo_raw.lower()
    if "musica" in tipo_raw or "música" in tipo_raw:
        return "MÚSICA"
    elif "teatro" in tipo_raw:
        return "TEATRO"
    elif "danza" in tipo_raw:
        return "DANZA"
    elif "comedia" in tipo_raw:
        return "COMEDIA"
    return "OTROS"

def scrape_eventos():
    url = "https://teatropablotobon.com/eventos/"
    resp = requests.get(url, timeout=15)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    eventos_data = []
    titulos = soup.find_all("h2")

    for t in titulos:
        nombre = t.get_text(strip=True)
        if not nombre or "pasados" in nombre.lower():
            continue

        tipo, ingreso = "N/A", "N/A"
        chips_block = t.find_previous("div", class_="chips")
        if chips_block:
            tipo_chip = chips_block.find(
                "div",
                class_=re.compile(r"chips__chip.*(musica|teatro|danza|comedia|otros)", re.IGNORECASE)
            )
            if tipo_chip:
                tipo = tipo_chip.get_text(strip=True)

            ingreso_chip = chips_block.find("div", class_=re.compile(r"chips__chip.*entrada", re.IGNORECASE))
            if ingreso_chip:
                ingreso = ingreso_chip.get_text(strip=True)

        fecha = "N/A"
        fecha_block = t.find_next("div")
        if fecha_block:
            fecha_tags = fecha_block.find_all("p", class_="mb-0")
            fecha_text = " ".join([f.get_text(strip=True) for f in fecha_tags]) if fecha_tags else ""
            fecha_match = re.search(r"(\d{1,2}\s+de\s+\w+)", fecha_text, re.IGNORECASE)
            if fecha_match:
                fecha = fecha_match.group(1)

        evento = {
            "tipo": normalizar_tipo(tipo),
            "nombre": limpiar_nombre(nombre),
            "fecha": normalizar_fecha_es(fecha),
            "ingreso": normalizar_ingreso(ingreso)
        }

        eventos_data.append(evento)

    return eventos_data

if __name__ == "__main__":
    eventos = scrape_eventos()
    ruta_salida = os.path.join(BASE_DIR, "scraping_teatropablotobon.json")
    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(eventos, f, indent=4, ensure_ascii=False)

    print(json.dumps(eventos, indent=4, ensure_ascii=False))
    print(f"✅ {len(eventos)} eventos normalizados guardados en {ruta_salida}")
