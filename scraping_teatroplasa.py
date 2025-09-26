import requests
from bs4 import BeautifulSoup
import json
import re
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------
# Funciones de normalización
# ----------------------------
def normalizar_fecha(fecha_raw: str):
    """
    Normaliza fechas al formato 'YYYY-MM-DD' si es posible.
    Si no se puede, devuelve texto capitalizado.
    """
    if not fecha_raw or fecha_raw == "N/A":
        return None

    fecha = fecha_raw.strip().title()

    # Reemplazar nombres de meses a números
    meses = {
        "Enero": "01", "Febrero": "02", "Marzo": "03", "Abril": "04",
        "Mayo": "05", "Junio": "06", "Julio": "07", "Agosto": "08",
        "Septiembre": "09", "Octubre": "10", "Noviembre": "11", "Diciembre": "12"
    }

    patron = re.search(r"(\d{1,2})\s+De\s+([A-Za-z]+)", fecha, re.IGNORECASE)
    if patron:
        dia = patron.group(1).zfill(2)
        mes = meses.get(patron.group(2).capitalize(), None)
        if mes:
            return f"2025-{mes}-{dia}"  # 🔹 año fijo (puedes mejorarlo con datetime.now().year)

    return fecha

def limpiar_nombre(nombre_raw: str):
    """Limpia nombres eliminando números o basura extra."""
    return re.sub(r"\d+", "", nombre_raw).strip()

# ----------------------------
# Scraping
# ----------------------------
def scrape_teatroplaza():
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

        if nombre and fecha:  # 🔹 solo eventos válidos
            evento = {
                "nombre": nombre,
                "fecha": fecha  # 🔹 ahora un solo campo, compatible con la tabla
            }
            eventos.append(evento)

    # Guardar en archivo JSON
    archivo_salida = os.path.join(BASE_DIR, "scraping_teatroplasa.json")
    with open(archivo_salida, "w", encoding="utf-8") as f:
        json.dump(eventos, f, indent=4, ensure_ascii=False)

    print(json.dumps(eventos, indent=4, ensure_ascii=False))
    print(f"✅ Archivo JSON creado: {archivo_salida}")

if __name__ == "__main__":
    scrape_teatroplaza()
