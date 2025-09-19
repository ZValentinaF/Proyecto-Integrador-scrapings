import requests
from bs4 import BeautifulSoup
import json
import re

# ----------------------------
# Funciones de normalización
# ----------------------------
def normalizar_fecha(fecha_raw: str):
    """
    Normaliza las fechas al formato 'DD de Mes YYYY' o título capitalizado.
    """
    if not fecha_raw or fecha_raw == "N/A":
        return None
    fecha = fecha_raw.strip().title()
    return fecha

def limpiar_nombre(nombre_raw: str):
    """
    Limpia nombres eliminando números o residuos innecesarios.
    """
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

# 👉 Guardar en archivo JSON
with open("scraping_teatroplasa.json", "w", encoding="utf-8") as f: json.dump(eventos, f, indent=4, ensure_ascii=False)

# 👉 Mostrar en consola con formato
print(json.dumps(eventos, indent=4, ensure_ascii=False))
print("✅ Archivo JSON normalizado creado correctamente")
