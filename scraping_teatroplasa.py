import requests
from bs4 import BeautifulSoup
import json
import re
import unicodedata
from datetime import date

# ----------------------------
# Funciones de normalización
# ----------------------------
def _sin_acentos(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')

MESES = {
    "enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05","junio":"06",
    "julio":"07","agosto":"08","septiembre":"09","setiembre":"09","octubre":"10",
    "noviembre":"11","diciembre":"12"
}

def _infer_year(mes: int, dia: int, hoy: date | None = None) -> int:
    hoy = hoy or date.today()
    y = hoy.year
    try:
        d = date(y, mes, dia)
        return y if d >= hoy else y + 1
    except ValueError:
        return y

def normalizar_fecha_es(fecha_raw: str, hoy: date | None = None) -> str | None:
    """Convierte '26 De Septiembre' → '2025-09-26'"""
    if not fecha_raw: return None
    s = _sin_acentos(fecha_raw.strip().lower())
    m = re.match(r'^(\d{1,2})\s+de\s+([a-z]+)(?:\s+de\s+(\d{4}))?$', s) or \
        re.match(r'^(\d{1,2})\s+([a-z]+)(?:\s+(\d{4}))?$', s)
    if not m: return None
    dia = int(m.group(1)); mes_txt = m.group(2); anio = m.group(3)
    mes = MESES.get(mes_txt)
    if not mes: return None
    y = int(anio) if anio else _infer_year(int(mes), dia, hoy=hoy)
    return f"{y}-{mes}-{dia:02d}"

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
    fecha_raw = fecha_elem.get_text(strip=True)
    fecha = normalizar_fecha_es(fecha_raw)

    evento = {
        "nombre": nombre,
        "fecha": fecha
    }
    eventos.append(evento)

# 👉 Guardar en archivo JSON
with open("scraping_teatroplasa.json", "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

# 👉 Mostrar en consola con formato
print(json.dumps(eventos, indent=4, ensure_ascii=False))
print("✅ Archivo JSON normalizado creado correctamente")
