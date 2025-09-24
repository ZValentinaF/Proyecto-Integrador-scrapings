import requests
from bs4 import BeautifulSoup
import re
import json
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

def normalizar_fecha(fecha_raw: str, hoy: date | None = None) -> str | None:
    """
    Convierte '26 De Septiembre' o '1 Octubre' a 'YYYY-MM-DD'.
    Si no puede interpretar, devuelve None.
    """
    if not fecha_raw or fecha_raw == "N/A":
        return None
    s = _sin_acentos(fecha_raw.strip().lower())
    m = re.match(r'^(\d{1,2})\s+de\s+([a-z]+)(?:\s+de\s+(\d{4}))?$', s) or \
        re.match(r'^(\d{1,2})\s+([a-z]+)(?:\s+(\d{4}))?$', s)
    if not m:
        return None
    dia = int(m.group(1)); mes_txt = m.group(2); anio = m.group(3)
    mes = MESES.get(mes_txt)
    if not mes:
        return None
    y = int(anio) if anio else _infer_year(int(mes), dia, hoy=hoy)
    return f"{y}-{mes}-{dia:02d}"

def normalizar_ingreso(ingreso_raw: str):
    ingreso_raw = ingreso_raw.lower()
    if "libre" in ingreso_raw:
        return "LIBRE"
    elif "costo" in ingreso_raw or "$" in ingreso_raw:
        return "COSTO"
    elif "inscripción" in ingreso_raw:
        return "INSCRIPCION"
    else:
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
    else:
        return "OTROS"

# ----------------------------
# Scraping
# ----------------------------
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

        # === CHIPS: tipo e ingreso ===
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

        # === FECHA ===
        fecha = "N/A"
        fecha_block = t.find_next("div")
        if fecha_block:
            fecha_tags = fecha_block.find_all("p", class_="mb-0")
            fecha_text = " ".join([f.get_text(strip=True) for f in fecha_tags]) if fecha_tags else ""
            fecha_match = re.search(r"(\d{1,2}\s+de\s+\w+)", fecha_text, re.IGNORECASE)
            if fecha_match:
                fecha = fecha_match.group(1)

        # ----------------------------
        # Aplicar normalización
        # ----------------------------
        evento = {
            "tipo": normalizar_tipo(tipo),
            "nombre": limpiar_nombre(nombre),
            "fecha": normalizar_fecha(fecha),
            "ingreso": normalizar_ingreso(ingreso)
        }

        eventos_data.append(evento)

    return eventos_data

# ----------------------------
# Ejecución
# ----------------------------
if __name__ == "__main__":
    eventos = scrape_eventos()

    if not eventos:
        print("⚠️ No se encontraron eventos.")
    else:
        # Resumen en consola
        total = len(eventos)
        validos = sum(1 for ev in eventos if ev.get("fecha"))
        invalidos = total - validos

        print("📊 Resumen de scraping Teatro Pablo Tobón")
        print(f"   Total eventos encontrados: {total}")
        print(f"   Con fecha válida: {validos}")
        print(f"   Sin fecha válida: {invalidos}")

        if eventos:
            print("\n🔎 Ejemplo de evento normalizado:")
            print(eventos[0])

        # Guardar en archivo JSON
        with open("scraping_teatropablotobon.json", "w", encoding="utf-8") as f:
            json.dump(eventos, f, indent=4, ensure_ascii=False)

        # Mostrar JSON completo
        print("\n📥 JSON completo:")
        print(json.dumps(eventos, indent=4, ensure_ascii=False))
        print("✅ Archivo JSON normalizado creado correctamente")
