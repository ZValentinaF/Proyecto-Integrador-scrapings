import requests
from bs4 import BeautifulSoup
import re
import json

# ----------------------------
# Funciones de normalización
# ----------------------------
def normalizar_fecha(fecha_raw: str):
    """
    Convierte fechas al formato estándar.
    Si hay día y mes -> devuelve en Title Case.
    """
    if not fecha_raw or fecha_raw == "N/A":
        return None
    return fecha_raw.title().strip()

def normalizar_ingreso(ingreso_raw: str):
    """
    Estandariza el ingreso en categorías fijas.
    """
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
    """
    Limpia nombres eliminando números o basura extra.
    """
    return re.sub(r"\d+", "", nombre_raw).strip()

def normalizar_tipo(tipo_raw: str):
    """
    Establece tipos de evento normalizados.
    """
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
        # Guardar en archivo JSON
        with open("scraping_teatropablotobon.json", "w", encoding="utf-8") as f:
            json.dump(eventos, f, indent=4, ensure_ascii=False)

        # Mostrar en consola con formato
        print(json.dumps(eventos, indent=4, ensure_ascii=False))
