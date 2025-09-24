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

def normalizar_fecha(fecha_raw: str, hoy: date | None = None):
    """
    Convierte fechas a formato estándar.
    Maneja rangos y horas.
    Devuelve dict {fecha_inicio, fecha_fin, hora}
    """
    if not fecha_raw or fecha_raw == "N/A":
        return {"fecha_inicio": None, "fecha_fin": None, "hora": None}

    s = _sin_acentos(fecha_raw.strip().lower())

    # Caso rango: "26 de septiembre al 7 de octubre"
    if " al " in s:
        partes = s.split(" al ")
        ini = normalizar_fecha(partes[0].strip(), hoy=hoy)["fecha_inicio"]
        fin = normalizar_fecha(partes[1].strip(), hoy=hoy)["fecha_inicio"]
        return {"fecha_inicio": ini, "fecha_fin": fin, "hora": None}

    # Caso fecha con hora: "25 de septiembre - 7:00 pm"
    if "-" in s:
        partes = [p.strip() for p in s.split("-", 1)]
        f = _parse_fecha(partes[0], hoy=hoy)
        hora = partes[1].upper() if len(partes) > 1 else None
        return {"fecha_inicio": f, "fecha_fin": f, "hora": hora}

    # Fecha simple
    f = _parse_fecha(s, hoy=hoy)
    return {"fecha_inicio": f, "fecha_fin": f, "hora": None}

def _parse_fecha(txt: str, hoy: date | None = None) -> str | None:
    m = re.match(r"^(\d{1,2})\s+de\s+([a-z]+)(?:\s+de\s+(\d{4}))?$", txt)
    if not m:
        return None
    dia = int(m.group(1))
    mes_txt = m.group(2)
    anio = m.group(3)
    mes = MESES.get(mes_txt)
    if not mes:
        return None
    y = int(anio) if anio else _infer_year(int(mes), dia, hoy)
    return f"{y}-{mes}-{dia:02d}"

def normalizar_ingreso(ingreso_raw: str):
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
    return re.sub(r"\d+", "", nombre_raw).strip()

# ----------------------------
# Scraping
# ----------------------------
def scrape_idartes():
    url = "https://www.idartes.gov.co/es/agenda"
    resp = requests.get(url, timeout=15)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

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
        # Normalización
        # ----------------------------
        nombre = limpiar_nombre(nombre)
        ingreso = normalizar_ingreso(ingreso)
        fecha_norm = normalizar_fecha(fecha)

        evento = {
            "tipo": tipo,
            "nombre": nombre,
            "fecha_inicio": fecha_norm["fecha_inicio"],
            "fecha_fin": fecha_norm["fecha_fin"],
            "hora": fecha_norm["hora"],
            "ingreso": ingreso
        }
        eventos.append(evento)

    return eventos

# ----------------------------
# Ejecución
# ----------------------------
if __name__ == "__main__":
    eventos = scrape_idartes()

    if not eventos:
        print("⚠️ No se encontraron eventos.")
    else:
        total = len(eventos)
        validos = sum(1 for ev in eventos if ev.get("fecha_inicio"))
        invalidos = total - validos

        print("📊 Resumen de scraping Idartes")
        print(f"   Total eventos encontrados: {total}")
        print(f"   Con fecha válida: {validos}")
        print(f"   Sin fecha válida: {invalidos}")

        if eventos:
            print("\n🔎 Ejemplo de evento normalizado:")
            print(eventos[0])

        with open("scraping_idartes.json", "w", encoding="utf-8") as f:
            json.dump(eventos, f, indent=4, ensure_ascii=False)

        print("\n📥 JSON completo:")
        print(json.dumps(eventos, indent=4, ensure_ascii=False))
        print("✅ Archivo JSON normalizado creado correctamente")
