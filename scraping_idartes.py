# scraping_idartes.py
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import os
from typing import Dict, Any, List, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MESES = {
    "enero": "01", "febrero": "02", "marzo": "03",
    "abril": "04", "mayo": "05", "junio": "06",
    "julio": "07", "agosto": "08", "septiembre": "09",
    "octubre": "10", "noviembre": "11", "diciembre": "12"
}


def convertir_fecha_simple(fecha_txt: str, year: int) -> Optional[str]:
    """
    Convierte textos tipo '10 de noviembre' -> 'YYYY-11-10'
    Devuelve None si no puede convertir.
    """
    if not fecha_txt:
        return None
    try:
        partes = fecha_txt.replace("de", " ").split()
        partes = [p.strip() for p in partes if p.strip()]
        if len(partes) >= 2:
            dia = partes[0]
            mes = MESES.get(partes[1].lower())
            if mes and dia.isdigit():
                return f"{year}-{mes.zfill(2)}-{dia.zfill(2)}"
    except Exception:
        return None
    return None


def normalizar_fecha_es(fecha_raw: str, year: int = datetime.now().year) -> Dict[str, Optional[str]]:
    """
    Normaliza fechas en español que pueden venir como:
      - '10 al 12 de noviembre'
      - '5 de diciembre - 7:30 p.m.'
      - '12 de noviembre'
    Retorna dict con: fecha_inicio, fecha_fin, hora (strings o None).
    No 'adivina' formatos raros: si no puede, deja None lo que no se pueda inferir.
    """
    out = {"fecha_inicio": None, "fecha_fin": None, "hora": None}
    if not fecha_raw or fecha_raw == "N/A":
        return out

    txt = " ".join(fecha_raw.split()).strip().lower()

    # Capturar hora si viene en el texto (p. ej., " - 7:00 p.m." o "7:00 pm")
    hora_match = re.search(r'(\d{1,2}[:.]\d{2}\s*(a\.?m\.?|p\.?m\.?)?)', txt, re.IGNORECASE)
    if hora_match:
        out["hora"] = hora_match.group(1)

    # Rango: "10 al 12 de noviembre"
    if " al " in txt:
        partes = [p.strip() for p in txt.split(" al ")]
        # partes[0] ~ "10" o "10 de noviembre"
        # partes[1] ~ "12 de noviembre"
        # Tomamos el mes del segundo fragmento
        m2 = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)", partes[1], re.IGNORECASE)
        if m2:
            dia2 = m2.group(1)
            mes_txt = m2.group(2).lower()
            mes2 = MESES.get(mes_txt, None)
            # fecha inicio: intentamos "10 de <mes_txt>" si el primero no trae mes
            m1 = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)", partes[0], re.IGNORECASE)
            if m1:
                dia1 = m1.group(1)
                mes1 = MESES.get(m1.group(2).lower(), None)
            else:
                # Primer fragmento solo trae día
                m1d = re.search(r"(\d{1,2})", partes[0])
                dia1 = m1d.group(1) if m1d else None
                mes1 = mes2

            if mes1 and mes2 and dia1 and dia2:
                out["fecha_inicio"] = f"{year}-{mes1.zfill(2)}-{str(dia1).zfill(2)}"
                out["fecha_fin"] = f"{year}-{mes2.zfill(2)}-{str(dia2).zfill(2)}"
                return out
        # Si no se pudo parsear como rango, caemos a intentos simples más abajo.

    # Caso "5 de diciembre - 7:30 p.m." o "5 de diciembre"
    m = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)", txt, re.IGNORECASE)
    if m:
        dia = m.group(1)
        mes_txt = m.group(2).lower()
        mes = MESES.get(mes_txt, None)
        if mes and dia.isdigit():
            fecha = f"{year}-{mes.zfill(2)}-{dia.zfill(2)}"
            out["fecha_inicio"] = fecha
            out["fecha_fin"] = fecha
            return out

    # Último recurso: intenta 'simple' con el primer número + primer mes encontrado
    m_dia = re.search(r"(\d{1,2})", txt)
    m_mes = re.search(r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)", txt, re.IGNORECASE)
    if m_dia and m_mes:
        dia = m_dia.group(1)
        mes = MESES.get(m_mes.group(1).lower(), None)
        if mes and dia.isdigit():
            fecha = f"{year}-{mes.zfill(2)}-{dia.zfill(2)}"
            out["fecha_inicio"] = fecha
            out["fecha_fin"] = fecha

    return out


def normalizar_ingreso(ingreso_raw: str) -> str:
    """
    Normaliza 'Entrada libre', 'Con costo', 'Inscripción', etc.
    Retorna una etiqueta breve: LIBRE | COSTO | INSCRIPCION | OTRO
    """
    if not ingreso_raw:
        return "OTRO"
    t = ingreso_raw.strip().lower()
    if "libre" in t or "gratuit" in t:
        return "LIBRE"
    if "costo" in t or "$" in t or "pago" in t or "bole" in t or "entrada" in t:
        return "COSTO"
    if "inscrip" in t:
        return "INSCRIPCION"
    return "OTRO"


def limpiar_nombre(nombre_raw: str) -> str:
    """
    Limpia el nombre del evento eliminando números 'ruidosos'
    y espacios redundantes.
    """
    if not nombre_raw:
        return "N/A"
    s = re.sub(r"\d+", "", nombre_raw).strip()
    # Normaliza espacios múltiples
    s = re.sub(r"\s{2,}", " ", s)
    return s


def scrape_idartes() -> List[Dict[str, Any]]:
    """
    Descarga y parsea la agenda de Idartes.
    Devuelve una lista de dicts con:
      - tipo, nombre, fecha_inicio, fecha_fin, hora, ingreso, url
    """
    url = "https://www.idartes.gov.co/es/agenda"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "html.parser")

    eventos: List[Dict[str, Any]] = []
    contenedores = soup.find_all("div", class_="cajashomeeventos")

    for cont in contenedores:
        # Tipo
        tipo_elem = cont.find("div", class_="ctg-ev-24 position-absolute bg-white")
        tipo = tipo_elem.get_text(strip=True) if tipo_elem else "N/A"

        # Nombre + URL
        nombre_elem = cont.select_one('a[hreflang="es"]')
        url_oficial = None
        if nombre_elem:
            nombre = nombre_elem.get_text(strip=True)
            href = nombre_elem.get("href") or ""
            if href.startswith("/"):
                url_oficial = "https://www.idartes.gov.co" + href
            elif href.startswith("http"):
                url_oficial = href
            # Si no hay texto, inferir desde el slug
            if not nombre:
                if href:
                    slug = href.strip("/").split("/")[-1]
                    nombre = slug.replace("-", " ").title()
        else:
            nombre = "N/A"

        # Fecha bruta + ingreso
        fecha_elem = cont.find("div", class_="fecha-ev24")
        fecha_raw = fecha_elem.get_text(" ", strip=True) if fecha_elem else "N/A"

        ingreso_elem = cont.find("div", class_="tipo_cajashomeeventos font2")
        ingreso_raw = ingreso_elem.get_text(strip=True) if ingreso_elem else "N/A"

        # Normalizaciones
        nombre = limpiar_nombre(nombre)
        ingreso = normalizar_ingreso(ingreso_raw)
        fecha_norm = normalizar_fecha_es(fecha_raw)

        evento = {
            "tipo": tipo,
            "nombre": nombre,
            "fecha_inicio": fecha_norm["fecha_inicio"],
            "fecha_fin": fecha_norm["fecha_fin"],
            "hora": fecha_norm["hora"],
            "ingreso": ingreso,
            "url": url_oficial
        }
        eventos.append(evento)

    return eventos


if __name__ == "__main__":
    # Ejecuta el scraping y guarda el JSON localmente
    eventos = scrape_idartes()
    ruta_salida = os.path.join(BASE_DIR, "scraping_idartes.json")
    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(eventos, f, indent=4, ensure_ascii=False)

    print(json.dumps(eventos, indent=4, ensure_ascii=False))
    print(f"✅ {len(eventos)} eventos normalizados guardados en {ruta_salida}")
