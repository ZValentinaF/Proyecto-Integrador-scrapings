import requests
from bs4 import BeautifulSoup
import json

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

    eventos.append({
        "tipo": tipo,
        "nombre": nombre,
        "fecha": fecha,
        "ingreso": ingreso
    })

with open("scraping_idartes.json", "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

print(json.dumps(eventos, indent=4, ensure_ascii=False))
