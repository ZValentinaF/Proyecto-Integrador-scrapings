import requests
from bs4 import BeautifulSoup
import json

base_url = "https://www.teatropablotobon.com/programacion"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

eventos = []
start = 0
max_pages = 10
page_count = 0

while page_count < max_pages:
    url = f"{base_url}?start={start}" if start > 0 else base_url
    response = requests.get(url, headers=headers, timeout=15)
    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "html.parser")

    contenedores = soup.select("div.event-description")

    if not contenedores:
        break

    for cont in contenedores:
        tipo_elem = cont.select_one("h3")
        tipo = tipo_elem.get_text(strip=True) if tipo_elem else "N/A"

        nombre_elem = cont.select_one("a")
        if nombre_elem:
            nombre = nombre_elem.get_text(strip=True)
            if not nombre:
                href = nombre_elem.get("href", "")
                if href:
                    slug = href.strip("/").split("/")[-1]
                    nombre = slug.replace("-", " ").title()
        else:
            nombre = "N/A"

        mes = cont.select_one(".month")
        dia = cont.select_one(".day")
        fecha = f"{dia.get_text(strip=True)} {mes.get_text(strip=True)}" if mes and dia else "N/A"

        ingreso_elem = cont.find_previous("span", class_="price")
        ingreso = ingreso_elem.get_text(strip=True) if ingreso_elem else "N/A"

        eventos.append({
            "tipo": tipo,
            "nombre": nombre,
            "fecha": fecha,
            "ingreso": ingreso
        })

    start += 12
    page_count += 1

with open("scraping_teatropablotobon.json", "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

print(json.dumps(eventos, indent=4, ensure_ascii=False))
