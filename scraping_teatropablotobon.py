import requests
from bs4 import BeautifulSoup
import re
import json

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

        # === CHIPS: tipo e ingreso por separado ===
        tipo, ingreso = "N/A", "N/A"
        chips_block = t.find_previous("div", class_="chips")
        if chips_block:
            # Buscar tipo
            tipo_chip = chips_block.find(
                "div",
                class_=re.compile(r"chips__chip.*(musica|teatro|danza|comedia|otros)", re.IGNORECASE)
            )
            if tipo_chip:
                tipo = tipo_chip.get_text(strip=True)

            # Buscar ingreso
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

        eventos_data.append({
            "tipo": tipo,
            "nombre": nombre,
            "fecha": fecha,
            "ingreso": ingreso
        })

    return eventos_data


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
