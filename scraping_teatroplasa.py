import requests
from bs4 import BeautifulSoup
import json

url = "https://teatroastorplaza.com"
response = requests.get(url, timeout=15)
response.encoding = "utf-8"
soup = BeautifulSoup(response.text, "html.parser")

eventos = []

nombres = soup.find_all("h2", class_="elementor-heading-title")
fechas = soup.find_all("span", style="vertical-align: inherit;")

for nombre_elem, fecha_elem in zip(nombres, fechas):
    nombre = nombre_elem.get_text(strip=True)
    fecha = fecha_elem.get_text(strip=True)
    eventos.append({"nombre": nombre, "fecha": fecha})

# 👉 Imprime en consola en formato JSON
print(json.dumps(eventos, indent=4, ensure_ascii=False))

# 👉 Guarda en archivo JSON válido
with open("scraping_teatroplasa.json", "w", encoding="utf-8") as f:
    json.dump(eventos, f, indent=4, ensure_ascii=False)

print("✅ Archivo JSON creado correctamente")
