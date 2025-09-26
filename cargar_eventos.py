import psycopg2
import requests
import json
import ast
import re
from datetime import datetime

DB_CONFIG = {
    "host": "awsaurorapg17-instance-1.cav2004g2f8p.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "dbname": "QueHayPaHacer",
    "user": "postgres",
    "password": "postgres"
}

FUENTES = {
    "idartes": {
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_idartes.json",
        "tabla": "idartes_eventos"
    },
    "pablobon": {
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/refs/heads/main/scraping_teatropablotobon.json",
        "tabla": "teatropablobon_eventos"   
    },
    "plaza": {
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_teatroplasa.json",
        "tabla": "teatroplaza_eventos"
    }
}


def limpiar_json(texto: str):
    """Intenta limpiar texto para que sea JSON válido."""
    if texto.startswith("\ufeff"):
        texto = texto.lstrip("\ufeff")  # quitar BOM
    texto = texto.strip()

    if "][" in texto:  # caso típico de listas pegadas
        texto = texto.replace("][", ",")

    if not texto.startswith("["):  # buscar primer bloque de lista si hay basura
        match = re.search(r"\[.*\]", texto, re.DOTALL)
        if match:
            texto = match.group(0)

    return texto


def es_evento_valido(evento: dict, tabla: str) -> bool:
    """Reglas simples de validación antes de insertar en DB."""
    if tabla == "idartes_eventos":
        return bool(evento.get("nombre")) and bool(evento.get("fecha_inicio"))
    elif tabla == "teatropablobon_eventos":
        return bool(evento.get("nombre")) and bool(evento.get("fecha"))
    elif tabla == "teatroplaza_eventos":
        return bool(evento.get("nombre")) and bool(evento.get("fecha"))
    return True


# ----------------------------
# Nueva función: Resumen de datos
# ----------------------------
def generar_resumen(fuente, tabla, eventos, validos, invalidos):
    resumen = []
    resumen.append(f"📊 Resumen {fuente} → {tabla}")
    resumen.append(f"   Total eventos: {len(eventos)}")
    resumen.append(f"   Válidos: {validos}")
    resumen.append(f"   Inválidos: {invalidos}")
    if eventos:
        resumen.append(f"   Ejemplo: {json.dumps(eventos[0], ensure_ascii=False)}")
    return "\n".join(resumen)


def cargar_datos():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ Conexión establecida")

        log_lines = []
        log_lines.append(f"\n=== EJECUCIÓN {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

        for fuente, config in FUENTES.items():
            url = config["url"]
            tabla = config["tabla"]

            print(f"\n📥 Cargando {fuente} → {tabla}")
            r = requests.get(url, timeout=15)

            texto = limpiar_json(r.text)

            try:
                eventos = json.loads(texto)
            except Exception:
                try:
                    eventos = ast.literal_eval(texto)
                except Exception as e:
                    print(f"❌ Error leyendo JSON de {fuente}: {e}")
                    continue

            if not isinstance(eventos, list):
                eventos = [eventos]

            print(f"   → {len(eventos)} eventos encontrados")

            validos, invalidos = 0, 0

            for ev in eventos:
                if not es_evento_valido(ev, tabla):
                    invalidos += 1
                    continue

                validos += 1
                if tabla == "idartes_eventos":
                    cur.execute(f"""
                        INSERT INTO {tabla} (tipo, nombre, fecha_inicio, fecha_fin, hora, ingreso, raw)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (nombre, fecha_inicio, fecha_fin, hora) DO NOTHING;
                    """, (
                        ev.get("tipo", "N/A"),
                        ev.get("nombre", "N/A"),
                        ev.get("fecha_inicio", "N/A"),
                        ev.get("fecha_fin", None),
                        ev.get("hora", None),
                        ev.get("ingreso", "N/A"),
                        json.dumps(ev, ensure_ascii=False)
                    ))

                elif tabla == "teatropablobon_eventos":
                    fecha = ev.get("fecha", "N/A") or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (tipo, nombre, fecha, ingreso, raw)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("tipo", "N/A"),
                        ev.get("nombre", "N/A"),
                        fecha,
                        ev.get("ingreso", "N/A"),
                        json.dumps(ev, ensure_ascii=False)
                    ))

                elif tabla == "teatroplaza_eventos":
                    fecha = ev.get("fecha", "N/A") or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (nombre, fecha, raw)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("nombre", "N/A"),
                        fecha,
                        json.dumps(ev, ensure_ascii=False)
                    ))

            # Generar resumen y agregarlo al log
            resumen_fuente = generar_resumen(fuente, tabla, eventos, validos, invalidos)
            print(resumen_fuente)
            log_lines.append(resumen_fuente)

        conn.commit()
        print("\n🎉 Datos cargados correctamente.")

        cur.close()
        conn.close()

        # Guardar resumen en archivo log
        with open("resumen_extracciones.log", "a", encoding="utf-8") as f:
            f.write("\n".join(log_lines) + "\n")

    except Exception as e:
        print("❌ Error general:", e)


if __name__ == "__main__":
    cargar_datos()
