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

def obtener_fecha_inicio(ev: dict) -> str:
    """Devuelve fecha_inicio si existe; si no, usa fecha (para fuentes con fecha simple)."""
    fi = ev.get("fecha_inicio")
    if fi and fi != "N/A":
        return fi
    f = ev.get("fecha")
    if f and f != "N/A":
        return f
    return "N/A"

def es_valido(ev: dict) -> bool:
    """
    HU-07: Control de calidad.
    Requiere nombre válido y al menos una fecha (fecha_inicio o fecha).
    """
    nombre = ev.get("nombre")
    if not nombre or nombre in ("N/A", "", None):
        return False
    fi = obtener_fecha_inicio(ev)
    return fi not in ("N/A", "", None)

def cargar_datos():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ Conexión establecida")

        resumen_log = []
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for fuente, config in FUENTES.items():
            url = config["url"]
            tabla = config["tabla"]
            print(f"\n📥 Descargando {fuente} desde {url}")

            r = requests.get(url, timeout=20)
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

            # Validación HU-07
            validos, invalidos = [], []
            for ev in eventos:
                if es_valido(ev):
                    validos.append(ev)
                else:
                    invalidos.append(ev)

            print(f"   → {len(eventos)} eventos encontrados")
            print(f"   ✅ Válidos: {len(validos)}")
            print(f"   ❌ Inválidos: {len(invalidos)}")
            if invalidos:
                print("   Ejemplos de inválidos:")
                for ejemplo in invalidos[:3]:
                    print(f"   - {ejemplo}")

            # HU-10: resumen por fuente
            resumen_log.append(
                f"[{timestamp}] {fuente}: {len(validos)} válidos / {len(invalidos)} inválidos"
            )

            # Inserción
            for ev in validos:
                if tabla == "idartes_eventos":
                    fecha_inicio = obtener_fecha_inicio(ev)
                    cur.execute(f"""
                        INSERT INTO {tabla} (tipo, nombre, fecha_inicio, fecha_fin, ingreso, raw)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (nombre, fecha_inicio, fecha_fin) DO NOTHING;
                    """, (
                        ev.get("tipo", "N/A"),
                        ev.get("nombre", "N/A"),
                        fecha_inicio,
                        ev.get("fecha_fin", None),
                        ev.get("ingreso", "N/A"),
                        json.dumps(ev, ensure_ascii=False)
                    ))

                elif tabla == "teatropablobon_eventos":
                    fecha = ev.get("fecha") or obtener_fecha_inicio(ev) or "N/A"
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
                    fecha = ev.get("fecha") or obtener_fecha_inicio(ev) or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (nombre, fecha, raw)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("nombre", "N/A"),
                        fecha,
                        json.dumps(ev, ensure_ascii=False)
                    ))

            conn.commit()

        print("\n🎉 Datos cargados correctamente.")

        # HU-10: guardar resumen en log
        with open("resumen_extracciones.log", "a", encoding="utf-8") as logf:
            logf.write("\n".join(resumen_log) + "\n")

        cur.close()
        conn.close()

    except Exception as e:
        print("❌ Error general:", e)

if __name__ == "__main__":
    cargar_datos()
