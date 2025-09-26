import psycopg2
import json
import re
import os
from datetime import datetime

# ----------------------------
# Configuración de rutas
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_CONFIG = {
    "host": "awsaurorapg17-instance-1.cav2004g2f8p.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "dbname": "QueHayPaHacer",
    "user": "postgres",
    "password": "postgres"
}

FUENTES = {
    "idartes": {
        "archivo": os.path.join(BASE_DIR, "scraping_idartes.json"),
        "tabla": "idartes_eventos"
    },
    "pablobon": {
        "archivo": os.path.join(BASE_DIR, "scraping_teatropablotobon.json"),
        "tabla": "teatropablobon_eventos"   
    },
    "plaza": {
        "archivo": os.path.join(BASE_DIR, "scraping_teatroplasa.json"),
        "tabla": "teatroplaza_eventos"
    }
}

def limpiar_json(texto: str):
    """Intenta limpiar texto para que sea JSON válido."""
    if texto.startswith("\ufeff"):
        texto = texto.lstrip("\ufeff")
    texto = texto.strip()

    if "][" in texto:
        texto = texto.replace("][", ",")

    if not texto.startswith("["):
        match = re.search(r"\[.*\]", texto, re.DOTALL)
        if match:
            texto = match.group(0)

    return texto

def es_valido(ev, tabla):
    """HU-07: reglas de validación de eventos"""
    if not ev.get("nombre") or ev.get("nombre") in ("N/A", "", None):
        return False
    if tabla == "idartes_eventos":
        return bool(ev.get("fecha_inicio") and ev.get("fecha_inicio") != "N/A")
    else:
        return bool(ev.get("fecha") and ev.get("fecha") != "N/A")

def cargar_datos():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ Conexión establecida")

        resumen_log = []
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for fuente, config in FUENTES.items():
            archivo = config["archivo"]
            tabla = config["tabla"]

            print(f"\n📥 Cargando {fuente} → {tabla}")

            try:
                with open(archivo, "r", encoding="utf-8") as f:
                    texto = limpiar_json(f.read())
                eventos = json.loads(texto)
            except Exception as e:
                print(f"❌ Error leyendo archivo {archivo}: {e}")
                continue

            if not isinstance(eventos, list):
                eventos = [eventos]

            validos, invalidos = [], []
            for ev in eventos:
                if es_valido(ev, tabla):
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

            # Guardar en log
            resumen_log.append(
                f"[{timestamp}] {fuente}: {len(validos)} válidos / {len(invalidos)} inválidos"
            )

            # Insertar solo válidos
            for ev in validos:
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

        conn.commit()
        print("\n🎉 Datos cargados correctamente.")

        # HU-10: escribir resumen en log
        log_path = os.path.join(BASE_DIR, "resumen_extracciones.log")
        with open(log_path, "a", encoding="utf-8") as logf:
            logf.write("\n".join(resumen_log) + "\n")

        cur.close()
        conn.close()

    except Exception as e:
        print("❌ Error general:", e)

if __name__ == "__main__":
    cargar_datos()
