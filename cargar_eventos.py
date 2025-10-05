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

# HU-08 (Sprint 3): Ciudad por fuente para etiquetado automático
CIUDAD_POR_FUENTE = {
    "idartes": "Bogotá",
    "pablobon": "Medellín",
    "plaza": "Cali"
}

# HU-06 (Sprint 3): ruta del log de corridas
LOG_PATH = "resumen_extracciones.log"

def limpiar_json(texto: str):
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

def obtener_fecha_inicio(ev: dict) -> str:
    fi = ev.get("fecha_inicio")
    if fi and fi != "N/A":
        return fi
    f = ev.get("fecha")
    if f and f != "N/A":
        return f
    return "N/A"

def es_valido(ev: dict) -> bool:
    """
    (Sprint 2 – HU-07, referencial)
    Control de calidad básico: requiere nombre y al menos una fecha.
    """
    nombre = ev.get("nombre")
    if not nombre or nombre in ("N/A", "", None):
        return False
    fi = obtener_fecha_inicio(ev)
    return fi not in ("N/A", "", None)

# HU-08 (Sprint 3): Etiquetado automático (categoría, ingreso normalizado y ciudad)
def inferir_etiquetas(ev: dict, ciudad: str):
    nombre = (ev.get("nombre") or "").lower()
    tipo   = (ev.get("tipo") or "").lower()
    ingreso_raw = (ev.get("ingreso") or "").lower()
    texto = f"{nombre} {tipo}"

    # Categoría por palabras clave
    if any(k in texto for k in ["música", "musica", "concierto", "banda", "festival"]):
        cat = "MÚSICA"
    elif any(k in texto for k in ["teatro", "obra", "dramaturgia"]):
        cat = "TEATRO"
    elif any(k in texto for k in ["danza", "baile", "ballet"]):
        cat = "DANZA"
    elif any(k in texto for k in ["comedia", "stand up", "standup", "humor"]):
        cat = "COMEDIA"
    else:
        cat = "OTROS"

    # Ingreso normalizado
    if "libre" in ingreso_raw:
        ing = "LIBRE"
    elif "costo" in ingreso_raw or "$" in ingreso_raw or "boleta" in ingreso_raw:
        ing = "COSTO"
    elif "inscrip" in ingreso_raw:
        ing = "INSCRIPCION"
    else:
        ing = "OTRO"

    return {"categoria": cat, "ingreso_norm": ing, "ciudad": ciudad}

def asegurar_indices_unicos(cur):
    """
    Asegura los índices únicos que usa ON CONFLICT (idempotencia).
    (Sigue siendo Sprint 2/infra; lo dejamos comentado para claridad)
    """
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_idartes_eventos_nfi
        ON idartes_eventos (nombre, fecha_inicio, fecha_fin);
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_teatropablobon_eventos_nf
        ON teatropablobon_eventos (nombre, fecha);
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_teatroplaza_eventos_nf
        ON teatroplaza_eventos (nombre, fecha);
    """)

def cargar_datos():
    # HU-06 (Sprint 3): inicio de corrida (para medir duración/estado)
    ts_start = datetime.now()
    status = "OK"

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("✅ Conexión establecida")

        asegurar_indices_unicos(cur)
        conn.commit()

        resumen_log = []  # (Sprint 2 – HU-10) Resumen por fuente; lo mantenemos
        timestamp = ts_start.strftime("%Y-%m-%d %H:%M:%S")

        for fuente, config in FUENTES.items():
            url = config["url"]
            tabla = config["tabla"]
            ciudad = CIUDAD_POR_FUENTE.get(fuente, "N/A")  # HU-08: ciudad para tags
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
                    resumen_log.append(f"[{timestamp}] {fuente}: 0 válidos / 0 inválidos (error: {e})")
                    continue

            if not isinstance(eventos, list):
                eventos = [eventos]

            # (Sprint 2 – HU-07) Validación básica
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

            # (Sprint 2 – HU-10) Resumen por fuente
            resumen_log.append(
                f"[{timestamp}] {fuente}: {len(validos)} válidos / {len(invalidos)} inválidos"
            )

            # Inserción con HU-08 (Sprint 3): tags en raw (incluye ciudad)
            for ev in validos:
                fecha_inicio = obtener_fecha_inicio(ev)
                raw_ev = dict(ev)
                raw_ev["tags"] = inferir_etiquetas(ev, ciudad)  # HU-08 (Sprint 3)

                if tabla == "idartes_eventos":
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
                        json.dumps(raw_ev, ensure_ascii=False)
                    ))

                elif tabla == "teatropablobon_eventos":
                    fecha = ev.get("fecha") or fecha_inicio or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (tipo, nombre, fecha, ingreso, raw)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("tipo", "N/A"),
                        ev.get("nombre", "N/A"),
                        fecha,
                        ev.get("ingreso", "N/A"),
                        json.dumps(raw_ev, ensure_ascii=False)
                    ))

                elif tabla == "teatroplaza_eventos":
                    fecha = ev.get("fecha") or fecha_inicio or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (nombre, fecha, raw)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("nombre", "N/A"),
                        fecha,
                        json.dumps(raw_ev, ensure_ascii=False)
                    ))

            conn.commit()

        print("\n🎉 Datos cargados correctamente.")

        # (Sprint 2 – HU-10) Guardar resumen por fuente
        with open("resumen_extracciones.log", "a", encoding="utf-8") as logf:
            logf.write("\n".join(resumen_log) + "\n")

    except Exception as e:
        status = "FAILED"
        print("❌ Error general:", e)

    finally:
        # HU-06 (Sprint 3): registrar corrida (duración/estado) para métricas
        ts_end = datetime.now()
        duration = (ts_end - ts_start).total_seconds()
        entry = {
            "ts_start": ts_start.strftime("%Y-%m-%d %H:%M:%S"),
            "ts_end": ts_end.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": round(duration, 2),
            "status": status
        }
        try:
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            print("⚠️ No se pudo escribir el log de métricas:", e)

        try:
            cur.close(); conn.close()
        except:
            pass

if __name__ == "__main__":
    cargar_datos()
