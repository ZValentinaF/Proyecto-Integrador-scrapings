# cargar_datos.py — Local-first con fallback remoto, HU-07 / HU-10 / Etiquetas en raw
import os
import re
import json
import time
from pathlib import Path
from datetime import datetime

import requests
import psycopg2

# ===================== Configuración BD =====================
DB_CONFIG = {
    "host": "awsaurorapg17-instance-1.cav2004g2f8p.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "dbname": "QueHayPaHacer",
    "user": "postgres",
    "password": "postgres",
}

# ===================== Rutas / Fuentes ======================
BASE_DIR = Path(__file__).resolve().parent
FRESH_HOURS = 6  # umbral de "frescura" del JSON local

FUENTES = {
    "idartes": {
        "archivo": str(BASE_DIR / "scraping_idartes.json"),
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_idartes.json",
        "tabla": "idartes_eventos",
        "ciudad": "Bogotá",
    },
    "pablobon": {
        "archivo": str(BASE_DIR / "scraping_teatropablotobon.json"),
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_teatropablotobon.json",
        "tabla": "teatropablobon_eventos",
        "ciudad": "Medellín",
    },
    "plaza": {
        "archivo": str(BASE_DIR / "scraping_teatroplasa.json"),
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_teatroplasa.json",
        "tabla": "teatroplaza_eventos",
        "ciudad": "Cali",
    },
}

# ===================== Utilidades JSON ======================
def limpiar_json(texto: str) -> str:
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

def es_fresco(path: str, horas: int = FRESH_HOURS) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    edad_horas = (time.time() - p.stat().st_mtime) / 3600
    return edad_horas <= horas

def leer_eventos(cfg: dict):
    """
    Lee eventos de archivo local si existe y está 'fresco'.
    Si no, hace fallback a la URL remota y guarda copia local.
    """
    archivo = cfg.get("archivo")
    url = cfg.get("url")

    # 1) Local "fresco"
    if archivo and es_fresco(archivo):
        with open(archivo, "r", encoding="utf-8") as f:
            txt = limpiar_json(f.read())
        try:
            return json.loads(txt)
        except Exception:
            import ast
            return ast.literal_eval(txt)

    # 2) Fallback remoto (y guarda copia local)
    if url:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        if archivo:
            try:
                with open(archivo, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except Exception:
                # si no se puede guardar, igual retornamos los datos
                pass
        return data

    raise FileNotFoundError("No hay archivo local fresco ni URL de respaldo definida.")

# ============== HU-07 — Validación mínima ===================
def obtener_fecha_inicio(ev: dict) -> str:
    """
    Devuelve la fecha 'clave':
    - Si existe 'fecha_inicio' válida, úsala.
    - Si no, usa 'fecha' (fuentes de fecha simple).
    """
    fi = ev.get("fecha_inicio")
    if fi and fi != "N/A":
        return fi
    f = ev.get("fecha")
    if f and f != "N/A":
        return f
    return "N/A"

def es_valido(ev: dict) -> bool:
    """
    Reglas mínimas:
    - nombre no vacío
    - alguna fecha válida (fecha_inicio o fecha)
    """
    nombre = ev.get("nombre")
    if not nombre or nombre in ("N/A", "", None):
        return False
    fi = obtener_fecha_inicio(ev)
    return fi not in ("N/A", "", None)

# ===== Etiquetado automático (categoría / ingreso / ciudad) =====
def inferir_categoria(ev: dict) -> str:
    txt = " ".join([str(ev.get("tipo", "")), str(ev.get("nombre", ""))]).lower()
    if any(k in txt for k in ("música", "musica", "concierto", "banda", "orquesta")):
        return "MÚSICA"
    if "teatro" in txt or "obra" in txt:
        return "TEATRO"
    if "danza" in txt or "ballet" in txt:
        return "DANZA"
    if "comedia" in txt or "stand up" in txt or "stand-up" in txt:
        return "COMEDIA"
    return "OTROS"

def inferir_ingreso(ev: dict) -> str:
    ingreso_raw = str(ev.get("ingreso", "")).lower()
    if "libre" in ingreso_raw:
        return "LIBRE"
    if "inscrip" in ingreso_raw:
        return "INSCRIPCION"
    if "costo" in ingreso_raw or "$" in ingreso_raw or "bole" in ingreso_raw:
        return "COSTO"
    return "OTRO"

def enriquecer_raw_con_tags(ev: dict, ciudad: str) -> dict:
    """
    Agrega 'tags' al raw con categoría, ingreso y ciudad (no cambia el esquema).
    """
    raw_obj = dict(ev)
    tags = set(raw_obj.get("tags", []))
    tags.add(inferir_categoria(ev))
    tags.add(inferir_ingreso(ev))
    if ciudad:
        tags.add(ciudad)
    raw_obj["tags"] = sorted(tags)
    return raw_obj

# ===================== Carga principal ======================
def cargar_datos():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("Conexion establecida")

        resumen_log = []
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for fuente, cfg in FUENTES.items():
            tabla = cfg["tabla"]
            ciudad = cfg.get("ciudad", "")

            print(f"\nCargando {fuente} -> {tabla}")
            try:
                eventos = leer_eventos(cfg)
            except Exception as e:
                print(f"  Error leyendo datos de la fuente '{fuente}': {e}")
                continue

            if not isinstance(eventos, list):
                eventos = [eventos]

            # HU-07 — Validación
            validos, invalidos = [], []
            for ev in eventos:
                if es_valido(ev):
                    validos.append(ev)
                else:
                    invalidos.append(ev)

            print(f"   -> {len(eventos)} eventos encontrados")
            print(f"   Validos: {len(validos)}")
            print(f"   Invalidos: {len(invalidos)}")
            if invalidos:
                print("   Ejemplos de invalidos:")
                for ejemplo in invalidos[:3]:
                    print(f"   - {ejemplo}")

            # HU-10 — Resumen por fuente (para log)
            resumen_log.append(f"[{timestamp}] {fuente}: {len(validos)} validos / {len(invalidos)} invalidos")

            # Inserción por tabla (sin cambiar esquema)
            for ev in validos:
                raw_obj = enriquecer_raw_con_tags(ev, ciudad)
                raw_json = json.dumps(raw_obj, ensure_ascii=False)

                if tabla == "idartes_eventos":
                    # columnas: (tipo, nombre, fecha_inicio, fecha_fin, ingreso, raw)
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
                        inferir_ingreso(ev),
                        raw_json,
                    ))

                elif tabla == "teatropablobon_eventos":
                    # columnas: (tipo, nombre, fecha, ingreso, raw)
                    fecha = ev.get("fecha") or obtener_fecha_inicio(ev) or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (tipo, nombre, fecha, ingreso, raw)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("tipo", "N/A"),
                        ev.get("nombre", "N/A"),
                        fecha,
                        inferir_ingreso(ev),
                        raw_json,
                    ))

                elif tabla == "teatroplaza_eventos":
                    # columnas: (nombre, fecha, raw)
                    fecha = ev.get("fecha") or obtener_fecha_inicio(ev) or "N/A"
                    cur.execute(f"""
                        INSERT INTO {tabla} (nombre, fecha, raw)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (nombre, fecha) DO NOTHING;
                    """, (
                        ev.get("nombre", "N/A"),
                        fecha,
                        raw_json,
                    ))

            conn.commit()

        print("\nDatos cargados correctamente.")

        # HU-10 — Guardar resumen en log local del proyecto
        log_path = BASE_DIR / "resumen_extracciones.log"
        try:
            with open(log_path, "a", encoding="utf-8") as logf:
                logf.write("\n".join(resumen_log) + "\n")
        except Exception:
            # no interrumpas la carga si falla el log
            pass

        cur.close()
        conn.close()

    except Exception as e:
        print("Error general al cargar datos:", e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    cargar_datos()
