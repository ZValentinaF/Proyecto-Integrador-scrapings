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
    "dbname": "QueHayPaHacer",  # <-- si estás probando en 'Eventos', cámbialo aquí
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
                pass
        return data

    raise FileNotFoundError("No hay archivo local fresco ni URL de respaldo definida.")

# ============== HU-07 — Validación mínima ===================
def obtener_fecha_inicio(ev: dict) -> str:
    fi = ev.get("fecha_inicio")
    if fi and fi != "N/A":
        return fi
    f = ev.get("fecha")
    if f and f != "N/A":
        return f
    return "N/A"

def es_valido(ev: dict) -> bool:
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
    raw_obj = dict(ev)
    tags = set(raw_obj.get("tags", []))
    tags.add(inferir_categoria(ev))
    tags.add(inferir_ingreso(ev))
    if ciudad:
        tags.add(ciudad)
    raw_obj["tags"] = sorted(tags)
    return raw_obj

# ======= Detección de UNIQUE para decidir estrategia de inserción =======
def tiene_unique(conn, tabla: str, columnas: tuple[str, ...]) -> bool:
    """
    Verifica si existe un constraint UNIQUE exactamente sobre 'columnas' en 'tabla'.
    """
    cols = list(columnas)
    with conn.cursor() as c:
        c.execute("""
            SELECT array_agg(a.attname ORDER BY a.attnum)
            FROM pg_constraint ct
            JOIN pg_class cl ON cl.oid = ct.conrelid
            JOIN pg_namespace ns ON ns.oid = cl.relnamespace
            JOIN unnest(ct.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum = k.attnum
            WHERE ct.contype = 'u'
              AND cl.relname = %s
            GROUP BY ct.oid
        """, (tabla,))
        for (arr,) in c.fetchall():
            if arr == cols:
                return True
    return False

# ===================== Carga principal ======================
def cargar_datos():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("Conexion establecida")

        # detectar si tenemos UNIQUE por tabla
        unique_map = {
            "idartes_eventos": tiene_unique(conn, "idartes_eventos", ("nombre", "fecha_inicio", "fecha_fin")),
            "teatropablobon_eventos": tiene_unique(conn, "teatropablobon_eventos", ("nombre", "fecha")),
            "teatroplaza_eventos": tiene_unique(conn, "teatroplaza_eventos", ("nombre", "fecha")),
        }

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

            resumen_log.append(f"[{timestamp}] {fuente}: {len(validos)} validos / {len(invalidos)} invalidos")

            # Inserción por tabla (usa ON CONFLICT si hay UNIQUE; si no, fallback a WHERE NOT EXISTS)
            use_on_conflict = unique_map.get(tabla, False)

            for ev in validos:
                raw_obj = enriquecer_raw_con_tags(ev, ciudad)
                raw_json = json.dumps(raw_obj, ensure_ascii=False)

                if tabla == "idartes_eventos":
                    fecha_inicio = obtener_fecha_inicio(ev)
                    if use_on_conflict:
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
                    else:
                        cur.execute(f"""
                            INSERT INTO {tabla} (tipo, nombre, fecha_inicio, fecha_fin, ingreso, raw)
                            SELECT %s, %s, %s, %s, %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {tabla} i
                                WHERE i.nombre = %s
                                  AND i.fecha_inicio = %s
                                  AND i.fecha_fin IS NOT DISTINCT FROM %s
                            );
                        """, (
                            ev.get("tipo", "N/A"),
                            ev.get("nombre", "N/A"),
                            fecha_inicio,
                            ev.get("fecha_fin", None),
                            inferir_ingreso(ev),
                            raw_json,
                            ev.get("nombre", "N/A"),
                            fecha_inicio,
                            ev.get("fecha_fin", None),
                        ))

                elif tabla == "teatropablobon_eventos":
                    fecha = ev.get("fecha") or obtener_fecha_inicio(ev) or "N/A"
                    if use_on_conflict:
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
                    else:
                        cur.execute(f"""
                            INSERT INTO {tabla} (tipo, nombre, fecha, ingreso, raw)
                            SELECT %s, %s, %s, %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {tabla} t
                                WHERE t.nombre = %s AND t.fecha = %s
                            );
                        """, (
                            ev.get("tipo", "N/A"),
                            ev.get("nombre", "N/A"),
                            fecha,
                            inferir_ingreso(ev),
                            raw_json,
                            ev.get("nombre", "N/A"),
                            fecha,
                        ))

                elif tabla == "teatroplaza_eventos":
                    fecha = ev.get("fecha") or obtener_fecha_inicio(ev) or "N/A"
                    if use_on_conflict:
                        cur.execute(f"""
                            INSERT INTO {tabla} (nombre, fecha, raw)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (nombre, fecha) DO NOTHING;
                        """, (
                            ev.get("nombre", "N/A"),
                            fecha,
                            raw_json,
                        ))
                    else:
                        cur.execute(f"""
                            INSERT INTO {tabla} (nombre, fecha, raw)
                            SELECT %s, %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {tabla} p
                                WHERE p.nombre = %s AND p.fecha = %s
                            );
                        """, (
                            ev.get("nombre", "N/A"),
                            fecha,
                            raw_json,
                            ev.get("nombre", "N/A"),
                            fecha,
                        ))

            conn.commit()

        print("\nDatos cargados correctamente.")

        # HU-10 — Guardar resumen en log local del proyecto
        log_path = BASE_DIR / "resumen_extracciones.log"
        try:
            with open(log_path, "a", encoding="utf-8") as logf:
                logf.write("\n".join(resumen_log) + "\n")
        except Exception:
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
