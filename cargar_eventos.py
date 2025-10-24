# cargar_eventos.py — Inserta todas las fuentes en la tabla 'evento' (DB: QueHayPaHacer)
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
FRESH_HOURS = 6  # horas de "frescura" del JSON local

FUENTES = {
    "idartes": {
        "archivo": str(BASE_DIR / "scraping_idartes.json"),
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_idartes.json",
        "ciudad": "Bogotá",
    },
    "pablobon": {
        "archivo": str(BASE_DIR / "scraping_teatropablotobon.json"),
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_teatropablotobon.json",
        "ciudad": "Medellín",
    },
    "plaza": {
        "archivo": str(BASE_DIR / "scraping_teatroplasa.json"),
        "url": "https://raw.githubusercontent.com/ZValentinaF/Proyecto-Integrador-scrapings/main/scraping_teatroplasa.json",
        "ciudad": "Cali",
    },
}

# ===================== Utilidades JSON / lectura =====================
def limpiar_json(texto: str) -> str:
    if texto.startswith("\ufeff"):
        texto = texto.lstrip("\ufeff")
    texto = texto.strip()
    if "][" in texto:
        texto = texto.replace("][", ",")
    if not texto.startswith("["):
        import re as _re
        m = _re.search(r"\[.*\]", texto, _re.DOTALL)
        if m:
            texto = m.group(0)
    return texto

def es_fresco(path: str, horas: int = FRESH_HOURS) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    edad_horas = (time.time() - p.stat().st_mtime) / 3600
    return edad_horas <= horas

def leer_eventos(cfg: dict):
    archivo = cfg.get("archivo")
    url = cfg.get("url")

    if archivo and es_fresco(archivo):
        with open(archivo, "r", encoding="utf-8") as f:
            txt = limpiar_json(f.read())
        try:
            return json.loads(txt)
        except Exception:
            import ast
            return ast.literal_eval(txt)

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

    raise FileNotFoundError("No hay archivo local fresco ni URL definida.")

# ===================== Validación / normalización =====================
def obtener_fecha_inicio(ev: dict):
    """Devuelve 'YYYY-MM-DD' si está; si no, None. No fuerza parseos raros."""
    fi = ev.get("fecha_inicio")
    if fi and fi != "N/A":
        return fi
    f = ev.get("fecha")
    if f and f != "N/A":
        return f
    return None

def es_valido(ev: dict) -> bool:
    return bool(ev.get("nombre")) and obtener_fecha_inicio(ev) is not None

def inferir_es_gratuito_y_precio(ev: dict):
    ingreso_raw = str(ev.get("ingreso", "")).lower()
    if "libre" in ingreso_raw or "gratuito" in ingreso_raw:
        return True, 0.0
    if any(x in ingreso_raw for x in ["costo", "$", "bole", "entrada"]):
        return False, None
    return False, None

def slugify(texto: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (texto or "").lower()).strip("-")
    return s[:60] if s else None

# ===================== ENUM: estadoeventoenum =====================
def obtener_estado_valido(conn) -> str:
    """
    Lee las etiquetas válidas del ENUM estadoeventoenum y retorna la mejor opción:
    - Prefiere 'ACTIVO' si existe,
    - si no, 'PUBLICADO' si existe,
    - si no, la primera etiqueta del ENUM.
    """
    with conn.cursor() as c:
        c.execute("""
            SELECT e.enumlabel
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE t.typname = 'estadoeventoenum'
            ORDER BY e.enumsortorder;
        """)
        rows = [r[0] for r in c.fetchall()]
    if not rows:
        # Fallback duro (no debería pasar si la columna es ENUM correcto)
        return 'ACTIVO'
    prefs = ["ACTIVO", "PUBLICADO"]
    for p in prefs:
        if p in rows:
            return p
    return rows[0]  # primer valor del enum

# ===================== Carga principal =====================
def cargar_datos():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print(f"✅ Conexión establecida con la base '{DB_CONFIG['dbname']}'")

        estado_enum_seguro = obtener_estado_valido(conn)

        for fuente, cfg in FUENTES.items():
            print(f"\n📥 Cargando {fuente} → evento")
            try:
                eventos = leer_eventos(cfg)
            except Exception as e:
                print(f"   ❌ Error leyendo datos: {e}")
                continue

            if not isinstance(eventos, list):
                eventos = [eventos]

            validos = [ev for ev in eventos if es_valido(ev)]
            invalidos = [ev for ev in eventos if not es_valido(ev)]

            print(f"   → {len(eventos)} eventos encontrados")
            print(f"   ✅ Válidos: {len(validos)}")
            print(f"   ❌ Inválidos: {len(invalidos)}")
            if invalidos:
                print("   Ejemplos de inválidos:")
                for ejemplo in invalidos[:3]:
                    print(f"   - {ejemplo}")

            for ev in validos:
                titulo = ev.get("nombre", "Evento sin título")
                descripcion = ev.get("tipo", "Sin descripción")
                estado = estado_enum_seguro
                imagen_url = None
                url_oficial = ev.get("url", None)
                es_gratuito, precio_desde = inferir_es_gratuito_y_precio(ev)
                moneda = "COP"
                slug = slugify(titulo)
                fecha_pub = obtener_fecha_inicio(ev)  # se envía como string 'YYYY-MM-DD'

                # Evitar duplicados sin requerir UNIQUE: usa (titulo, fecha_publicacion)
                cur.execute("""
                    INSERT INTO evento 
                    (titulo, descripcion, estado, imagen_url, organizador_id, lugar_id,
                     url_oficial, es_gratuito, precio_desde, moneda, slug, fecha_publicacion)
                    SELECT %s, %s, %s, %s, NULL, NULL, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM evento e
                        WHERE e.titulo = %s
                          AND e.fecha_publicacion = %s
                    );
                """, (
                    titulo, descripcion, estado, imagen_url,
                    url_oficial, es_gratuito, precio_desde, moneda, slug, fecha_pub,
                    titulo, fecha_pub
                ))

            conn.commit()

        print("\n🎉 Datos cargados correctamente en 'evento'.")
        cur.close()
        conn.close()

    except Exception as e:
        print("❌ Error general al cargar datos:", e)
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
