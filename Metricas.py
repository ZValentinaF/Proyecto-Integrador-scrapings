# metrics.py — HU-06 Dashboard de métricas (CLI)
import json
from datetime import datetime, timedelta
import psycopg2

DB_CONFIG = {
    "host": "awsaurorapg17-instance-1.cav2004g2f8p.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "dbname": "QueHayPaHacer",
    "user": "postgres",
    "password": "postgres"
}

LOG_PATH = "resumen_extracciones.log"

def leer_corridas(dias=7):
    ahora = datetime.now()
    desde = ahora - timedelta(days=dias)
    corridas = []
    try:
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # hay dos tipos de líneas: texto (resumen por fuente) y JSON (corrida)
                if line.startswith("{") and line.endswith("}"):
                    try:
                        obj = json.loads(line)
                        ts = datetime.strptime(obj["ts_start"], "%Y-%m-%d %H:%M:%S")
                        if ts >= desde:
                            corridas.append(obj)
                    except Exception:
                        pass
    except FileNotFoundError:
        pass
    return corridas

def contar_tablas():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM idartes_eventos;")
            idartes = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM teatropablobon_eventos;")
            pablobon = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM teatroplaza_eventos;")
            plaza = cur.fetchone()[0]
    return idartes, pablobon, plaza

def main():
    corridas = leer_corridas(dias=7)
    total = len(corridas)
    ok = sum(1 for c in corridas if c.get("status") == "OK")
    failed = total - ok
    if total > 0:
        avg = round(sum(c.get("duration_sec", 0) for c in corridas) / total, 2)
        last = max(corridas, key=lambda x: x["ts_start"])
        last_str = f'{last["ts_start"]} [{last["status"]}] {last["duration_sec"]}s'
    else:
        avg = 0.0
        last_str = "—"

    print("📊 Métricas últimos 7 días")
    print(f"- Corridas: {total} (OK: {ok}, FAILED: {failed})")
    print(f"- Duración promedio: {avg}s")
    print(f"- Última: {last_str}\n")

    idartes, pablobon, plaza = contar_tablas()
    print("📥 Registros en BD:")
    print(f"  - idartes_eventos: {idartes}")
    print(f"  - teatropablobon_eventos: {pablobon}")
    print(f"  - teatroplaza_eventos: {plaza}")

if __name__ == "__main__":
    main()
