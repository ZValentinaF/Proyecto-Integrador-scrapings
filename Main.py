try:
    import print_safe 
except Exception:
    pass

import argparse
import subprocess
import sys
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Optional

SCRAPERS: List[str] = [
    "scraping_idartes.py",
    "scraping_teatropablotobon.py",
    "scraping_teatroplasa.py",
]
LOADER: str = "cargar_eventos.py"


def resolve_python(python_bin: Optional[str]) -> str:
    return python_bin or sys.executable


def ensure_files_exist(files: List[str], base: Path) -> None:
    missing = [f for f in files if not (base / f).exists()]
    if missing:
        detail = "\n  - ".join(missing)
        raise FileNotFoundError(
            f"No se encontraron estos archivos en {base}:\n  - {detail}\n"
            f"Verifica rutas/nombres o usa --cwd para fijar el directorio de trabajo."
        )


def run_cmd(pyfile: str, python_bin: str, workdir: Path, show_cmds: bool = False) -> Tuple[str, int, float, str]:
    start = time.time()
    cmd = [python_bin, str(workdir / pyfile)]
    if show_cmds:
        print(f"CMD> {' '.join(cmd)}")

    # Fuerza UTF-8 en subprocesos para evitar UnicodeEncodeError en Windows
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    proc = subprocess.Popen(
        cmd,
        cwd=str(workdir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    out, err = proc.communicate()
    dur = round(time.time() - start, 2)

    if out:
        print(out, end="")

    return (pyfile, proc.returncode, dur, err)


def run_scrapers(parallel: bool, max_workers: int, stop_on_fail: bool,
                 python_bin: str, workdir: Path, show_cmds: bool) -> Tuple[float, int]:
    print("Lanzando scrapings " + ("en paralelo" if parallel else "en secuencia"))
    t0 = time.time()
    failures = 0

    if parallel:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(run_cmd, s, python_bin, workdir, show_cmds): s for s in SCRAPERS}
            for f in as_completed(futures):
                name, code, dur, err = f.result()
                tag = Path(name).stem
                if code == 0:
                    print(f"[OK] {tag} ({dur}s)")
                else:
                    failures += 1
                    print(f"[FAIL] {tag} ({dur}s)")
                    if err:
                        print(err)
                    if stop_on_fail:
                        for other in futures:
                            other.cancel()
                        break
    else:
        for s in SCRAPERS:
            name, code, dur, err = run_cmd(s, python_bin, workdir, show_cmds)
            tag = Path(name).stem
            if code == 0:
                print(f"[OK] {tag} ({dur}s)")
            else:
                failures += 1
                print(f"[FAIL] {tag} ({dur}s)")
                if err:
                    print(err)
                if stop_on_fail:
                    break

    total = round(time.time() - t0, 2)
    print(f"Tiempo total scrapings: {total}s\n")
    return total, failures


def run_loader(python_bin: str, workdir: Path, show_cmds: bool) -> int:
    print("Cargando datos a BD...")
    name, code, dur, err = run_cmd(LOADER, python_bin, workdir, show_cmds)
    tag = Path(name).stem
    if code == 0:
        print(f"[OK] {tag} ({dur}s)")
    else:
        print(f"[FAIL] {tag} ({dur}s)")
        if err:
            print(err)
    return code


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Pipeline scraping → carga BD (HU-09). Orquesta scrapers y cargador con opción de paralelo.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--parallel", action="store_true", help="Ejecuta los scrapings en paralelo")
    ap.add_argument("--max-workers", type=int, default=3, help="Hilos para --parallel")
    ap.add_argument("--skip-load", action="store_true", help="Ejecuta solo scrapers; omite carga a BD")
    ap.add_argument("--stop-on-scraper-fail", action="store_true", help="Detiene si falla un scraper")
    ap.add_argument("--cwd", type=str, default=".", help="Directorio base del pipeline")
    ap.add_argument("--python", type=str, default=None, help="Ruta del intérprete Python")
    ap.add_argument("--show-cmds", action="store_true", help="Imprime los comandos ejecutados")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    workdir = Path(args.cwd).resolve()
    python_bin = resolve_python(args.python)

    ensure_files_exist(SCRAPERS + [LOADER], workdir)

    total_time, failures = run_scrapers(
        parallel=args.parallel,
        max_workers=args.max_workers,
        stop_on_fail=args.stop_on_scraper_fail,
        python_bin=python_bin,
        workdir=workdir,
        show_cmds=args.show_cmds,
    )

    if failures > 0 and args.stop_on_scraper_fail:
        print(f"Se detectaron {failures} scraper(s) con error. Pipeline detenido antes de la carga.")
        sys.exit(2)

    if not args.skip_load:
        rc = run_loader(python_bin, workdir, args.show_cmds)
        if rc != 0:
            sys.exit(rc)
    else:
        print("Carga a BD omitida por --skip-load")

    print("Pipeline finalizado correctamente.")


if __name__ == "__main__":
    main()
