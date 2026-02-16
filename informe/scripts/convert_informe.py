import pandas as pd
from pathlib import Path
import json
import re
import unicodedata
import numpy as np
from datetime import datetime
import csv as csvmod

INPUT_DIR = Path("informe/input")
OUTPUT_DIR = Path("informe/data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_NAME = "informe_ganaderia"

def normalize_key(s: str) -> str:
    s = (s or "").strip().upper()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s

def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:80] or "sheet"

def fmt_date(v):
    # Formato dd/mm/yyyy (para que el dashboard detecte años/fechas bien)
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""

    if isinstance(v, (pd.Timestamp, datetime)):
        d = v.date()
        return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"

    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""

    # "YYYY-MM-DD 00:00:00" -> "DD/MM/YYYY"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{d:02d}/{mo:02d}/{y:04d}"

    return s

def read_sheets(path: Path):
    suf = path.suffix.lower()
    if suf == ".xls":
        return pd.read_excel(path, sheet_name=None, header=None, engine="xlrd")
    return pd.read_excel(path, sheet_name=None, header=None, engine="openpyxl")

def canonical_filename(sheet_name: str) -> str:
    # Esto ayuda a que tu dashboard “detecte” módulos por nombre
    k = normalize_key(sheet_name)
    if "INVENTARIO" in k and "CATEGOR" in k:
        return "Inventario X Categoria.csv"
    if "FLUJO" in k and "CATEGOR" in k:
        return "Flujo de Ganado x Categoria.csv"
    if ("ENTRADA" in k and "SALIDA" in k) or ("ENTRADAS" in k and "SALIDAS" in k) or ("MOVIMIENTO" in k):
        return "Inventario Entradas y Salidas.csv"
    if "NACIM" in k:
        return "Nacimientos 2026.csv"
    if "MUERT" in k:
        return "Muertes 2026.csv"
    if "DESTETE" in k:
        return "Destete de Terneros.csv"
    if ("PROYEC" in k or "PROYECION" in k) and "PART" in k:
        return "Proyeccion de Partos.csv"
    if "VENTAS" in k:
        return "Ventas 2026.csv"
    # fallback
    return f"{sheet_name}.csv"

# Buscar el Excel (fijo)
xlsx = INPUT_DIR / f"{BASE_NAME}.xlsx"
xls  = INPUT_DIR / f"{BASE_NAME}.xls"
if xlsx.exists():
    src = xlsx
elif xls.exists():
    src = xls
else:
    raise SystemExit(f"No encontré {BASE_NAME}.xlsx ni {BASE_NAME}.xls en {INPUT_DIR}")

sheets = read_sheets(src)

manifest = []
seen_slugs = set()

for sheet_name, raw in sheets.items():
    if raw is None or raw.empty:
        continue

    df = raw.copy()
    df = df.fillna("")

    # Normalizar fechas (sin tocar números normales)
    df = df.applymap(fmt_date)

    slug = slugify(sheet_name)
    base_slug = slug
    i = 2
    while slug in seen_slugs:
        slug = f"{base_slug}_{i}"
        i += 1
    seen_slugs.add(slug)

    out_csv = OUTPUT_DIR / f"{slug}.csv"
    df.to_csv(
        out_csv,
        index=False,
        header=False,   # lo dejamos “crudo” tipo reporte (tu dashboard lo procesa así)
        sep=";",
        encoding="utf-8",
        quoting=csvmod.QUOTE_MINIMAL
    )

    manifest.append({
        "sheet": sheet_name,
        "slug": slug,
        "csv": f"data/{slug}.csv",                 # ruta relativa a informe/index.html
        "fileName": canonical_filename(sheet_name) # nombre “para detección”
    })

with open(OUTPUT_DIR / "sheets.json", "w", encoding="utf-8") as fp:
    json.dump(manifest, fp, ensure_ascii=False, indent=2)

print(f"OK -> {len(manifest)} hojas exportadas en informe/data/ + sheets.json")
