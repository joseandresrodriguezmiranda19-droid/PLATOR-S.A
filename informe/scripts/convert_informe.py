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

# Nombre fijo del Excel (para que sea fácil siempre)
# Puedes subir .xlsx o .xls, el script busca ambos.
BASE_NAME = "informe_ganaderia"

def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:80] or "sheet"

def fmt_date(v):
    """Convierte fechas a DD/MM/YYYY (para que el dashboard detecte años/fechas mejor)."""
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""

    if isinstance(v, (pd.Timestamp, datetime)):
        d = v.date()
        return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"

    # serial Excel
    if isinstance(v, (int, float)):
        x = float(v)
        if 1 <= x <= 80000:
            try:
                ts = pd.to_datetime(x, unit="D", origin="1899-12-30")
                d = ts.date()
                return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"
            except Exception:
                pass

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

def trim_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.fillna("")
    # quita filas totalmente vacías
    df = df.loc[~(df.astype(str).apply(lambda r: "".join(r).strip() == "", axis=1))]
    if df.empty:
        return df
    # quita columnas totalmente vacías
    df = df.loc[:, ~(df.astype(str).apply(lambda c: "".join(c).strip() == "", axis=0))]
    return df

def normalize_dates_df(df: pd.DataFrame) -> pd.DataFrame:
    # aplica fmt_date a todo (seguro, porque todo se guarda como texto)
    return df.applymap(fmt_date)

# 1) Buscar el Excel
xlsx = INPUT_DIR / f"{BASE_NAME}.xlsx"
xls  = INPUT_DIR / f"{BASE_NAME}.xls"
if xlsx.exists():
    src = xlsx
elif xls.exists():
    src = xls
else:
    raise SystemExit(f"No encontré {BASE_NAME}.xlsx ni {BASE_NAME}.xls en {INPUT_DIR}")

# 2) Leer todas las hojas
sheets = read_sheets(src)

manifest = []
seen_slugs = set()

for sheet_name, raw in sheets.items():
    if raw is None or raw.empty:
        continue

    df = trim_df(raw)
    if df.empty:
        continue

    # normaliza fechas
    df = normalize_dates_df(df)

    slug = slugify(sheet_name)
    # evitar colisión si hay hojas con nombres parecidos
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
        header=False,          # dejamos el Excel “tal cual”, como reporte
        sep=";",
        encoding="utf-8",
        quoting=csvmod.QUOTE_MINIMAL
    )

    manifest.append({
        "sheet": sheet_name,
        "slug": slug,
        "csv": f"informe/data/{slug}.csv",
        # esto es el “nombre” con el que el dashboard detecta el módulo
        "fileName": f"{sheet_name}.csv"
    })

# 3) Guardar índice para auto-carga
with open(OUTPUT_DIR / "sheets.json", "w", encoding="utf-8") as fp:
    json.dump(manifest, fp, ensure_ascii=False, indent=2)

print(f"OK -> {len(manifest)} hojas exportadas a informe/data/*.csv + sheets.json")
