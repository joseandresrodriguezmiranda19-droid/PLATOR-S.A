import pandas as pd
from pathlib import Path
import json
import re
import unicodedata
import numpy as np
from datetime import datetime, date
import csv as csvmod

INPUT_DIR = Path("informe/input")
OUTPUT_DIR = Path("informe/data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Utilidades
# -----------------------------
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

def parse_date_from_filename(name: str):
    """
    Busca fechas tipo YYYY-MM-DD o YYYYMMDD dentro del nombre.
    Devuelve date o None.
    """
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", name)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    m = re.search(r"(\d{4})(\d{2})(\d{2})", name)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    return None

def pick_latest_excel(input_dir: Path) -> Path:
    files = sorted([p for p in input_dir.glob("*") if p.suffix.lower() in [".xls", ".xlsx"]])
    if not files:
        raise SystemExit(f"No hay archivos .xls/.xlsx en: {input_dir}")

    scored = []
    for p in files:
        d = parse_date_from_filename(p.name)
        scored.append((d or date(1900, 1, 1), p.name.lower(), p))

    scored.sort()
    return scored[-1][2]

def fmt_date(v):
    """
    Normaliza fechas a DD/MM/YYYY (solo cuando detecta fechas).
    Deja otros valores como texto.
    """
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
    """
    Nombre “amigable” para que tu dashboard detecte el módulo por nombre.
    Ajusta aquí si tus hojas tienen otros nombres.
    """
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

# -----------------------------
# Proceso principal
# -----------------------------
src = pick_latest_excel(INPUT_DIR)
used_date = parse_date_from_filename(src.name)

print("Usando archivo:", src.name)

sheets = read_sheets(src)

manifest = []
seen_slugs = set()

for sheet_name, raw in sheets.items():
    if raw is None or raw.empty:
        continue

    df = raw.copy().fillna("")

    # Normalizar fechas (evita "YYYY-MM-DD 00:00:00")
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
        header=False,
        sep=";",
        encoding="utf-8",
        quoting=csvmod.QUOTE_MINIMAL
    )

    manifest.append({
        "sheet": sheet_name,
        "slug": slug,
        "csv": f"data/{slug}.csv",                 # relativo a informe/index.html
        "fileName": canonical_filename(sheet_name) # nombre para detección
    })

# Índice para auto-carga
with open(OUTPUT_DIR / "sheets.json", "w", encoding="utf-8") as fp:
    json.dump(manifest, fp, ensure_ascii=False, indent=2)

# Meta (no rompe nada, es extra)
meta = {
    "source_file": src.name,
    "source_date": used_date.isoformat() if used_date else None,
    "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "sheets_count": len(manifest)
}
with open(OUTPUT_DIR / "source.json", "w", encoding="utf-8") as fp:
    json.dump(meta, fp, ensure_ascii=False, indent=2)

print(f"OK -> {len(manifest)} hojas exportadas en informe/data/ + sheets.json + source.json")
