import pandas as pd
from pathlib import Path
import json
import re
import unicodedata
import numpy as np
from datetime import datetime

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAP = {
    "las_cuchillas": "Las Cuchillas",
    "los_carbonales": "Los Carbonales",
    "monte_fresco": "Monte Fresco",
    "primero_de_mayo": "Primero de Mayo",
}

# Códigos “fake” solo para que el dashboard nombre bien la finca cuando no viene ES-xxx en el archivo
CODE_MAP = {
    "las_cuchillas": "ES-003",
    "los_carbonales": "ES-005",
    "monte_fresco": "ES-007",
    "primero_de_mayo": "ES-001",
}

WANT = [
    "numero","número","nombre","e.mes","e. años","edad año - mes",
    "ea","dpar","#c","#p","abort","gest","f. preñ","estado reprod",
    "tpreñez","d.ab","f. p. p","uiep","del"
]

def canon(s: str) -> str:
    s = str(s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def read_excel_any(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".xls":
        return pd.read_excel(path, sheet_name=0, header=None, engine="xlrd")
    return pd.read_excel(path, sheet_name=0, header=None, engine="openpyxl")

def row_text(row) -> str:
    parts = []
    for v in row:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            parts.append(s)
    return " ".join(parts)

def find_header_idx(df: pd.DataFrame) -> int:
    best_i, best_score = None, -1
    scan = min(80, len(df))
    for i in range(scan):
        txt = row_text(df.iloc[i].tolist()).lower()
        score = sum(1 for w in WANT if w in txt)
        if score > best_score:
            best_score, best_i = score, i
    if best_i is None or best_score < 6:
        raise ValueError("No pude detectar la fila de encabezados.")
    return best_i

def make_unique(headers):
    seen = {}
    out = []
    for h in headers:
        h = (h or "").strip()
        if not h:
            h = "COL"
        k = h.lower()
        if k in seen:
            seen[k] += 1
            out.append(f"{h}_{seen[k]}")
        else:
            seen[k] = 1
            out.append(h)
    return out

def fmt_excel_date(x):
    if x is None:
        return ""
    if isinstance(x, float) and np.isnan(x):
        return ""

    # datetime / Timestamp
    if isinstance(x, (pd.Timestamp, datetime)):
        return x.date().isoformat()

    # excel serial number
    if isinstance(x, (int, float)):
        v = float(x)
        if 1 <= v <= 80000:
            try:
                ts = pd.to_datetime(v, unit="D", origin="1899-12-30")
                return ts.date().isoformat()
            except Exception:
                pass

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""

    compact = re.sub(r"\s+", "", s)
    if compact in {"-", "--", "---"}:
        return ""

    # "YYYY-MM-DD 00:00:00" -> "YYYY-MM-DD"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    # "DD/MM/YYYY"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        d = int(m.group(1)); mo = int(m.group(2)); y = int(m.group(3))
        if y < 100:
            y = 2000 + y
        return f"{y:04d}-{mo:02d}-{d:02d}"

    return s

def normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Detecta columnas de fechas por nombre “canonizado”
    for col in list(df.columns):
        ck = canon(col)
        if ("fultpar" in ck) or (ck.startswith("fpre")) or ("fpp" in ck):
            df[col] = df[col].apply(fmt_excel_date)
    return df

index = {}

for slug, label in MAP.items():
    f_xlsx = INPUT_DIR / f"{slug}.xlsx"
    f_xls  = INPUT_DIR / f"{slug}.xls"

    if f_xlsx.exists():
        f = f_xlsx
    elif f_xls.exists():
        f = f_xls
    else:
        continue

    raw = read_excel_any(f)
    hidx = find_header_idx(raw)

    # preámbulo
    meta_lines = [row_text(raw.iloc[i].tolist()) for i in range(hidx)]
    meta_lines = [ln for ln in meta_lines if ln]

    # FORZAR una línea ES-xxx para nombrar finca (por si el archivo no trae)
    meta_lines = [ln for ln in meta_lines if not re.search(r"ES-\d+\s*-\s*", ln, re.I)]
    meta_lines.insert(0, f"{CODE_MAP.get(slug,'ES-001')} - {label.upper()}")

    # encabezados
    header_row = raw.iloc[hidx].tolist()
    headers = [("" if pd.isna(v) else str(v).strip()) for v in header_row]

    last = 0
    for i, h in enumerate(headers):
        if h:
            last = i
    headers = headers[: last + 1]
    headers = make_unique(headers)

    data = raw.iloc[hidx + 1 :, : last + 1].copy()
    data.columns = headers
    data = data.fillna("")

    # quita filas vacías
    data = data.loc[~(data.astype(str).apply(lambda r: "".join(r).strip() == "", axis=1))]

    # normaliza fechas para que el dashboard las entienda
    data = normalize_date_columns(data)

    # quita filas de totales si vienen
    mask_total = data.astype(str).apply(lambda r: "total animales" in " ".join(r).lower(), axis=1)
    data = data.loc[~mask_total]

    out_csv = OUTPUT_DIR / f"{slug}.csv"
    with open(out_csv, "w", encoding="utf-8", newline="") as fp:
        for ln in meta_lines:
            fp.write(ln + "\n")
        data.to_csv(fp, index=False, sep=";")

    index[slug] = {"label": label, "csv": f"data/{slug}.csv", "source": f.name}

with open(OUTPUT_DIR / "fincas.json", "w", encoding="utf-8") as fp:
    json.dump(index, fp, ensure_ascii=False, indent=2)

print("OK -> data/*.csv y data/fincas.json (fechas normalizadas)")
