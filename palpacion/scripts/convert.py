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

# Fincas esperadas (según tus nombres fijos en /input)
MAP = {
    "las_cuchillas": "Las Cuchillas",
    "los_carbonales": "Los Carbonales",
    "monte_fresco": "Monte Fresco",
    "primero_de_mayo": "Primero de Mayo",
}

# Código "ES-xxx" (solo para asegurar que el dashboard nombre bien la finca en el meta)
CODE_MAP = {
    "las_cuchillas": "ES-003",
    "los_carbonales": "ES-005",
    "monte_fresco": "ES-007",
    "primero_de_mayo": "ES-001",
}

# Palabras clave para detectar la fila de encabezados
WANT = [
    "numero", "número", "nombre", "e.mes", "e. años", "edad año - mes",
    "ea", "dpar", "#c", "#p", "abort", "gest", "f. preñ",
    "estado reprod", "tpreñez", "d.ab", "f. p. p", "uiep", "del"
]

# Si quieres que el CSV tenga una columna "Hoja" (útil para depurar)
INCLUDE_SHEET_COLUMN = True

# Si quieres eliminar duplicados por Número (si te salen repetidos por varias hojas)
DEDUPE_BY_NUMERO = False


def canon(s: str) -> str:
    s = str(s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def read_excel_sheets(path: Path):
    """Devuelve dict: {nombre_hoja: DataFrame} con header=None (todo crudo)."""
    suf = path.suffix.lower()
    if suf == ".xls":
        return pd.read_excel(path, sheet_name=None, header=None, engine="xlrd")
    return pd.read_excel(path, sheet_name=None, header=None, engine="openpyxl")


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
    """Devuelve fecha en ISO 'YYYY-MM-DD' cuando se puede, o ''."""
    if x is None:
        return ""
    if isinstance(x, float) and np.isnan(x):
        return ""

    if isinstance(x, (pd.Timestamp, datetime)):
        return x.date().isoformat()

    # Excel serial number
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

    # "YYYY-MM-DD 00:00:00" -> "YYYY-MM-DD"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    # "DD/MM/YYYY" -> ISO
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        if y < 100:
            y = 2000 + y
        return f"{y:04d}-{mo:02d}-{d:02d}"

    return s


def normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas de fecha por nombre aproximado."""
    for col in list(df.columns):
        ck = canon(col)
        # cubre: F. Preñ, Fecha Preñez, F. P. P, UIEP, etc (si fueran fechas)
        if ("fpre" in ck) or ("fpp" in ck) or ("fechap" in ck) or ("uiep" in ck) or ("fechapren" in ck):
            df[col] = df[col].apply(fmt_excel_date)
    return df


def extract_table_from_sheet(raw: pd.DataFrame, sheet_name: str):
    """Devuelve (meta_lines, data_df) o None si no detecta tabla."""
    hidx = find_header_idx(raw)

    meta_lines = [row_text(raw.iloc[i].tolist()) for i in range(hidx)]
    meta_lines = [ln for ln in meta_lines if ln]

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

    # eliminar filas completamente vacías
    data = data.loc[~(data.astype(str).apply(lambda r: "".join(r).strip() == "", axis=1))]

    if INCLUDE_SHEET_COLUMN:
        data.insert(0, "Hoja", sheet_name)

    return meta_lines, data


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

    sheets = read_excel_sheets(f)  # dict hoja -> df
    all_data = []
    meta_base = None
    used_sheets = []

    for sheet_name, raw in sheets.items():
        try:
            meta_lines, data = extract_table_from_sheet(raw, sheet_name)
        except Exception:
            continue  # hoja sin tabla

        if meta_base is None:
            meta_base = meta_lines
        all_data.append(data)
        used_sheets.append(sheet_name)

    if not all_data:
        continue

    data_all = pd.concat(all_data, ignore_index=True)

    # Normaliza fechas (para que el dashboard calcule Próximos Partos, etc.)
    data_all = normalize_date_columns(data_all)

    # Quita filas de totales si vienen dentro de la tabla
    try:
        mask_total = data_all.astype(str).apply(
            lambda r: ("total animales" in " ".join(r).lower()) or ("total:" in " ".join(r).lower()),
            axis=1
        )
        data_all = data_all.loc[~mask_total]
    except Exception:
        pass

    # Deduplicar por Número (si se repite por varias hojas)
    if DEDUPE_BY_NUMERO:
        # intenta encontrar columna que parezca "Número"
        num_col = None
        for c in data_all.columns:
            if canon(c) in {"numero", "número", "no", "no.", "numeroanimal", "numerodelanimal"}:
                num_col = c
                break
        if num_col:
            data_all = data_all.drop_duplicates(subset=[num_col], keep="first")

    # Meta: fuerza ES-xxx - FINCA (para que el dashboard lo detecte)
    meta_lines_final = meta_base[:] if meta_base else []
    meta_lines_final = [ln for ln in meta_lines_final if not re.search(r"ES-\d+\s*-\s*", ln, re.I)]
    meta_lines_final.insert(0, f"{CODE_MAP.get(slug,'ES-001')} - {label.upper()}")

    if used_sheets:
        meta_lines_final.append(f"Hojas incluidas: {', '.join(used_sheets)}")

    out_csv = OUTPUT_DIR / f"{slug}.csv"
    with open(out_csv, "w", encoding="utf-8", newline="") as fp:
        for ln in meta_lines_final:
            fp.write(ln + "\n")
        data_all.to_csv(fp, index=False, sep=";", encoding="utf-8")

    index[slug] = {"label": label, "csv": f"data/{slug}.csv", "source": f.name}

with open(OUTPUT_DIR / "fincas.json", "w", encoding="utf-8") as fp:
    json.dump(index, fp, ensure_ascii=False, indent=2)

print("OK -> data/*.csv y data/fincas.json (multi-hoja)")
