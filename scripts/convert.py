import pandas as pd
from pathlib import Path
import json

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAP = {
    "las_cuchillas": "Las Cuchillas",
    "los_carbonales": "Los Carbonales",
    "monte_fresco": "Monte Fresco",
    "primero_de_mayo": "Primero de Mayo",
}

WANT = [
    "numero", "número", "nombre", "e.mes", "e. años", "edad año - mes",
    "ea", "dpar", "#c", "#p", "abort", "gest", "f. preñ", "estado reprod",
    "tpreñez", "d.ab", "f. p. p", "uiep", "del"
]

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
    scan = min(60, len(df))
    for i in range(scan):
        txt = row_text(df.iloc[i].tolist()).lower()
        score = sum(1 for w in WANT if w in txt)
        if score > best_score:
            best_score, best_i = score, i
    if best_i is None or best_score < 6:
        raise ValueError("No pude detectar la fila de encabezados en el Excel.")
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

    meta_lines = [row_text(raw.iloc[i].tolist()) for i in range(hidx)]
    meta_lines = [ln for ln in meta_lines if ln]

    header_row = raw.iloc[hidx].tolist()
    headers = [(("" if pd.isna(v) else str(v)).strip()) for v in header_row]

    last = 0
    for i, h in enumerate(headers):
        if h:
            last = i
    headers = headers[: last + 1]
    headers = make_unique(headers)

    data = raw.iloc[hidx + 1 :, : last + 1].copy()
    data.columns = headers
    data = data.fillna("")

    data = data.loc[~(data.astype(str).apply(lambda r: "".join(r).strip() == "", axis=1))]

    out_csv = OUTPUT_DIR / f"{slug}.csv"
    with open(out_csv, "w", encoding="utf-8", newline="") as fp:
        for ln in meta_lines:
            fp.write(ln + "\n")
        data.to_csv(fp, index=False, sep=";")

    index[slug] = {"label": label, "csv": f"data/{slug}.csv", "source": f.name}

with open(OUTPUT_DIR / "fincas.json", "w", encoding="utf-8") as fp:
    json.dump(index, fp, ensure_ascii=False, indent=2)

print("OK -> data/*.csv y data/fincas.json")
