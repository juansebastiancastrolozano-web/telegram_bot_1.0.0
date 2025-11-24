import pandas as pd

def _normalizar_columna(nombre) -> str:
    if pd.isna(nombre):
        return ""
    s = str(nombre).strip()
    for ch in ["#", "/", "-", "."]:
        s = s.replace(ch, " ")
    s = "_".join(s.split())
    return s.lower()


def _cargar_excel_con_encabezado_profundo(ruta: str) -> pd.DataFrame:
    raw = pd.read_excel(ruta, header=None)

    header_row_idx = None
    for idx, row in raw.iterrows():
        valores = [str(v).strip().lower() for v in row.tolist() if not pd.isna(v)]
        if any(v in ("po #", "po#", "po") for v in valores):
            header_row_idx = idx
            break

    if header_row_idx is None:
        for idx, row in raw.iterrows():
            if not row.isna().all():
                header_row_idx = idx
                break

    header_row = raw.iloc[header_row_idx]
    data = raw.iloc[header_row_idx + 1 :].copy()
    data.columns = [_normalizar_columna(c) for c in header_row]
    data = data.dropna(how="all").reset_index(drop=True)
    return data


def cargar_tabla(ruta: str) -> pd.DataFrame:
    ruta_lower = ruta.lower()

    if ruta_lower.endswith(".csv"):
        df = pd.read_csv(ruta)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how="all").reset_index(drop=True)
        return df

    if ruta_lower.endswith(".xls") or ruta_lower.endswith(".xlsx"):
        return _cargar_excel_con_encabezado_profundo(ruta)

    raise ValueError(f"No sé cómo leer este archivo: {ruta}")
