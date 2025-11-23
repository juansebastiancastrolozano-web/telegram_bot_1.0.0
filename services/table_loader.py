import pandas as pd

def _normalizar_columna(nombre) -> str:
    """
    Convierte 'PO #' -> 'po', 'Ship Date' -> 'ship_date',
    'Notes for the vendor' -> 'notes_for_the_vendor', etc.
    """
    if pd.isna(nombre):
        return ""

    s = str(nombre).strip()

    # Quitar símbolos molestos
    for ch in ["#", "/", "-", "."]:
        s = s.replace(ch, " ")

    # Colapsar espacios y pasar a snake_case
    s = "_".join(s.split())
    return s.lower()


def cargar_tabla(ruta: str) -> pd.DataFrame:
    # Leemos SIN encabezado, porque el header real está más abajo
    raw = pd.read_excel(ruta, header=None)

    header_row_idx = None

    # Buscamos la fila donde esté "PO #" (o variantes) para usarla como encabezado
    for idx, row in raw.iterrows():
        valores = [str(v).strip().lower() for v in row.tolist() if not pd.isna(v)]
        if any(v in ("po #", "po#", "po") for v in valores):
            header_row_idx = idx
            break

    if header_row_idx is None:
        # Fallback: primera fila no vacía
        for idx, row in raw.iterrows():
            if not row.isna().all():
                header_row_idx = idx
                break

    # Fila de encabezado real
    header_row = raw.iloc[header_row_idx]

    # Datos empiezan en la fila siguiente
    data = raw.iloc[header_row_idx + 1 :].copy()

    # Asignar nombres normalizados
    data.columns = [_normalizar_columna(c) for c in header_row]

    # Eliminar filas totalmente vacías
    data = data.dropna(how="all").reset_index(drop=True)

    return data

