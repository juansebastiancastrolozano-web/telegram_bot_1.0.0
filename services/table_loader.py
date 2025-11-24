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


def _cargar_excel_con_encabezado_profundo(ruta: str) -> pd.DataFrame:
    """
    Caso Confirm POs.xls:
    - El header real no está en la fila 0.
    - Buscamos la fila donde aparezca 'PO #' (o similar) y la usamos como encabezado.
    """
    # Leemos SIN encabezado, porque el header real está más abajo
    raw = pd.read_excel(ruta, header=None)

    header_row_idx = None

    # Buscamos la fila donde esté algo tipo "PO #"
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


def _cargar_csv_con_encabezado_profundo(ruta: str) -> pd.DataFrame:
    """
    Versión "profunda" para CSV.

    Nos sirve tanto para:
    - CSV normales (header en la fila 0, p.ej. proveedores),
    - Como para cosas tipo AEROLINEAS, donde el header real está varias filas más abajo.

    Estrategia:
    - Leemos con header=None.
    - Buscamos la primera fila que tenga alguna de estas palabras clave en minúsculas:
      'po', 'po #', 'codigo', 'cod', 'vendor', 'proveedor', 'aerolinea'.
    - Esa fila se usa como encabezado.
    """
    raw = pd.read_csv(ruta, header=None)

    header_row_idx = None
    palabras_clave = {"po #", "po#", "po", "codigo", "cod", "vendor", "proveedor", "aerolinea"}

    for idx, row in raw.iterrows():
        valores = [str(v).strip().lower() for v in row.tolist() if not pd.isna(v)]
        if any(v in palabras_clave for v in valores):
            header_row_idx = idx
            break

    if header_row_idx is None:
        # Si no encontramos nada "especial", asumimos fila 0 como encabezado normal.
        header_row_idx = 0

    header_row = raw.iloc[header_row_idx]
    data = raw.iloc[header_row_idx + 1 :].copy()

    data.columns = [_normalizar_columna(c) for c in header_row]
    data = data.dropna(how="all").reset_index(drop=True)
    return data


def cargar_tabla(ruta: str) -> pd.DataFrame:
    """
    Lector universal:
    - Si es CSV  → _cargar_csv_con_encabezado_profundo
    - Si es XLS* → _cargar_excel_con_encabezado_profundo
    """
    ruta_lower = ruta.lower()

    if ruta_lower.endswith(".csv"):
        return _cargar_csv_con_encabezado_profundo(ruta)

    if ruta_lower.endswith(".xls") or ruta_lower.endswith(".xlsx"):
        return _cargar_excel_con_encabezado_profundo(ruta)

    raise ValueError(f"No sé cómo leer este archivo: {ruta}")

