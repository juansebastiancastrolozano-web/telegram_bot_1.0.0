import os
from typing import List, Optional
import datetime as dt
import math

import pandas as pd
from supabase import create_client, Client

# -------------------------------------------------------------------
# Crear cliente de Supabase usando las variables de entorno
# -------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_KEY en las variables de entorno")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------------------------------------------------
# Columnas que son INTEGER / DATE por tabla
# -------------------------------------------------------------------
INT_COLS_BY_TABLE = {
    "confirm_po": ["boxes", "confirmed", "total_units"],
    # aquí puedes añadir más tablas
}

DATE_COLS_BY_TABLE = {
    "confirm_po": ["ship_date"],
    # idem, puedes añadir más luego
}

# -------------------------------------------------------------------
# Saneadores
# -------------------------------------------------------------------
def _sanear_enteros_en_fila(fila: dict, columnas: List[str]) -> None:
    """
    Convierte cualquier cosa rara ("1.0", 1.0, " 2 ", "<NA>", "nan")
    a int o None. Modifica la fila EN SITIO.
    """
    for col in columnas:
        if col not in fila:
            continue

        valor = fila[col]

        if valor is None:
            continue

        s = str(valor).strip().lower()

        if s in ("", "nan", "none", "<na>", "na"):
            fila[col] = None
            continue

        try:
            fila[col] = int(float(s))
        except Exception:
            fila[col] = None


def _sanear_fechas_en_fila(fila: dict, columnas: List[str]) -> None:
    """
    Convierte cualquier cosa a fecha ISO "YYYY-MM-DD" o None.
    Apta para ship_date, etc.
    """
    for col in columnas:
        if col not in fila:
            continue

        valor = fila[col]

        if valor is None:
            continue

        # NaN de pandas en float
        if isinstance(valor, float) and math.isnan(valor):
            fila[col] = None
            continue

        # Tipos fecha directos
        if isinstance(valor, (pd.Timestamp, dt.datetime, dt.date)):
            fila[col] = valor.strftime("%Y-%m-%d")
            continue

        # String u otra cosa
        s = str(valor).strip()
        if not s or s.lower() in ("nan", "none", "<na>", "na"):
            fila[col] = None
            continue

        try:
            parsed = pd.to_datetime(s, errors="coerce")
            if pd.isna(parsed):
                fila[col] = None
            else:
                fila[col] = parsed.date().strftime("%Y-%m-%d")
        except Exception:
            fila[col] = None


def _sanear_floats_genericos(fila: dict) -> None:
    """
    Recorre TODOS los valores y si encuentra NaN / ±inf en floats,
    los convierte en None para que el JSON sea estándar.
    """
    for k, v in list(fila.items()):
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                fila[k] = None
        # pd.NA / NaN de pandas en otros tipos
        try:
            if pd.isna(v):
                fila[k] = None
        except Exception:
            # si no soporta pd.isna, lo dejamos tal cual
            pass


# -------------------------------------------------------------------
# Inserción / UPSERT
# -------------------------------------------------------------------
def insertar_dataframe(
    nombre_tabla: str,
    df: pd.DataFrame,
    columna_unica: Optional[str] = None,
) -> str:
    """
    Inserta / upsertea un DataFrame en Supabase, saneando enteros, fechas
    y floats raros para evitar errores como:
      - invalid input syntax for type integer: "1.0"
      - Object of type datetime is not JSON serializable
      - Out of range float values are not JSON compliant
    """

    filas = df.to_dict(orient="records")

    if not filas:
        return f"0 filas (nada que insertar) en {nombre_tabla}"

    int_cols = INT_COLS_BY_TABLE.get(nombre_tabla, [])
    date_cols = DATE_COLS_BY_TABLE.get(nombre_tabla, [])

    for fila in filas:
        _sanear_enteros_en_fila(fila, int_cols)
        _sanear_fechas_en_fila(fila, date_cols)
        _sanear_floats_genericos(fila)

    query = supabase.table(nombre_tabla)

    if columna_unica:
        resp = query.upsert(filas, on_conflict=columna_unica).execute()
    else:
        resp = query.insert(filas).execute()

    error = getattr(resp, "error", None)
    if error:
        raise Exception(error)

    data = getattr(resp, "data", None) or []
    return f"{len(data)} filas procesadas en {nombre_tabla}"
