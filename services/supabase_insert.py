import os
from typing import List, Optional

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
# Columnas que en cada tabla son INTEGER en la base de datos
# -------------------------------------------------------------------
INT_COLS_BY_TABLE = {
    "confirm_po": ["boxes", "confirmed", "total_units"],
    # aquí puedes añadir más tablas si hace falta
}


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
            # Si no se puede convertir, mejor mandarlo como NULL
            fila[col] = None


def insertar_dataframe(
    nombre_tabla: str,
    df: pd.DataFrame,
    columna_unica: Optional[str] = None,
) -> str:
    """
    Inserta / upsertea un DataFrame en Supabase, saneando enteros
    para evitar el clásico:
        invalid input syntax for type integer: "1.0"
    """

    filas = df.to_dict(orient="records")

    if not filas:
        return f"0 filas (nada que insertar) en {nombre_tabla}"

    # Saneamos enteros según la tabla destino
    int_cols = INT_COLS_BY_TABLE.get(nombre_tabla, [])
    for fila in filas:
        _sanear_enteros_en_fila(fila, int_cols)

    # Llamada a Supabase
    query = supabase.table(nombre_tabla)
    if columna_unica:
        resp = query.upsert(filas, on_conflict=columna_unica).execute()
    else:
        resp = query.insert(filas).execute()

    # Manejo de error según objeto de respuesta de supabase-py
    error = getattr(resp, "error", None)
    if error:
        # esto sube el error para que el handler te lo muestre en Telegram
        raise Exception(error)

    data = getattr(resp, "data", None) or []
    return f"{len(data)} filas procesadas en {nombre_tabla}"

