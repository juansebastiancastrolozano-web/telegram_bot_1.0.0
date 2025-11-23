import supabase_client

# Intentamos detectar el cliente real de Supabase
_supabase = getattr(supabase_client, "supabase", None)
if _supabase is None:
    _supabase = getattr(supabase_client, "client", None)
if _supabase is None and hasattr(supabase_client, "get_client"):
    _supabase = supabase_client.get_client()

if _supabase is None:
    # Si llegamos aquí, tu supabase_client.py está raro;
    # mejor explotar con un mensaje claro.
    raise RuntimeError(
        "No encontré ningún cliente en supabase_client.py. "
        "Define algo como 'supabase = create_client(...)' "
        "o 'client = create_client(...)'."
    )

supabase = _supabase

# Columnas que DEBEN ser enteros por tabla
INT_COLS_BY_TABLE = {
    "confirm_po": ["boxes", "confirmed", "total_units"],
    # aquí puedes añadir otras tablas si hace falta
}


def _sanear_enteros_en_fila(fila: dict, columnas: list[str]) -> None:
    """
    Convierte cualquier cosa rara ("1.0", 1.0, " 2 ", "<NA>", "nan")
    a int o None. Modifica la fila en sitio.
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
            # Si no se puede convertir, mejor guardarlo como NULL
            fila[col] = None


def insertar_dataframe(nombre_tabla: str, df, columna_unica: str | None = None) -> str:
    """
    Inserta / upsertea un DataFrame en Supabase, saneando enteros para evitar
    el famoso 'invalid input syntax for type integer: "1.0"'.
    """
    filas = df.to_dict(orient="records")

    if not filas:
        return f"0 filas (nada que insertar) en {nombre_tabla}"

    # Saneamos enteros según la tabla
    int_cols = INT_COLS_BY_TABLE.get(nombre_tabla, [])
    for fila in filas:
        _sanear_enteros_en_fila(fila, int_cols)

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

