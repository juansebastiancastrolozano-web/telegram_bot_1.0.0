from supabase_client import supabase

# Columnas que DEBEN ser enteros por tabla
INT_COLS_BY_TABLE = {
    "confirm_po": ["boxes", "confirmed", "total_units"],
    # aquí luego puedes añadir otras tablas
}


def _sanear_enteros_en_fila(fila: dict, columnas: list[str]) -> None:
    """
    Convierte cualquier cosa rara ("1.0", 1.0, " 2 ", "<NA>", "nan") a int o None.
    Modifica la fila en sitio.
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

    # DataFrame -> lista de dicts
    filas = df.to_dict(orient="records")

    if not filas:
        return f"0 filas (nada que insertar) en {nombre_tabla}"

    # Saneamos enteros por tabla
    int_cols = INT_COLS_BY_TABLE.get(nombre_tabla, [])

    for fila in filas:
        _sanear_enteros_en_fila(fila, int_cols)

    # Llamada a Supabase
    query = supabase.table(nombre_tabla)

    if columna_unica:
        respuesta = query.upsert(filas, on_conflict=columna_unica).execute()
    else:
        respuesta = query.insert(filas).execute()

    # Manejo de errores del cliente de Supabase
    error = getattr(respuesta, "error", None)
    if error:
        # Lo lanzamos para que el handler lo capture y te lo muestre
        raise Exception(error)

    data = getattr(respuesta, "data", None) or []
    return f"{len(data)} filas procesadas en {nombre_tabla}"

