from functools import lru_cache

from supabase_client import supabase


# Columnas conocidas "a mano" para tablas importantes.
# Aquí solo metemos lo que realmente necesitamos controlar.
COLUMNAS_FIJAS = {
    "confirm_po": {
        "id",
        "po_number",
        "vendor",
        "ship_date",
        "product",
        "boxes",
        "confirmed",
        "box_type",
        "total_units",
        "cost",
        "customer_name",
        "origin",
        "status",
        "mark_code",
        "ship_country",
        "notes",
        "created_at",
        "updated_at",
        "import_batch_id",
        "source_file",
        "b_t",
        "total_u",
    },
    # Cuando quieras puedes añadir más tablas:
    # "proveedores": {"id", "cod", "aerolinea", "ciudad", ...},
    # "airlines": {"id", "cod", "aerolinea", ...},
}


def detectar_tabla(df):
    """
    Versión simple que intenta adivinar la tabla por las columnas presentes.
    La puedes seguir usando si te sirve para debugging, pero ya no es crítica.
    """
    columnas = set(df.columns)

    reglas = {
        "price_list": {"price", "variety", "stem", "color"},
        "customers": {"customer", "country", "email"},
        "farms": {"farm", "location", "code"},
        # Ajustado a tu esquema real de confirm_po
        "confirm_po": {"po_number", "product", "boxes"},
    }

    for tabla, columnas_esperadas in reglas.items():
        if columnas_esperadas.intersection(columnas):
            return tabla

    return None


@lru_cache(maxsize=32)
def obtener_columnas_tabla(nombre_tabla: str) -> set[str]:
    """
    Devuelve el set de columnas válidas para `nombre_tabla` en Supabase.

    Orden de resolución:
    1) Si la tabla está en COLUMNAS_FIJAS -> usamos eso.
    2) Si no, hacemos SELECT * LIMIT 1 a Supabase y tomamos las keys del primer row.
    3) Si no hay filas o algo peta, devolvemos set().

    Ojo: devolver set() NO debe romper nada siempre que en el handler hagas:
        if columnas_validas:
            df = df[[c for c in df.columns if c in columnas_validas]]
    """
    if not nombre_tabla:
        return set()

    # 1) Diccionario fijo
    if nombre_tabla in COLUMNAS_FIJAS:
        return COLUMNAS_FIJAS[nombre_tabla]

    # 2) Preguntar a Supabase
    try:
        resp = supabase.table(nombre_tabla).select("*").limit(1).execute()
    except Exception:
        # Si algo falla (tabla mal escrita, fallo de red, etc.) devolvemos set vacío.
        return set()

    # Compatibilidad con posibles formas de respuesta
    data = getattr(resp, "data", None)
    if data is None and isinstance(resp, dict):
        data = resp.get("data")

    if not data:
        # Tabla sin filas: no podemos inferir columnas -> dejamos que el caller decida.
        return set()

    first_row = data[0]
    return set(first_row.keys())

