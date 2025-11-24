from functools import lru_cache


def detectar_tabla(df):
    """
    Versión simple para adivinar tabla a partir de columnas.
    No es crítica para el flujo actual, pero la dejamos por si la usas.
    """
    columnas = set(df.columns)

    reglas = {
        "price_list": {"price", "variety", "stem", "color"},
        "customers": {"customer", "country", "email"},
        "farms": {"farm", "location", "code"},
        "confirm_po": {"po_number", "product", "boxes"},
        "proveedores": {"codigo", "proveedor"},
        "airlines": {"cod", "aerolinea"},
    }

    for tabla, columnas_esperadas in reglas.items():
        if columnas_esperadas.intersection(columnas):
            return tabla

    return None


# Mapa estático de columnas por tabla.
# Importante: los nombres aquí deben ser EXACTAMENTE los de Supabase.
COLUMNAS_TABLAS = {
    # Esta realmente no la usamos en el handler (porque confirm_po
    # tiene su mapeo especial), pero la dejo coherente igual.
    "confirm_po": [
        {"nombre": "po_number"},
        {"nombre": "vendor"},
        {"nombre": "ship_date"},
        {"nombre": "product"},
        {"nombre": "boxes"},
        {"nombre": "confirmed"},
        {"nombre": "box_type"},
        {"nombre": "total_units"},
        {"nombre": "cost"},
        {"nombre": "customer_name"},
        {"nombre": "origin"},
        {"nombre": "status"},
        {"nombre": "mark_code"},
        {"nombre": "ship_country"},
        {"nombre": "notes"},
        {"nombre": "import_batch_id"},
        {"nombre": "source_file"},
        {"nombre": "b_t"},
        {"nombre": "total_u"},
    ],

    # Operacional - Proveedores.csv  (normalizado por tu table_loader)
    # Columnas en el CSV -> 'codigo','proveedor','contacto', 'direccion',...
    "proveedores": [
        {"nombre": "codigo"},
        {"nombre": "proveedor"},
        {"nombre": "contacto"},
        {"nombre": "direccion"},
        {"nombre": "ciudad"},
        {"nombre": "edo"},
        {"nombre": "pais"},
        {"nombre": "telefono"},
        {"nombre": "nit"},
        {"nombre": "predio_fito"},
        {"nombre": "ng"},
        {"nombre": "mes_trm"},
        {"nombre": "dia_trm"},
        {"nombre": "gerente"},
        {"nombre": "correo"},
        {"nombre": "cod1q"},
        {"nombre": "correo_po"},
    ],

    # Operacional - AEROLINEAS.csv (después de limpiarlo)
    # El header real es: COD, AEROLINEA, ..., num, dia
    # y tu loader las normaliza a: cod, aerolinea, num, dia
    "airlines": [
        {"nombre": "cod"},
        {"nombre": "aerolinea"},
        {"nombre": "num"},
        {"nombre": "dia"},
    ],
}


@lru_cache(maxsize=32)
def obtener_columnas_tabla(nombre_tabla: str):
    """
    Devuelve una lista de dicts con clave 'nombre' para la tabla dada.

    Esto es exactamente lo que tu handler espera:

        esquema = obtener_columnas_tabla(tabla_destino)
        mapa_db = {
            _norm_generico(col["nombre"]): col["nombre"]
            for col in esquema
        }
    """
    return COLUMNAS_TABLAS.get(nombre_tabla, [])
