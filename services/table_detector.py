from functools import lru_cache

def detectar_tabla(df):
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


COLUMNAS_TABLAS = {
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
    ],

    # ------- VERSIÓN ESTABLE DE PROVEEDORES -------
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

    # ------- Airlines SOLO con las columnas reales -------
    "airlines": [
        {"nombre": "cod"},
        {"nombre": "aerolinea"},
    ],
}

@lru_cache(maxsize=32)
def obtener_columnas_tabla(nombre_tabla: str):
    return COLUMNAS_TABLAS.get(nombre_tabla, [])
