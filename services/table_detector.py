from functools import lru_cache

COLUMNAS_TABLAS = {
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

    "airlines": [
        {"nombre": "cod"},
        {"nombre": "aerolinea"},
        {"nombre": "num"},
        {"nombre": "dia"},
    ],

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
    ]
}


@lru_cache(maxsize=32)
def obtener_columnas_tabla(nombre_tabla: str):
    return COLUMNAS_TABLAS.get(nombre_tabla, [])
