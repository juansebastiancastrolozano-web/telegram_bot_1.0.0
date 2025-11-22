

def detectar_tabla(df):
    columnas = set(df.columns)

    reglas = {
        "price_list": {"price", "variety", "stem", "color"},
        "customers": {"customer", "country", "email"},
        "farms": {"farm", "location", "code"},
        "confirm_po": {"po_number", "customer", "variety", "stems"},
    }

    for tabla, columnas_esperadas in reglas.items():
        if columnas_esperadas.intersection(columnas):
            return tabla

    return None
