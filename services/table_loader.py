import pandas as pd
import numpy as np
import os

def detectar_header(df):
    mejor_idx = 0
    mejor_score = -1

    for i, row in df.iterrows():
        # Conteo de celdas no vacías como heurística
        score = row.count()

        # También damos puntos si hay strings presentes (muy típico de headers)
        score += sum(row.apply(lambda x: isinstance(x, str)))

        if score > mejor_score:
            mejor_score = score
            mejor_idx = i

    return mejor_idx


def cargar_tabla(path):
    ext = os.path.splitext(path)[1].lower()

    if ext in ['.xlsx', '.xls']:
        df_raw = pd.read_excel(path, header=None)
    elif ext in ['.csv']:
        df_raw = pd.read_csv(path, header=None)
    else:
        raise Exception(f"No sé leer este tipo de archivo: {ext}")

    # Detectar encabezado real
    header = detectar_header(df_raw)

    df = None
    if ext in ["xlsx", ".xls"]:
        df = pd.read_excel(path, header=header)
    else:
        df = pd.read_csv(path, header=header)

    # Quitar filas o columnas completamente vacías
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    # Normalizar nombres de columnas
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
    )

    return df
