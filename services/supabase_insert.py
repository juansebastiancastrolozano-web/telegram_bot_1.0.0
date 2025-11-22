import os
import pandas as pd
import numpy as np
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def convertir_fechas(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df

def limpiar_valores_invalidos(df):
    # Reemplazar NaN, inf, -inf por None
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)

    # Convertir números demasiado grandes o raros a strings
    for col in df.columns:
        for i, val in enumerate(df[col]):
            if isinstance(val, float):
                if val is None:
                    continue
                # JSON no permite floats fuera de rango
                if np.isinf(val) or np.isnan(val):
                    df.at[i, col] = None
                # Si el valor es demasiado grande para JSON
                elif abs(val) > 1e308:
                    df.at[i, col] = str(val)
            elif isinstance(val, pd.Timestamp):
                df.at[i, col] = val.strftime("%Y-%m-%d")
    return df

def insertar_dataframe(tabla, df, columna_unica=None):
    # Convertir todo a string seguro
    df = df.astype(str)

    # Limpiar valores prohibidos JSON
    df = df.replace({
        "nan": None,
        "NaN": None,
        "None": None,
        "NaT": None,
        "inf": None,
        "-inf": None,
        "": None
    })

    datos = df.to_dict(orient="records")

    if columna_unica:
        res = supabase.table(tabla).upsert(
            datos,
            on_conflict=columna_unica
        ).execute()
    else:
        res = supabase.table(tabla).insert(datos).execute()

    if res.error:
        raise Exception(res.error)

    return f"{len(datos)} filas insertadas/upserteadas en {tabla}."

