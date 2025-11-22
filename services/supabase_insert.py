import os
from supabase import create_client, Client
import pandas as pd

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def convertir_fechas(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_timedelta64_dtype(df[col]):
            df[col] = df[col].astype(str)
    return df

def insertar_dataframe(tabla, df, columna_unica=None):
    df = convertir_fechas(df)

    datos = df.replace({pd.NaT: None}).to_dict(orient="records")

    if not datos:
        return "La tabla está vacía después de procesar."

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

