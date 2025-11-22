# services/supabase_insert.py

import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def insertar_dataframe(tabla, df, columna_unica=None):
    datos = df.to_dict(orient="records")

    if not datos:
        return "La tabla quedó vacía, no se insertó nada."

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

