import os
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe
from services.table_detector import obtener_columnas_tabla

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _norm_generico(nombre: str) -> str:
    """
    Normalización suave para comparar nombres de columnas:
    - quita espacios extremos
    - pasa a minúsculas
    - convierte espacios internos en '_'

    Ej:
    'Customer Name' -> 'customer_name'
    'CIUDAD'        -> 'ciudad'
    """
    s = str(nombre).strip().lower()
    s = s.replace(" ", "_")
    return s


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    archivo = update.message.document
    tg_file = await context.bot.get_file(archivo.file_id)

    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"Procesando {archivo.file_name}…")

    try:
        # 1) Leemos el archivo (Excel raro o CSV, eso lo decide table_loader)
        df = cargar_tabla(ruta)

        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text(
                "Primero selecciona una tabla con /tabla nombre_tabla"
            )
            return

        # ============================================================
        # CASO ESPECIAL: confirm_po (Excel con nombres diferentes)
        # ============================================================
        if tabla_destino == "confirm_po":
            mapeo = {
                "po": "po_number",
                "vendor": "vendor",
                "ship_date": "ship_date",
                "product": "product",
                "qty_po": "boxes",
                "confirmed": "confirmed",
                "b_t": "box_type",
                "total_u": "total_units",
                "cost": "cost",
                "customer": "customer_name",
                "origin": "origin",
                "status": "status",
                "mark_code": "mark_code",
                "ship_country": "ship_country",
                "notes_for_the_vendor": "notes",
            }

            # Renombrar columnas del Excel al nombre real de Supabase
            df.rename(columns=mapeo, inplace=True)

            # Columnas que en Supabase son integer
            cols_int = ["boxes", "confirmed", "total_units"]

            def convertir_entero_seguro(x):
                if pd.isna(x):
                    return None
                s = str(x).strip().lower()
                if s in ("", "nan", "none", "<na>", "na"):
                    return None
                try:
                    return int(float(s))
                except Exception:
                    return None

            for col in cols_int:
                if col in df.columns:
                    df[col] = df[col].apply(convertir_entero_seguro)

            # Nos quedamos solo con las columnas que existen en la tabla
            columnas_validas = list(mapeo.values())
            df = df[[c for c in df.columns if c in columnas_validas]]

            # Limpieza de filas completamente vacías
            df = df.dropna(how="all").reset_index(drop=True)

            resultado = insertar_dataframe(
                tabla_destino,
                df,
                columna_unica="po_number,product,vendor",
            )

        # ============================================================
        # CASO GENERAL: cualquier otra tabla (proveedores, airlines, etc.)
        # ============================================================
        else:
            # 1) Esquema real de la tabla en Supabase
            esquema = obtener_columnas_tabla(tabla_destino)
            #   esquema = [ {"nombre": "...", "tipo": "...", ...}, ... ]

            # Mapa: nombre_normalizado_en_db -> nombre_real_en_db
            mapa_db = {
                _norm_generico(col["nombre"]): col["nombre"]
                for col in esquema
            }

            # 2) Normalizamos nombres del archivo
            df.columns = [str(c).strip() for c in df.columns]

            columnas_originales = []
            renombrar = {}

            for c in df.columns:
                key = _norm_generico(c)
                if key in mapa_db:
                    columnas_originales.append(c)
                    renombrar[c] = mapa_db[key]
                # si NO está en el mapa_db => se ignora, tal como querías

            # Si ninguna columna coincide, df quedará vacío y ya
            if columnas_originales:
                df = df[columnas_originales]
                df = df.rename(columns=renombrar)
            else:
                df = df.iloc[0:0]  # DataFrame vacío con 0 filas

            # Quitamos filas 100% vacías
            df = df.dropna(how="all").reset_index(drop=True)

            # Inserción sin upsert por defecto (si quieres upsert aquí,
            # se puede meter luego una clave única por tabla).
            resultado = insertar_dataframe(
                tabla_destino,
                df,
                columna_unica=None,
            )

        await update.message.reply_text(f"✔️ {resultado}")

    except Exception as e:
        # Si algo peta, que al menos sepas por qué
        await update.message.reply_text(f"❌ Error: {e}")

