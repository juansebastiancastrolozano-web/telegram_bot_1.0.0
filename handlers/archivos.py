import os
import pandas as pd
import numpy as np
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    archivo = update.message.document
    tg_file = await context.bot.get_file(archivo.file_id)

    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"Procesando {archivo.file_name}…")

    try:
        df = cargar_tabla(ruta)

        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text("Primero selecciona tabla con /tabla confirm_po")
            return

        # -------------------------------
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
            "notes_for_the_vendor": "notes"
        }

        df.rename(columns=mapeo, inplace=True)

        # -------------------------------
        # FIX NUMÉRICO
        # -------------------------------
        cols_int = ["boxes", "confirmed", "total_units"]

        def conv(x):
            if pd.isna(x): return None
            s = str(x).strip().lower()
            if s in ["", "nan", "none", "<na>", "na"]: return None
            try: return int(float(x))
            except: return None

        for col in cols_int:
            if col in df.columns:
                df[col] = df[col].apply(conv)

        # -------------------------------
        # CORTAMOS TODO LO QUE NO EXISTA:
        # -------------------------------

        columnas_validas = list(mapeo.values())

        # Mantener solo columnas válidas
        df = df[columnas_validas].copy()

        # ----------------------------------
        # FILTRAR FILAS VACÍAS
        # ----------------------------------
        df.dropna(how="all", inplace=True)

        resultado = insertar_dataframe(
            tabla_destino,
            df,
            columna_unica="po_number,product,vendor"
        )

        await update.message.reply_text(f"✔️ {resultado}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

