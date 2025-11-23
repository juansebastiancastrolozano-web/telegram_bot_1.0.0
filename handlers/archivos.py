import os
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 1) Descargar archivo enviado por el usuario
    archivo = update.message.document
    tg_file = await context.bot.get_file(archivo.file_id)

    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"Procesando {archivo.file_name}…")

    try:
        # 2) Cargar tabla con el lector universal (detecta encabezado, etc.)
        df = cargar_tabla(ruta)

        # 3) Saber a qué tabla de Supabase va este archivo
        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text(
                "Primero selecciona la tabla destino, por ejemplo:\n"
                "/tabla confirm_po"
            )
            return

        # 4) Mapeo columnas ARCHIVO → columnas SUPABASE
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

        # Renombrar columnas según mapeo (solo las que existan)
        df.rename(columns=mapeo, inplace=True)

        # 5) FILTRO IMPORTANTE:
        #    Nos quedamos solo con filas "de verdad":
        #    las que tienen vendor (BM, etc.).
        #    Así ignoramos todo el bloque de "Report Explanation".
        if "vendor" in df.columns:
            df = df[
                df["vendor"].notna()
                & (df["vendor"].astype(str).str.strip() != "")
            ]

        # 6) Limitar a las columnas que existen en Supabase
        columnas_validas = list(mapeo.values())
        df = df[[c for c in df.columns if c in columnas_validas]]

        # 7) Enviar a Supabase
        #    Limpieza de tipos (enteros, fechas, floats) se hace dentro
        #    de insertar_dataframe().
        resultado = insertar_dataframe(
            tabla_destino,
            df,
            columna_unica="po_number,product,vendor",
        )

        await update.message.reply_text(f"✔️ {resultado}")

    except Exception as e:
        # Si algo explota, lo ves en Telegram
        await update.message.reply_text(f"❌ Error: {e}")

