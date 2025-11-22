# handlers/archivos.py

import os
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.table_detector import detectar_tabla
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
        tabla_seleccionada = user_tablas.get(user_id)

        if tabla_seleccionada:
            tabla_destino = tabla_seleccionada
        else:
            tabla_destino = detectar_tabla(df)

        if not tabla_destino:
            await update.message.reply_text(
                "No pude detectar a qué tabla pertenece.\n"
                "Usa /tabla <nombre> antes de subir el archivo."
            )
            return

        # Si quieres evitar duplicados, define la columna única
        columna_unica = "po_number" if tabla_destino == "confirm_po" else None

        resultado = insertar_dataframe(tabla_destino, df, columna_unica)
        await update.message.reply_text(f"✔️ {resultado}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

