import os
from telegram import Update
from telegram.ext import ContextTypes

# Crear carpeta uploads incluso en Railway
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mensaje = update.message

    if not mensaje.document:
        await update.message.reply_text("No recibí archivo, solo datos etéreos.")
        return

    archivo = mensaje.document
    file_id = archivo.file_id
    nombre = archivo.file_name

    # Descarga
    tg_file = await context.bot.get_file(file_id)
    ruta = os.path.join(UPLOAD_DIR, nombre)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(
        f"Archivo recibido: {nombre}\n"
        f"Guardado temporalmente.\n"
        "Listo para procesar."
    )

    # Aquí después llamaremos tu procesamiento inteligente
    # Ejemplo futuro:
    # df = load_any_table(ruta)
    # subir_a_postgres(df)
