from telegram import Update
from telegram.ext import ContextTypes
import requests
import os

N8N_FINCA_URL = os.getenv("N8N_FINCA_URL")

async def handle_finca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) == 0:
        await update.message.reply_text("Necesito un código de finca. Ejemplo:\n/finca FLO")
        return

    code = context.args[0].upper()

    await update.message.reply_text(f"🏞️ Buscando finca {code}...")

    try:
        res = requests.post(N8N_FINCA_URL, json={"finca": code})

        if res.status_code != 200:
            await update.message.reply_text("⚠️ Error al consultar la finca.")
            return

        data = res.json()

        respuesta = (
            f"📄 *Finca encontrada:*\n\n"
            f"Nombre: {data.get('name')}\n"
            f"Código: {data.get('code')}\n"
            f"País: {data.get('country')}\n"
            f"Contacto: {data.get('contact')}\n"
            f"Teléfono: {data.get('phone')}\n"
            f"Email: {data.get('email')}\n"
        )

        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"💥 Error inesperado: {e}")
