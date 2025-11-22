from telegram import Update
from telegram.ext import ContextTypes
import requests
import os

N8N_CLIENTE_URL = os.getenv("N8N_CLIENTE_URL")

async def handle_cliente(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) == 0:
        await update.message.reply_text("Necesito un código de cliente. Ejemplo:\n/cliente C-WDG")
        return

    code = context.args[0]

    await update.message.reply_text(f"🔎 Buscando cliente {code}...")

    try:
        res = requests.post(N8N_CLIENTE_URL, json={"cliente": code})

        if res.status_code != 200:
            await update.message.reply_text("⚠️ Error al consultar el cliente.")
            return

        data = res.json()

        respuesta = (
            f"📄 *Cliente encontrado:*\n\n"
            f"Nombre: {data.get('name')}\n"
            f"Ciudad: {data.get('city')}\n"
            f"País: {data.get('country')}\n"
            f"Numero: {data.get('phone')}\n"
        )

        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"💥 Error inesperado: {e}")
