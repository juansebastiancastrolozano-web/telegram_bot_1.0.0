from telegram import Update
from telegram.ext import ContextTypes
import requests
import os
from dotenv import load_dotenv

# Cargar .env también aquí
load_dotenv()

N8N_PO_URL = os.getenv("N8N_PO_URL")

async def handle_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) == 0:
        await update.message.reply_text(
            "Necesito el número de PO. Ejemplo: /po P083638"
        )
        return

    po_number = context.args[0]

    await update.message.reply_text("🔎 Consultando PO en el sistema...")

    try:
        res = requests.post(N8N_PO_URL, json={"po": po_number})

        if res.status_code != 200:
            await update.message.reply_text(
                f"⚠️ Hubo un problema consultando la PO (error {res.status_code})."
            )
            return

        data = res.json()

        await update.message.reply_text(
            f"📄 Resultado de la PO {po_number}:\n\n"
            f"Cliente: {data.get('customer', '—')}\n"
            f"Producto: {data.get('product', '—')}\n"
            f"Cajas: {data.get('boxes', '—')}\n"
            f"Estado: {data.get('status', '—')}"
        )

    except Exception as e:
        await update.message.reply_text(f"💥 Error: {e}")

