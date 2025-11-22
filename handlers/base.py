from telegram import Update
from telegram.ext import ContextTypes
from supabase_client import supabase_select

async def handle_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.split()

    if len(txt) < 2:
        await update.message.reply_text("Necesito un número de PO. Ejemplos:\n/po P083638")
        return

    po = txt[1]

    result = supabase_select("confirm_po", {"po_number": po})

    if not result:
        await update.message.reply_text("No encontré nada en la base…")
        return

    rows = result
    respuesta = f"📦 *Resultados para {po}:*\n\n"

    for r in rows:
        respuesta += (
            f"• Producto: {r['product']}\n"
            f"  Cliente: {r['customer_name']}\n"
            f"  Cajas: {r['boxes']} confirmadas: {r['confirmed']}\n"
            f"  Status: {r['status']}\n\n"
        )

    await update.message.reply_text(respuesta, parse_mode="Markdown")
