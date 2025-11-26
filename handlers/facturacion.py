import os
from telegram import Update
from telegram.ext import ContextTypes
from services.generador_pdf import generador_pdf

async def comando_generar_factura(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Uso: `/factura P083638`", parse_mode="Markdown")
        return

    po_number = context.args[0].upper().strip()
    await update.message.reply_text(f"📄 Generando factura para <b>{po_number}</b>...", parse_mode="HTML")

    # Ruta temporal
    ruta_pdf = f"Factura_{po_number}.pdf"

    exito, resultado = generador_pdf.generar_pdf_orden(po_number, ruta_pdf)

    if exito:
        await update.message.reply_document(document=open(ruta_pdf, 'rb'), caption=f"Aquí tienes la factura de {po_number}")
        os.remove(ruta_pdf) # Limpieza
    else:
        await update.message.reply_text(f"❌ Error: {resultado}")
