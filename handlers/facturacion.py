import os
from telegram import Update
from telegram.ext import ContextTypes
from services.generador_pdf import generador_documentos

async def comando_generar_factura(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Genera Factura (Cliente) y PO (Finca).
    """
    if not context.args:
        await update.message.reply_text("⚠️ Uso: `/factura P12345`", parse_mode="Markdown")
        return

    po_number = context.args[0].strip().upper()
    await update.message.reply_text(f"⚙️ Generando documentos para <b>{po_number}</b>...", parse_mode="HTML")

    # Nombres de archivo
    file_factura = f"Factura_{po_number}.pdf"
    file_po = f"PO_Finca_{po_number}.pdf"

    # 1. Generar Factura Cliente
    ok1, msg1 = generador_documentos.generar_factura_cliente(po_number, file_factura)
    # 2. Generar PO Finca
    ok2, msg2 = generador_documentos.generar_po_finca(po_number, file_po)

    if ok1 and ok2:
        # Enviar Factura
        await update.message.reply_document(
            document=open(file_factura, 'rb'),
            caption=f"💵 Factura para el Cliente ({po_number})"
        )
        # Enviar PO
        await update.message.reply_document(
            document=open(file_po, 'rb'),
            caption=f"🚜 Orden para la Finca ({po_number})\n<i>(Sin precios de venta)</i>",
            parse_mode="HTML"
        )
        
        # Limpieza
        os.remove(file_factura)
        os.remove(file_po)
    else:
        await update.message.reply_text(f"❌ Error generando docs: {msg1} // {msg2}")
