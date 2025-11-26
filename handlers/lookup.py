from telegram import Update
from telegram.ext import ContextTypes
from services.cliente_supabase import db_client, logger

async def handle_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Busca una PO en la estructura relacional (Sales Orders + Items).
    Uso: /po P083638
    """
    if len(context.args) == 0:
        await update.message.reply_text(
            "⚠️ Necesito el número de PO.\nEjemplo: <code>/po P083638</code>",
            parse_mode="HTML"
        )
        return

    po_number = context.args[0].strip().upper()
    await update.message.reply_text(f"🔍 Buscando <b>{po_number}</b> en la base relacional...", parse_mode="HTML")

    try:
        # 1. CONSULTA DE CABECERA (La Nave)
        res_head = db_client.table("sales_orders").select("*").eq("po_number", po_number).execute()

        if not res_head.data:
            # Fallback opcional: Podrías buscar en la tabla vieja 'confirm_po' aquí si quisieras
            await update.message.reply_text(f"❌ No encontré la PO <b>{po_number}</b> en la tabla de órdenes.", parse_mode="HTML")
            return

        orden = res_head.data[0]
        order_id = orden['id']

        # 2. CONSULTA DE DETALLES (La Carga)
        res_items = db_client.table("sales_items").select("*").eq("order_id", order_id).execute()
        items = res_items.data or []

        # 3. CONSTRUCCIÓN DEL REPORTE
        # Encabezado
        mensaje = (
            f"📦 <b>REPORTE DE ORDEN {po_number}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Fecha:</b> {orden.get('ship_date')}\n"
            f"🏭 <b>Vendor:</b> {orden.get('vendor')}\n"
            f"📍 <b>Origen:</b> {orden.get('origin')}\n"
            f"📊 <b>Total Cajas:</b> {orden.get('total_boxes')}\n"
            f"💰 <b>Total Valor:</b> ${orden.get('total_value', 0):.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>Detalle de Ítems ({len(items)}):</b>\n\n"
        )

        # Iteración de ítems
        if items:
            for i, item in enumerate(items, 1):
                cliente = item.get('customer_code', 'N/A')
                producto = item.get('product_name', 'Producto Desconocido')
                cajas = item.get('boxes', 0)
                tipo = item.get('box_type', 'QB')
                precio = item.get('unit_price', 0)
                
                mensaje += (
                    f"<b>{i}. {cliente}</b>\n"
                    f"   └ 🌺 {producto}\n"
                    f"   └ 📦 {cajas} {tipo}  | 💲${precio}\n\n"
                )
        else:
            mensaje += "⚠️ <i>La orden existe pero no tiene ítems asociados.</i>"

        await update.message.reply_text(mensaje, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error buscando PO {po_number}: {e}")
        await update.message.reply_text(f"💥 Error técnico buscando la orden: {e}")
