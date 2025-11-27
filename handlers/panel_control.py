from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.cliente_supabase import db_client

async def comando_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Muestra el Tablero de Control con las órdenes pendientes.
    Reemplaza visualmente a la hoja ORDENAA.
    """
    # 1. Consultar Órdenes en Borrador (Draft) o Confirmadas pero sin Facturar
    res = db_client.table("sales_orders")\
        .select("*")\
        .neq("status", "Invoiced")\
        .order("created_at", desc=True)\
        .limit(10)\
        .execute()
    
    ordenes = res.data or []

    if not ordenes:
        await update.message.reply_text("✅ No hay órdenes pendientes en el tablero.")
        return

    texto = "🎛 <b>PANEL DE CONTROL (ORDENAA DIGITAL)</b>\n\n"
    keyboard = []

    for orden in ordenes:
        po = orden['po_number']
        cliente = orden['customer_name'] or "Desconocido"
        cajas = orden['total_boxes']
        estado = orden.get('workflow_status', 'Draft')
        
        # Icono de estado
        icono = "🔴" if estado == 'Draft' else "🟡" if estado == 'Reviewed' else "🟢"
        
        texto += f"{icono} <b>{po}</b> | {cliente} | {cajas} Cajas\n"
        
        # Botón para "Entrar" a la orden
        keyboard.append([
            InlineKeyboardButton(f"⚙️ Gestionar {po}", callback_data=f"gest_{po}")
        ])

    keyboard.append([InlineKeyboardButton("🔄 Actualizar", callback_data="panel_refresh")])
    
    await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def menu_gestion_orden(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Cuando le das click a una PO, entras aquí.
    """
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "panel_refresh":
        # Lógica para refrescar (volver a llamar al panel)
        await query.message.delete()
        await comando_panel(update, context)
        return

    # Si es gest_PO123
    po_number = data.split("_")[1]
    
    # Traemos detalles
    res_head = db_client.table("sales_orders").select("*").eq("po_number", po_number).execute()
    if not res_head.data:
        await query.edit_message_text("❌ Orden no encontrada.")
        return
        
    orden = res_head.data[0]
    
    # Menú de Acciones para esta Orden
    texto_detalle = (
        f"📦 <b>GESTIÓN DE ORDEN: {po_number}</b>\n"
        f"👤 Cliente: {orden['customer_name']}\n"
        f"📅 Vuelo: {orden.get('ship_date')}\n"
        f"✈️ AWB: {orden.get('awb') or '❌ FALTANTE'}\n"
        f"🏠 HAWB: {orden.get('hawb') or '❌ FALTANTE'}\n"
        f"💰 Valor: ${orden.get('total_value')}\n\n"
        f"¿Qué quieres hacer?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✈️ Asignar AWB/House", callback_data=f"awb_{po_number}")],
        [InlineKeyboardButton("🚜 Ver/Editar Productos", callback_data=f"prod_{po_number}")],
        [InlineKeyboardButton("📄 Generar Factura & PO", callback_data=f"docs_{po_number}")],
        [InlineKeyboardButton("🔙 Volver al Panel", callback_data="panel_refresh")]
    ]
    
    await query.edit_message_text(texto_detalle, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# Aquí necesitaremos lógica para capturar el AWB manual...
