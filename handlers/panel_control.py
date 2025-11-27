from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.cliente_supabase import db_client

async def comando_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Muestra las órdenes agrupadas por PO.
    """
    res = db_client.table("staging_komet")\
        .select("*")\
        .neq("status", "Processed")\
        .order("created_at", desc=True)\
        .execute()
    
    data = res.data or []

    if not data:
        await update.message.reply_text("🎉 Bandeja vacía. Todo procesado.", parse_mode="Markdown")
        return

    # Agrupar por PO
    ordenes = {}
    for row in data:
        po = row['po_komet']
        if po not in ordenes:
            ordenes[po] = {"cliente": row['customer_code'], "items": 0, "missing": []}
        ordenes[po]["items"] += 1
        if not row.get('awb'): ordenes[po]["missing"].append("AWB")
        if not row.get('sales_price'): ordenes[po]["missing"].append("$$")

    texto = "🎛 <b>PANEL ORDENAA</b>\n\n"
    keyboard = []

    for po, info in list(ordenes.items())[:8]:
        estado = "✅ Listo" if not info['missing'] else f"⚠️ Falta: {', '.join(info['missing'][:2])}"
        texto += f"🔹 <b>{po}</b> ({info['cliente']}) - {estado}\n"
        keyboard.append([InlineKeyboardButton(f"⚙️ {po}", callback_data=f"gest_po_{po}")])

    keyboard.append([InlineKeyboardButton("🔄 Actualizar", callback_data="panel_refresh")])
    
    msg = await (update.callback_query.edit_message_text if update.callback_query else update.message.reply_text)
    await msg(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def menu_detalle_orden(update: Update, context: ContextTypes.DEFAULT_TYPE, po_number: str):
    query = update.callback_query
    
    # Traemos un ítem de muestra para ver qué falta
    res = db_client.table("staging_komet").select("*").eq("po_komet", po_number).limit(1).execute()
    if not res.data: return
    head = res.data[0]

    txt = (
        f"📦 <b>PO: {po_number}</b>\n"
        f"👤 Cliente: {head.get('customer_code')}\n"
        f"✈️ AWB: {head.get('awb') or '---'}\n"
        f"💰 Venta: ${head.get('sales_price') or 0}\n"
        f"📊 PCUC: {head.get('pcuc') or 0} | VC: {head.get('vc') or 0}\n"
    )

    kb = [
        [InlineKeyboardButton("✈️ Logística (AWB, Vuelo)", callback_data=f"cat_log_{po_number}")],
        [InlineKeyboardButton("💰 Finanzas (Precios, PCUC, VC)", callback_data=f"cat_fin_{po_number}")],
        [InlineKeyboardButton("📝 Control (Invoice, Consec)", callback_data=f"cat_ctrl_{po_number}")],
        [InlineKeyboardButton("🚀 PROCESAR FINAL", callback_data=f"approve_{po_number}")],
        [InlineKeyboardButton("🔙 Volver", callback_data="panel_refresh")]
    ]
    await query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

async def router_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if "panel_refresh" in data: await comando_panel(update, context)
    elif "gest_po_" in data: await menu_detalle_orden(update, context, data.replace("gest_po_", ""))
    
    # --- SUBMENÚS DE EDICIÓN ---
    elif "cat_log_" in data:
        po = data.replace("cat_log_", "")
        kb = [
            [InlineKeyboardButton("✏️ AWB", callback_data=f"edit_awb_{po}"), InlineKeyboardButton("✏️ HAWB", callback_data=f"edit_hawb_{po}")],
            [InlineKeyboardButton("✏️ Carrier", callback_data=f"edit_carrier_{po}"), InlineKeyboardButton("✏️ FlyDate", callback_data=f"edit_fly_date_{po}")],
            [InlineKeyboardButton("🔙 Atrás", callback_data=f"gest_po_{po}")]
        ]
        await query.edit_message_text(f"✈️ <b>Logística {po}</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

    elif "cat_fin_" in data:
        po = data.replace("cat_fin_", "")
        kb = [
            [InlineKeyboardButton("💲 Precio Venta", callback_data=f"edit_sales_price_{po}"), InlineKeyboardButton("💸 Precio Compra", callback_data=f"edit_unit_price_purchase_{po}")],
            [InlineKeyboardButton("📊 PCUC", callback_data=f"edit_pcuc_{po}"), InlineKeyboardButton("📊 VC", callback_data=f"edit_vc_{po}")],
            [InlineKeyboardButton("1️⃣.2️⃣5️⃣ Factor", callback_data=f"edit_factor_1_25_{po}"), InlineKeyboardButton("🔙 Atrás", callback_data=f"gest_po_{po}")]
        ]
        await query.edit_message_text(f"💰 <b>Finanzas {po}</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

    elif "cat_ctrl_" in data:
        po = data.replace("cat_ctrl_", "")
        kb = [
            [InlineKeyboardButton("#️⃣ Invoice", callback_data=f"edit_invoice_number_{po}"), InlineKeyboardButton("🔢 Consecutivo", callback_data=f"edit_consecutive_{po}")],
            [InlineKeyboardButton("🔙 Atrás", callback_data=f"gest_po_{po}")]
        ]
        await query.edit_message_text(f"📝 <b>Control {po}</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

    # --- CAPTURA DE EDICIÓN ---
    elif data.startswith("edit_"):
        # Formato: edit_CAMPO_PO
        # Ejemplo: edit_awb_P12345
        # Truco: Split solo en el primer 'edit_' y el último '_' para sacar el PO
        parts = data.replace("edit_", "").rsplit("_", 1)
        campo = parts[0] # 'awb', 'pcuc', 'sales_price'
        po = parts[1]    # 'P12345'
        
        context.user_data['estado_panel'] = {'campo': campo, 'po': po}
        await query.edit_message_text(f"⌨️ Escribe el nuevo valor para <b>{campo.upper()}</b>:", parse_mode="HTML")

async def procesar_input_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    estado = context.user_data.get('estado_panel')
    if not estado: return

    valor = update.message.text
    po = estado['po']
    campo = estado['campo']

    # Guardar en DB (Actualiza toda la PO)
    db_client.table("staging_komet").update({campo: valor}).eq("po_komet", po).execute()
    
    await update.message.reply_text(f"✅ <b>{campo}</b> actualizado a: {valor}", parse_mode="HTML")
    context.user_data['estado_panel'] = None
    
    # Botón para volver
    kb = [[InlineKeyboardButton(f"🔙 Volver a {po}", callback_data=f"gest_po_{po}")]]
    await update.message.reply_text("¿Seguimos?", reply_markup=InlineKeyboardMarkup(kb))
