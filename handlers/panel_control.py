from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.cliente_supabase import db_client

# --- ESTADOS Y NAVEGACIÓN ---

async def comando_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Muestra el Tablero Principal (Dashboard).
    Agrupa las órdenes de 'staging_komet' por PO.
    """
    res = db_client.table("staging_komet")\
        .select("*")\
        .neq("status", "Processed")\
        .order("created_at", desc=True)\
        .execute()
    
    data = res.data or []

    if not data:
        await update.message.reply_text("🎉 **Todo limpio.** No hay órdenes pendientes.", parse_mode="Markdown")
        return

    ordenes_unicas = {}
    for row in data:
        po = row['po_komet']
        if po not in ordenes_unicas:
            ordenes_unicas[po] = {
                "cliente": row['customer_code'],
                "items": 0,
                "fecha": row['ship_date'],
                "missing_info": []
            }
        ordenes_unicas[po]["items"] += 1
        if not row.get('awb'): ordenes_unicas[po]["missing_info"].append("AWB")
        if not row.get('sales_price'): ordenes_unicas[po]["missing_info"].append("$$$")

    texto = "🎛 <b>PANEL DE CONTROL (ORDENAA)</b>\n\n"
    keyboard = []

    for po, info in list(ordenes_unicas.items())[:8]:
        faltantes = list(set(info['missing_info']))
        icono = "🟢" if not faltantes else "🔴"
        estado_txt = "LISTO" if not faltantes else f"Falta: {', '.join(faltantes[:2])}"

        texto += f"{icono} <b>{po}</b> | {info['cliente']}\n   └ <i>{estado_txt}</i>\n"
        
        keyboard.append([
            InlineKeyboardButton(f"⚙️ Gestionar {po}", callback_data=f"gest_po_{po}")
        ])

    keyboard.append([InlineKeyboardButton("🔄 Actualizar Tablero", callback_data="panel_refresh")])
    
    if update.callback_query:
        await update.callback_query.edit_message_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    else:
        await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def menu_detalle_orden(update: Update, context: ContextTypes.DEFAULT_TYPE, po_number: str):
    """
    Entras a una orden específica. Muestra menú categorizado.
    """
    query = update.callback_query
    
    # Traemos datos para el resumen
    res = db_client.table("staging_komet").select("*").eq("po_komet", po_number).execute()
    items = res.data
    if not items:
        await query.answer("Esa orden ya no existe.")
        return

    head = items[0]
    
    # Semáforo rápido
    awb_status = "✅" if head.get('awb') else "❌"
    price_status = "✅" if head.get('sales_price') else "❌"
    
    texto_detalle = (
        f"📦 <b>GESTIÓN DE ORDEN: {po_number}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Cliente:</b> {head['customer_code']}\n"
        f"🌺 <b>Items:</b> {len(items)} líneas\n"
        f"✈️ AWB: {awb_status} | 💰 Precio: {price_status}\n\n"
        f"<i>Selecciona una categoría para editar:</i>"
    )
    
    # MENÚ CATEGORIZADO (La solución a las 30 columnas)
    keyboard = [
        [
            InlineKeyboardButton("✈️ Logística (AWB, Vuelo)", callback_data=f"cat_log_{po_number}"),
            InlineKeyboardButton("💰 Precios y Costos", callback_data=f"cat_fin_{po_number}")
        ],
        [
            InlineKeyboardButton("📦 Empaque y Marcas", callback_data=f"cat_pack_{po_number}"),
            InlineKeyboardButton("📝 Notas y Códigos", callback_data=f"cat_notes_{po_number}")
        ],
        [
            InlineKeyboardButton("🚀 APROBAR Y FACTURAR", callback_data=f"approve_{po_number}")
        ],
        [InlineKeyboardButton("🔙 Volver al Panel", callback_data="panel_refresh")]
    ]
    
    await query.edit_message_text(texto_detalle, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def procesar_input_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Captura texto cuando el usuario está editando un campo.
    """
    estado = context.user_data.get('estado_panel') 
    if not estado: return 

    texto_user = update.message.text
    po_target = estado['po']
    campo_db = estado['campo']

    try:
        # Actualizamos TODAS las líneas de esa PO en Staging
        # (Asumimos que AWB, Carrier, etc son compartidos por PO)
        db_client.table("staging_komet").update({campo_db: texto_user}).eq("po_komet", po_target).execute()
        
        await update.message.reply_text(f"✅ Dato actualizado: <b>{texto_user}</b>", parse_mode="HTML")
        context.user_data['estado_panel'] = None
        
        # Botón para volver
        keyboard = [[InlineKeyboardButton(f"🔙 Volver a {po_target}", callback_data=f"gest_po_{po_target}")]]
        await update.message.reply_text("¿Seguimos?", reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        await update.message.reply_text(f"❌ Error guardando: {e}")

# --- ROUTER DEL PANEL ---
async def router_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data == "panel_refresh":
        await comando_panel(update, context)
    
    elif data.startswith("gest_po_"):
        po = data.replace("gest_po_", "")
        await menu_detalle_orden(update, context, po)

    # --- SUB-MENÚS DE CATEGORÍA ---
    elif data.startswith("cat_log_"):
        po = data.replace("cat_log_", "")
        # Mostramos opciones específicas de logística
        keyb = [
            [InlineKeyboardButton("✏️ Editar AWB", callback_data=f"edit_field_awb_{po}")],
            [InlineKeyboardButton("✏️ Editar Carrier", callback_data=f"edit_field_carrier_{po}")],
            [InlineKeyboardButton("✏️ Editar HAWB", callback_data=f"edit_field_hawb_{po}")],
            [InlineKeyboardButton("🔙 Atrás", callback_data=f"gest_po_{po}")]
        ]
        await query.edit_message_text(f"✈️ <b>Logística {po}</b>", reply_markup=InlineKeyboardMarkup(keyb), parse_mode="HTML")

    elif data.startswith("cat_fin_"):
        po = data.replace("cat_fin_", "")
        keyb = [
            [InlineKeyboardButton("💲 Precio Venta", callback_data=f"edit_field_sales_price_{po}")],
            [InlineKeyboardButton("💸 Precio Compra", callback_data=f"edit_field_purchase_price_{po}")],
            [InlineKeyboardButton("🔙 Atrás", callback_data=f"gest_po_{po}")]
        ]
        await query.edit_message_text(f"💰 <b>Finanzas {po}</b>", reply_markup=InlineKeyboardMarkup(keyb), parse_mode="HTML")
    
    # --- LÓGICA DE EDICIÓN GENÉRICA ---
    elif data.startswith("edit_field_"):
        # Formato: edit_field_NOMBRECAMPO_NUMEROPO
        # Ej: edit_field_awb_P12345
        # El truco es separar el PO del campo. Como PO puede tener _, usamos rsplit
        parts = data.replace("edit_field_", "").rsplit("_", 1) 
        if len(parts) != 2: return
        
        campo, po = parts
        
        context.user_data['estado_panel'] = {'campo': campo, 'po': po}
        await query.edit_message_text(f"⌨️ <b>Escribe el nuevo valor para '{campo.upper()}'</b> en la orden {po}:", parse_mode="HTML")

    elif data.startswith("approve_"):
        await query.answer("🚧 Aquí dispararemos la Facturación Masiva...", show_alert=True)
