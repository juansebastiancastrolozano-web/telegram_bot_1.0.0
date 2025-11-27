import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.supabase_client import supabase
from datetime import datetime

logger = logging.getLogger(__name__)

ITEMS_PER_PAGE = 5
TABLE_NAME = "staging_komet" 

# --- 1. COMANDO PRINCIPAL ---
async def comando_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['current_page'] = 0
    context.user_data['estado_panel'] = None 
    context.user_data['current_editing_id'] = None
    await show_orders_page(update, context)

# --- 2. ROUTER DEL PANEL ---
async def router_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() 
    data = query.data
    
    # A. Navegación
    if "page_" in data:
        current_page = context.user_data.get('current_page', 0)
        if "next" in data:
            context.user_data['current_page'] = current_page + 1
        elif "prev" in data and current_page > 0:
            context.user_data['current_page'] = current_page - 1
        await show_orders_page(update, context)
        
    elif data in ["panel_refresh", "panel_back"]:
        context.user_data['estado_panel'] = None
        await show_orders_page(update, context)

    # B. Ver Detalle (El Manifiesto Completo)
    elif data.startswith("view_order_"):
        order_id = data.split("_")[-1]
        context.user_data['current_editing_id'] = order_id
        await show_order_detail(update, context, order_id)

    # C. Submenús
    elif data.startswith("menu_"):
        await show_submenu(update, context, data)

    # D. Edición
    elif data.startswith("edit_"):
        parts = data.split("_")
        field = parts[1]
        order_id = parts[2]
        context.user_data['estado_panel'] = f"editing_{field}"
        context.user_data['editing_id'] = order_id
        
        txt = f"✍️ *Editando {field.upper()}*\n\nEscribe el nuevo valor:"
        await query.edit_message_text(txt, parse_mode='Markdown')

    # E. Acciones
    elif data.startswith("action_"):
        await execute_action(update, context, data)

    # F. Creación Manual
    elif data == "create_manual":
        await create_manual_order(update, context)

# --- 3. PROCESADOR DE INPUT (El Escriba Universal) ---
async def procesar_input_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    estado = context.user_data.get('estado_panel')
    if not estado or not estado.startswith("editing_"):
        return

    text = update.message.text
    order_id = context.user_data.get('editing_id')
    field_alias = estado.split("_")[1]
    
    # MAPA EXPANDIDO: Conectamos los deseos del usuario con las columnas de Supabase
    col_map = {
        # Logística Básica
        'awb': 'awb',
        'hawb': 'hawb',
        'fly': 'fly_date',
        'ship': 'ship_date',
        'cajas': 'quantity_boxes',
        'box': 'box_type',
        'mark': 'mark_code',
        
        # Financiero
        'price': 'unit_price_purchase',
        'pr': 'pr',
        'pcuc': 'pcuc',
        'vc': 'vc',
        'factor': 'factor_1_25',
        'credits': 'credits',
        'sugg': 'suggested_price',
        
        # Identificadores
        'po': 'po_consecutive',
        'inv': 'invoice_number'
    }
    
    db_col = col_map.get(field_alias)
    
    if db_col and order_id:
        try:
            supabase.table(TABLE_NAME).update({db_col: text}).eq("id", order_id).execute()
            await update.message.reply_text(f"✅ *{field_alias.upper()}* mutado a: `{text}`", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error DB: {e}")
    
    context.user_data['estado_panel'] = None
    
    keyboard = [[InlineKeyboardButton("🔙 Volver al Manifiesto", callback_data=f"view_order_{order_id}")]]
    await update.message.reply_text("¿Siguiente movimiento?", reply_markup=InlineKeyboardMarkup(keyboard))

# --- VISTAS ---

async def show_orders_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    page = context.user_data.get('current_page', 0)
    
    try:
        response = supabase.table(TABLE_NAME)\
            .select("id, customer_code, po_komet, fly_date, ship_date, status, product_name")\
            .order("created_at", desc=True)\
            .range(page * ITEMS_PER_PAGE, (page + 1) * ITEMS_PER_PAGE - 1)\
            .execute()
        orders = response.data
    except Exception as e:
        logger.error(f"Error Supabase: {e}")
        msg = f"🔥 Error crítico leyendo {TABLE_NAME}:\n{str(e)}"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    header = f"📋 *PANEL DE CONTROL (Pág {page})*\n\n"
    keyboard = []
    
    if not orders:
        header += "🍂 El vacío nos contempla (Sin órdenes)."
    else:
        for o in orders:
            cust = (o.get('customer_code') or "??")[:4]
            po = (o.get('po_komet') or "NO-PO")[-5:]
            fecha = o.get('fly_date') or o.get('ship_date') or "SinFecha"
            status = o.get('status') or "New"
            
            icon = "🟢" if status == 'Ready' else "🔴" if 'Pending' in status else "⚠️"
            
            btn_txt = f"{icon} {cust} | {po} | {fecha}"
            keyboard.append([InlineKeyboardButton(btn_txt, callback_data=f"view_order_{o['id']}")])

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️", callback_data="page_prev"))
    nav.append(InlineKeyboardButton("➕ Manual", callback_data="create_manual"))
    nav.append(InlineKeyboardButton("🔄", callback_data="panel_refresh"))
    nav.append(InlineKeyboardButton("➡️", callback_data="page_next"))
    keyboard.append(nav)

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(header, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(header, reply_markup=reply_markup, parse_mode='Markdown')

async def show_order_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, order_id: str):
    try:
        data = supabase.table(TABLE_NAME).select("*").eq("id", order_id).execute().data[0]
    except Exception as e:
        await update.callback_query.edit_message_text(f"❌ La orden se ha disuelto en la nada: {e}")
        return

    # --- EXTRACCIÓN DE LA VERDAD ---
    def g(key, default="---"): return str(data.get(key) or default)
    def money(key): return str(data.get(key) or 0)

    # Bloque Identidad
    po_int = g('po_consecutive', '⚠️ PENDIENTE')
    inv = g('invoice_number', 'NO GENERADA')
    
    # Bloque Producto
    cajas_txt = f"{g('quantity_boxes')} (Conf: {g('confirmed_boxes')})"
    stems = g('total_stems', 0)
    
    # Bloque Financiero Complejo
    # [PCUC | VC | PR]
    fin_row1 = f"PCUC: {money('pcuc')} | VC: {money('vc')} | PR: {money('pr')}"
    # [Fac 1.25 | Valor T | Sugerido]
    fin_row2 = f"F.1.25: {money('factor_1_25')} | Val T: {money('valor_t')}"
    fin_row3 = f"Sug: {money('suggested_price')} | Venta: {money('unit_price_purchase')}"
    fin_row4 = f"Créditos: {money('credits')} | Cash: {money('cash_payment')}"

    txt = (
        f"📦 *MANIFIESTO DE ORDEN* `{g('po_komet')}`\n"
        f"👤 *Cliente:* {g('customer_code')} ({g('status_komet')})\n"
        f"🏷️ *Prod:* {g('product_name')}\n"
        f"📦 *Pack:* {g('box_type')} | Marca: {g('mark_code')}\n"
        f"📊 *Cant:* {cajas_txt} Cajas | {stems} Tallos\n"
        f"📍 *Origen:* {g('origin')} | Finca: {g('vendor')}\n\n"
        
        f"✈️ *LOGÍSTICA*\n"
        f"Ship: {g('ship_date')} | Fly: {g('fly_date')}\n"
        f"AWB: `{g('awb')}`\n"
        f"HAWB: `{g('hawb')}`\n"
        f"UDV: {g('udv')}\n\n"
        
        f"💰 *INGENIERÍA FINANCIERA*\n"
        f"{fin_row3}\n"
        f"{fin_row1}\n"
        f"{fin_row2}\n"
        f"{fin_row4}\n\n"
        
        f"📝 *CONTROL*\n"
        f"PO Int: `{po_int}`\n"
        f"Invoice: `{inv}`\n"
        f"Farm Inv: `{g('farm_invoice')}`\n"
        f"Notas: _{g('notes')}_"
    )

    keyboard = [
        [
            InlineKeyboardButton("✈️ Logística", callback_data=f"menu_log_{order_id}"),
            InlineKeyboardButton("💰 Finanzas", callback_data=f"menu_fin_{order_id}")
        ],
        [
            InlineKeyboardButton("📄 Documentos", callback_data=f"menu_docs_{order_id}"),
            InlineKeyboardButton("🔙 Volver", callback_data="panel_back")
        ]
    ]
    await update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_submenu(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    parts = data.split("_")
    menu_type = parts[1]
    order_id = parts[2]
    
    txt = f"⚙️ *Ajuste de Tuercas: {menu_type.upper()}*"
    keyboard = []
    
    if menu_type == "log":
        keyboard = [
            [InlineKeyboardButton("✏️ AWB", callback_data=f"edit_awb_{order_id}"), 
             InlineKeyboardButton("✏️ HAWB", callback_data=f"edit_hawb_{order_id}")],
            [InlineKeyboardButton("✏️ Fly Date", callback_data=f"edit_fly_{order_id}"),
             InlineKeyboardButton("✏️ Ship Date", callback_data=f"edit_ship_{order_id}")],
            [InlineKeyboardButton("✏️ Tipo Caja", callback_data=f"edit_box_{order_id}"),
             InlineKeyboardButton("✏️ Marca", callback_data=f"edit_mark_{order_id}")]
        ]
    elif menu_type == "fin":
        keyboard = [
            [InlineKeyboardButton("✏️ Precio Venta", callback_data=f"edit_price_{order_id}"),
             InlineKeyboardButton("✏️ PR (Costo)", callback_data=f"edit_pr_{order_id}")],
            [InlineKeyboardButton("✏️ PCUC", callback_data=f"edit_pcuc_{order_id}"),
             InlineKeyboardButton("✏️ VC", callback_data=f"edit_vc_{order_id}")],
            [InlineKeyboardButton("✏️ Créditos", callback_data=f"edit_credits_{order_id}"),
             InlineKeyboardButton("✏️ Factor 1.25", callback_data=f"edit_factor_{order_id}")]
        ]
    elif menu_type == "docs":
        keyboard = [
            [InlineKeyboardButton("🎲 Generar PO#", callback_data=f"action_genpo_{order_id}")],
            [InlineKeyboardButton("📑 Generar INV#", callback_data=f"action_geninv_{order_id}")]
        ]

    keyboard.append([InlineKeyboardButton("🔙 Volver al Manifiesto", callback_data=f"view_order_{order_id}")])
    
    await update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def execute_action(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    action = data.split("_")[1]
    order_id = data.split("_")[2]
    
    new_val = ""
    col = ""
    
    if action == "genpo":
        # TODO: Conectar con tabla 'system_sequences' real
        new_val = f"PO-{datetime.now().strftime('%m%d%H%M')}"
        col = "po_consecutive"
    elif action == "geninv":
        new_val = f"INV-{datetime.now().strftime('%m%d')}-001"
        col = "invoice_number"
        
    try:
        supabase.table(TABLE_NAME).update({col: new_val}).eq("id", order_id).execute()
        await update.callback_query.answer(f"✅ Realidad alterada: {new_val}")
        await show_order_detail(update, context, order_id)
    except Exception as e:
        await update.callback_query.answer("❌ Error en la matrix")

async def create_manual_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_row = {
            "status": "Manual_Pending",
            "notes": "Génesis manual desde Telegram",
            "created_at": datetime.now().isoformat()
        }
        res = supabase.table(TABLE_NAME).insert(new_row).execute()
        new_id = res.data[0]['id']
        context.user_data['current_editing_id'] = new_id
        await show_order_detail(update, context, new_id)
    except Exception as e:
        logger.error(f"Error manual: {e}")
        if update.callback_query:
            await update.callback_query.answer("❌ Error creando orden.")
