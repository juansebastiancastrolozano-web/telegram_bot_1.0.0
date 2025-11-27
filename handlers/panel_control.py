import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.supabase_client import supabase
from datetime import datetime

# Configuración de Logging
logger = logging.getLogger(__name__)

# Constantes
ITEMS_PER_PAGE = 5

# --- 1. COMANDO PRINCIPAL (/panel) ---
async def comando_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Punto de entrada: Inicializa el panel y muestra la primera página.
    """
    # Limpiamos estados previos para evitar conflictos
    context.user_data['current_page'] = 0
    context.user_data['estado_panel'] = None 
    context.user_data['current_editing_id'] = None
    
    # Invocamos la vista de listado
    await show_orders_page(update, context)

# --- 2. ROUTER DEL PANEL (El Cerebro de Navegación) ---
async def router_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Distribuye los clics de los botones del panel a su función correspondiente.
    """
    query = update.callback_query
    await query.answer() # Confirmar a Telegram que recibimos el clic
    
    data = query.data
    
    # A. Navegación y Listados
    if "page_" in data:
        current_page = context.user_data.get('current_page', 0)
        if "next" in data:
            context.user_data['current_page'] = current_page + 1
        elif "prev" in data and current_page > 0:
            context.user_data['current_page'] = current_page - 1
        await show_orders_page(update, context)
        
    elif data == "panel_refresh" or data == "panel_back":
        # Volver al inicio / Recargar
        context.user_data['estado_panel'] = None
        await show_orders_page(update, context)

    # B. Ver Detalle de Orden
    elif data.startswith("view_order_"):
        order_id = data.split("_")[-1]
        context.user_data['current_editing_id'] = order_id
        await show_order_detail(update, context, order_id)

    # C. Submenús (Logística, Finanzas, etc.)
    elif data.startswith("menu_"):
        await show_submenu(update, context, data)

    # D. Activar Edición (Pone al bot a escuchar texto)
    elif data.startswith("edit_"):
        # data ej: edit_awb_UUID
        parts = data.split("_")
        field = parts[1] # awb, hawb, price
        order_id = parts[2]
        
        # Guardamos qué estamos editando
        context.user_data['estado_panel'] = f"editing_{field}"
        context.user_data['editing_id'] = order_id
        
        txt = f"✍️ *Editando {field.upper()}*\n\nPor favor, escribe el nuevo valor:"
        await query.edit_message_text(txt, parse_mode='Markdown')

    # E. Acciones Ejecutables (Generar cosas)
    elif data.startswith("action_"):
        await execute_action(update, context, data)

    # F. Creación Manual
    elif data == "create_manual":
        await create_manual_order(update, context)

# --- 3. PROCESADOR DE INPUT DE TEXTO (Cuando el usuario escribe) ---
async def procesar_input_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Captura el texto escrito por el usuario si está en modo edición del panel.
    """
    estado = context.user_data.get('estado_panel')
    if not estado or not estado.startswith("editing_"):
        return

    text = update.message.text
    order_id = context.user_data.get('editing_id')
    field_alias = estado.split("_")[1] # awb, hawb, price...
    
    # Mapeo de alias a columnas reales de Supabase
    col_map = {
        'awb': 'awb',
        'hawb': 'hawb',
        'fly': 'fly_date',
        'price': 'unit_price_purchase',
        'pr': 'pr',
        'cajas': 'quantity_boxes'
    }
    
    db_col = col_map.get(field_alias)
    
    if db_col and order_id:
        try:
            # Actualizamos Supabase
            supabase.table("staging_orders").update({db_col: text}).eq("id", order_id).execute()
            await update.message.reply_text(f"✅ *{field_alias.upper()}* actualizado a: `{text}`", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error guardando en BD: {e}")
    
    # Reseteamos estado y volvemos al detalle
    context.user_data['estado_panel'] = None
    
    # Truco: Mostramos de nuevo el detalle para seguir trabajando
    # (Necesitamos volver a invocar show_order_detail, pero requiere update distinto)
    # Por simplicidad, enviamos un botón para volver
    keyboard = [[InlineKeyboardButton("🔙 Volver a la Orden", callback_data=f"view_order_{order_id}")]]
    await update.message.reply_text("¿Qué más deseas hacer?", reply_markup=InlineKeyboardMarkup(keyboard))

# --- FUNCIONES AUXILIARES (VISTAS) ---

async def show_orders_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    page = context.user_data.get('current_page', 0)
    
    try:
        # Consulta a Supabase
        response = supabase.table("staging_orders")\
            .select("id, customer_code, po_komet, fly_date, status")\
            .order("created_at", desc=True)\
            .range(page * ITEMS_PER_PAGE, (page + 1) * ITEMS_PER_PAGE - 1)\
            .execute()
        orders = response.data
    except Exception as e:
        logger.error(f"Error Supabase: {e}")
        txt = "🔥 Error conectando a la Inteligencia Líquida (Supabase)."
        if update.callback_query:
            await update.callback_query.edit_message_text(txt)
        else:
            await update.message.reply_text(txt)
        return

    # Construir listado
    header = f"📋 *PANEL ORDENAA (Pág {page})*\n\n"
    keyboard = []
    
    if not orders:
        header += "🍂 No hay órdenes en el horizonte."
    else:
        for o in orders:
            cust = (o.get('customer_code') or "???")[:5]
            po = (o.get('po_komet') or "NO-PO")[-5:]
            fly = o.get('fly_date') or "SinFecha"
            status = o.get('status') or "New"
            
            icon = "🟢" if status == 'Ready' else "🔴"
            btn_txt = f"{icon} {cust} | {po} | {fly}"
            keyboard.append([InlineKeyboardButton(btn_txt, callback_data=f"view_order_{o['id']}")])

    # Botones de navegación
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
    # Fetch full data
    try:
        data = supabase.table("staging_orders").select("*").eq("id", order_id).execute().data[0]
    except IndexError:
        await update.callback_query.edit_message_text("❌ Orden no encontrada.")
        return

    txt = (
        f"📦 *DETALLE ORDEN* `{data.get('po_komet')}`\n"
        f"👤 *Cliente:* {data.get('customer_code')}\n"
        f"🔢 *PO Interna:* `{data.get('po_consecutive') or '---'}`\n"
        f"✈️ *Vuelo:* {data.get('fly_date') or '---'}\n"
        f"🛫 *AWB:* `{data.get('awb') or '---'}`\n"
        f"🏭 *Finca:* {data.get('vendor') or '---'}\n"
        f"💰 *Venta:* ${data.get('unit_price_purchase') or 0} | *Costo:* ${data.get('pr') or 0}\n"
        f"📄 *Invoice:* `{data.get('invoice_number') or 'NO GENERADA'}`"
    )

    keyboard = [
        [
            InlineKeyboardButton("✈️ Logística", callback_data=f"menu_log_{order_id}"),
            InlineKeyboardButton("💰 Finanzas", callback_data=f"menu_fin_{order_id}")
        ],
        [
            InlineKeyboardButton("📄 Generar Docs", callback_data=f"menu_docs_{order_id}"),
            InlineKeyboardButton("🔙 Volver", callback_data="panel_back")
        ]
    ]
    await update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def show_submenu(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    parts = data.split("_")
    menu_type = parts[1]
    order_id = parts[2]
    
    txt = f"⚙️ *Menú {menu_type.upper()}*"
    keyboard = []
    
    if menu_type == "log":
        keyboard = [
            [InlineKeyboardButton("✏️ Editar AWB", callback_data=f"edit_awb_{order_id}")],
            [InlineKeyboardButton("✏️ Editar Fecha Vuelo", callback_data=f"edit_fly_{order_id}")]
        ]
    elif menu_type == "fin":
        keyboard = [
            [InlineKeyboardButton("✏️ Precio Venta", callback_data=f"edit_price_{order_id}")],
            [InlineKeyboardButton("✏️ Precio Compra (PR)", callback_data=f"edit_pr_{order_id}")]
        ]
    elif menu_type == "docs":
        keyboard = [
            [InlineKeyboardButton("🎲 Generar PO# Consec", callback_data=f"action_genpo_{order_id}")],
            [InlineKeyboardButton("📑 Generar Invoice#", callback_data=f"action_geninv_{order_id}")]
        ]

    keyboard.append([InlineKeyboardButton("🔙 Volver al Detalle", callback_data=f"view_order_{order_id}")])
    
    await update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def execute_action(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    action = data.split("_")[1] # genpo, geninv
    order_id = data.split("_")[2]
    
    # Simulación de generación de consecutivos (Aquí conectarás tu lógica real luego)
    new_val = ""
    col = ""
    
    if action == "genpo":
        new_val = f"PO-{datetime.now().strftime('%m%d')}-X"
        col = "po_consecutive"
    elif action == "geninv":
        new_val = f"INV-{datetime.now().strftime('%m%d')}-99"
        col = "invoice_number"
        
    try:
        supabase.table("staging_orders").update({col: new_val}).eq("id", order_id).execute()
        await update.callback_query.answer(f"✅ Generado: {new_val}")
        # Recargamos la vista detalle
        await show_order_detail(update, context, order_id)
    except Exception as e:
        await update.callback_query.answer("❌ Error al generar")

async def create_manual_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Insertar fila vacía
        new_row = {"status": "Manual", "created_at": datetime.now().isoformat()}
        res = supabase.table("staging_orders").insert(new_row).execute()
        new_id = res.data[0]['id']
        # Ir a editarla
        context.user_data['current_editing_id'] = new_id
        await show_order_detail(update, context, new_id)
    except Exception as e:
        logger.error(f"Error creating manual: {e}")
