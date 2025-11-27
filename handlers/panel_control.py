"""
panel_control.py
Módulo de Inteligencia Líquida para la gestión de órdenes en Telegram.
Reemplaza la tiranía del Excel ORDENAA con la libertad de Supabase.
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler, CommandHandler, CallbackQueryHandler
from services.supabase_client import supabase  # Asumo que existe y está configurado
from datetime import datetime

# Configuración de Logging con un toque de seriedad
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Estados para conversaciones si fueran necesarias (por ahora manejaremos mucho con callbacks)
SELECTING_ACTION, EDITING_FIELD = range(2)

# Constantes de Paginación
ITEMS_PER_PAGE = 5

async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Punto de entrada: El despertar del panel.
    Muestra las órdenes activas (Status != 'Shipped' o filtro por defecto).
    """
    user = update.effective_user
    logger.info(f"Usuario {user.id} invocando el orden desde el caos.")
    
    # Limpiamos contexto previo
    context.user_data['current_page'] = 0
    context.user_data['filters'] = {"status": "Pending"} # Filtro inicial por defecto
    
    await show_orders_page(update, context)

async def show_orders_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Renderiza una página de la lista de órdenes tipo 'ORDENAA'.
    """
    page = context.user_data.get('current_page', 0)
    filters = context.user_data.get('filters', {})
    
    # 1. Consulta a Supabase (Simulación de query compleja)
    # En producción: services.order_service.get_orders(page, filters)
    try:
        response = supabase.table("staging_orders")\
            .select("*")\
            .order("created_at", desc=True)\
            .range(page * ITEMS_PER_PAGE, (page + 1) * ITEMS_PER_PAGE - 1)\
            .execute()
        
        orders = response.data
    except Exception as e:
        logger.error(f"Error fatal en la matrix de datos: {e}")
        text_method = update.message.reply_text if update.message else update.callback_query.message.reply_text
        await text_method("🔥 Error de conexión con la Inteligencia Líquida.")
        return

    if not orders:
        text = "🍂 No hay órdenes en este limbo (staging) por ahora."
        keyboard = [[InlineKeyboardButton("🔄 Recargar", callback_data="panel_refresh")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        return

    # 2. Construcción de la vista (El 'Render' del Excel)
    keyboard = []
    
    # Encabezado visual (Customer | PO | Vuelo | Status)
    header_text = "📋 *PANEL ORDENAA* \n_Cust | PO# | Vuelo | Status_\n" + "—" * 20 + "\n"
    
    for order in orders:
        # Formateo resiliente ante datos nulos
        cust = (order.get('customer_code') or "???")[:4]
        po = (order.get('po_komet') or "N/A")[-5:] # Últimos 5 chars
        fly = order.get('fly_date') or "Sin Fecha"
        status = order.get('status') or "New"
        
        # Icono de estado
        icon = "🟢" if status == 'Ready' else "🔴" if 'Pending' in status else "⚠️"
        
        btn_text = f"{icon} {cust} | {po} | {fly}"
        callback_data = f"view_order_{order['id']}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])

    # Controles de Paginación
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Anterior", callback_data="page_prev"))
    nav_buttons.append(InlineKeyboardButton("➕ Manual", callback_data="create_manual")) # El Problemita solver
    nav_buttons.append(InlineKeyboardButton("➡️ Siguiente", callback_data="page_next"))
    
    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("🔄 Actualizar Todo", callback_data="panel_refresh")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text=header_text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text=header_text, reply_markup=reply_markup, parse_mode='Markdown')

async def order_details_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Vista detallada de una orden específica. Aquí es donde ocurre la magia de edición.
    """
    query = update.callback_query
    await query.answer()
    
    order_id = query.data.split("_")[-1]
    context.user_data['current_editing_id'] = order_id
    
    # Fetch fresh data
    data = supabase.table("staging_orders").select("*").eq("id", order_id).execute().data[0]
    
    # Renderizado del "Manifiesto de la Orden"
    # Mapeo a las columnas conceptuales solicitadas
    txt = (
        f"📦 *DETALLE DE ORDEN* \n"
        f"🆔 `{data.get('id')}`\n\n"
        f"👤 *Cliente:* {data.get('customer_code')}\n"
        f"🔖 *PO Komet:* `{data.get('po_komet')}`\n"
        f"🔢 *PO Interna:* `{data.get('po_consecutive') or 'Pendiente'}`\n"
        f"✈️ *Vuelo:* {data.get('fly_date') or '⚠️ Definir'}\n"
        f"🏭 *Finca:* {data.get('vendor') or '⚠️ Asignar'}\n"
        f"📄 *Invoice:* `{data.get('invoice_number') or 'NO GENERADA'}`\n"
        f"📦 *Cajas:* {data.get('quantity_boxes')} x {data.get('box_type')}\n"
        f"💐 *Tallos:* {data.get('total_stems')}\n"
        f"💵 *Venta:* ${data.get('unit_price_purchase')} | Costo: ${data.get('pr') or 0}\n"
        f"📝 *Notas:* {data.get('notes')}\n"
        f"🛫 *AWB:* `{data.get('awb') or '---'}`\n"
    )

    # Menú de acciones categorizadas (Clusters)
    keyboard = [
        [
            InlineKeyboardButton("✈️ Logística", callback_data=f"menu_log_{order_id}"),
            InlineKeyboardButton("💰 Finanzas", callback_data=f"menu_fin_{order_id}")
        ],
        [
            InlineKeyboardButton("📦 Empaque", callback_data=f"menu_pack_{order_id}"),
            InlineKeyboardButton("📝 Control/ID", callback_data=f"menu_ctrl_{order_id}")
        ],
        [
             InlineKeyboardButton("📄 Generar PDF Finca", callback_data=f"gen_pdf_farm_{order_id}"),
             InlineKeyboardButton("📑 Facturar Cliente", callback_data=f"gen_inv_client_{order_id}")
        ],
        [InlineKeyboardButton("🔙 Volver al Listado", callback_data="panel_back")]
    ]
    
    await query.edit_message_text(text=txt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def sub_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Manejador genérico para los submenús (Logística, Finanzas, etc.)
    """
    query = update.callback_query
    data = query.data
    order_id = data.split("_")[-1]
    menu_type = data.split("_")[1] # log, fin, pack, ctrl
    
    keyboard = []
    text_prompt = ""
    
    if menu_type == "log":
        text_prompt = "✈️ *Edición Logística*"
        keyboard = [
            [InlineKeyboardButton("Editar Fecha Vuelo", callback_data=f"edit_fly_date_{order_id}")],
            [InlineKeyboardButton("Editar AWB", callback_data=f"edit_awb_{order_id}")],
            [InlineKeyboardButton("Editar HAWB", callback_data=f"edit_hawb_{order_id}")]
        ]
    elif menu_type == "fin":
        text_prompt = "💰 *Edición Financiera*"
        keyboard = [
            [InlineKeyboardButton("Precio Venta", callback_data=f"edit_price_{order_id}")],
            [InlineKeyboardButton("Precio Compra (PR)", callback_data=f"edit_pr_{order_id}")]
        ]
    elif menu_type == "ctrl":
        text_prompt = "📝 *Control e Identificadores*\nGenerar consecutivos irrevocables."
        keyboard = [
            [InlineKeyboardButton("🎲 Asignar PO Consecutivo", callback_data=f"action_gen_po_{order_id}")],
            [InlineKeyboardButton("🔢 Asignar Invoice #", callback_data=f"action_gen_inv_{order_id}")]
        ]

    keyboard.append([InlineKeyboardButton("🔙 Volver a Detalle", callback_data=f"view_order_{order_id}")])
    
    await query.edit_message_text(text=text_prompt, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def manual_creation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    El 'Problemita': Crear orden desde cero (Email vago).
    Aquí inyectamos una fila en blanco inteligente en Supabase y llevamos al usuario a editarla.
    """
    query = update.callback_query
    await query.answer("Iniciando protocolo de emergencia manual...")
    
    # 1. Crear fila vacía con defaults
    new_order = {
        "status": "Manual_Pending",
        "notes": "Creado manualmente desde Telegram",
        "created_at": datetime.now().isoformat()
        # La IA debería sugerir datos aquí en una v2
    }
    
    data = supabase.table("staging_orders").insert(new_order).execute()
    new_id = data.data[0]['id']
    
    # 2. Redirigir al detalle para que edite
    # Hack: Modificamos el callback data para simular que clicó en una orden
    query.data = f"view_order_{new_id}"
    await order_details_handler(update, context)

async def generate_consecutive_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Lógica crítica: Generación de consecutivos (PO o Invoice).
    Reemplaza la hoja INDICES.
    """
    query = update.callback_query
    action_type = query.data.split("_")[2] # 'po' o 'inv'
    order_id = query.data.split("_")[-1]
    
    # Aquí iría la llamada a tu servicio de secuencias (system_sequences)
    # Por ahora simulamos la "inteligencia"
    
    # Fetch current order to get context (Finca, Date)
    order = supabase.table("staging_orders").select("*").eq("id", order_id).execute().data[0]
    
    if action_type == 'po':
        # Lógica: Finca + YYMMDD + / + Seq
        finca = order.get('vendor', 'GEN')[:3]
        date_str = datetime.now().strftime("%y%m%d")
        # TODO: Llamar a DB function get_next_sequence('PO', f"{finca}-{date_str}")
        simulated_seq = "0869" # Simulación
        new_val = f"{finca}{date_str}/{simulated_seq}"
        field = "po_consecutive"
        
    elif action_type == 'inv':
        # Lógica: YYMMDD + / + Seq
        date_str = datetime.now().strftime("%y%m%d")
        # TODO: Llamar a DB function get_next_sequence('INV', date_str)
        simulated_seq = "0790"
        new_val = f"{date_str}/{simulated_seq}"
        field = "invoice_number"

    # Update Supabase
    supabase.table("staging_orders").update({field: new_val}).eq("id", order_id).execute()
    
    await query.answer(f"🔮 Consecutivo generado: {new_val}")
    
    # Refrescar vista
    query.data = f"view_order_{order_id}"
    await order_details_handler(update, context)

async def navigation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manejo de paginación"""
    query = update.callback_query
    current = context.user_data.get('current_page', 0)
    
    if "next" in query.data:
        context.user_data['current_page'] = current + 1
    elif "prev" in query.data and current > 0:
        context.user_data['current_page'] = current - 1
    elif "refresh" in query.data:
        pass # Solo recarga
    elif "back" in query.data:
        # Volver al listado
        pass 
        
    await show_orders_page(update, context)

# --- Dispatcher Setup ---
def register_handlers(application):
    """
    Registra los handlers en la aplicación principal.
    """
    application.add_handler(CommandHandler("panel", cmd_panel))
    application.add_handler(CommandHandler("ordenaa", cmd_panel))
    
    # Callback router: El corazón del flujo
    application.add_handler(CallbackQueryHandler(navigation_handler, pattern="^page_|^panel_"))
    application.add_handler(CallbackQueryHandler(order_details_handler, pattern="^view_order_"))
    application.add_handler(CallbackQueryHandler(sub_menu_handler, pattern="^menu_"))
    application.add_handler(CallbackQueryHandler(manual_creation_handler, pattern="^create_manual"))
    application.add_handler(CallbackQueryHandler(generate_consecutive_handler, pattern="^action_gen_"))
    
    # Aquí faltarían los handlers de edición específicos (ConversationHandler o Input text)
    # Pero por brevedad del prompt, la estructura base está lista.
