import os
import logging
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes, 
    CallbackQueryHandler
)
import requests

# --- IMPORTACIONES DE TUS HANDLERS EXISTENTES ---
from handlers.lookup import handle_lookup
from handlers.cliente import handle_cliente
from handlers.finca import handle_finca
from handlers.help import handle_help
from handlers.archivos import handle_file
from handlers.tabla import set_tabla
from handlers.tablageneral import tablageneral

# --- CEREBRO COMERCIAL ---
from handlers.gestion_pedidos import (
    comando_sugerir_pedido, 
    procesar_callback_pedido, 
    recibir_ajuste_precio, 
    comando_rutina_diaria
)
from handlers.facturacion import comando_generar_factura

# --- EL NUEVO ORDEN: PANEL DE CONTROL ---
from handlers.panel_control import comando_panel, router_panel, procesar_input_panel

# Configuración
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌼 <b>Sistema J&G Flowers.</b>\n\n"
        "<b>Comandos:</b>\n"
        "🎛 <code>/panel</code> - Ordenaa Digital\n"
        "📅 <code>/rutina</code> - Oportunidades\n"
        "🔮 <code>/sugerir MEXT</code> - Pedido manual\n"
        "🔎 <code>/po P123</code> - Buscar PO\n\n"
        "<i>Arrastra un Excel para procesar.</i>",
        parse_mode="HTML"
    )

# --- 1. ROUTER GLOBAL DE BOTONES (El Guardián Corregido) ---
async def global_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = str(query.data)

    # LISTA DE INVITADOS ACTUALIZADA:
    # Agregué: 'menu_', 'page_', 'action_', 'create_' que faltaban.
    patrones_panel = [
        "panel_", "view_", "menu_", "page_", "action_", 
        "create_", "edit_", "gest_", "cat_", "approve_"
    ]

    # Si el botón contiene CUALQUIERA de las llaves del panel:
    if any(key in data for key in patrones_panel):
        await router_panel(update, context)
    
    else:
        # Si no, va al flujo antiguo (Sugerencias/Pedidos)
        await procesar_callback_pedido(update, context)

# --- 2. ROUTER GLOBAL DE TEXTO ---
async def handle_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    # A: Usuario editando PRECIO (/sugerir)
    if context.user_data.get('prediccion_activa_id'):
        await recibir_ajuste_precio(update, context)
        return

    # B: Usuario editando CAMPO DEL PANEL (AWB, etc)
    if context.user_data.get('estado_panel'):
        await procesar_input_panel(update, context)
        return

    # C: Default N8N
    await handle_message_n8n(update, context)

# --- 3. CONEXIÓN N8N ---
async def handle_message_n8n(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user = update.message.from_user.username or update.message.from_user.first_name

    msg = await update.message.reply_text("📤 ...")

    try:
        response = requests.post(N8N_WEBHOOK_URL, json={
            "user": user,
            "message": text
        }, timeout=10)

        if response.status_code == 200:
            try:
                data = response.json()
                reply_text = data.get("reply", "🤷‍♂️ Sin respuesta.")
            except:
                reply_text = "⚠️ Error leyendo JSON."
            
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg.message_id)
            await update.message.reply_text(reply_text)
        else:
            await update.message.reply_text(f"⚠️ Error n8n ({response.status_code}).")

    except Exception as e:
        await update.message.reply_text(f"💥 Error: {e}")

if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", handle_help))
    app.add_handler(CommandHandler("po", handle_lookup))
    app.add_handler(CommandHandler("cliente", handle_cliente))
    app.add_handler(CommandHandler("finca", handle_finca))
    app.add_handler(CommandHandler("tabla", set_tabla))
    app.add_handler(CommandHandler("tablageneral", tablageneral))
    
    app.add_handler(CommandHandler("sugerir", comando_sugerir_pedido))
    app.add_handler(CommandHandler("rutina", comando_rutina_diaria))
    app.add_handler(CommandHandler("factura", comando_generar_factura))
    app.add_handler(CommandHandler("panel", comando_panel)) 

    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    app.add_handler(CallbackQueryHandler(global_callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_router))

    print("🤖 J&G Bot Operativo y Corregido.")
    app.run_polling()
