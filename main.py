import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import requests

# --- IMPORTACIONES DE TUS HANDLERS ---
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

# --- NUEVO: PANEL DE CONTROL (ORDENAA DIGITAL) ---
from handlers.panel_control import comando_panel, router_panel, procesar_input_panel

# Cargar variables del entorno
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

# Comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌼 <b>Bienvenido al Sistema J&G Flowers.</b>\n\n"
        "<b>Comandos Operativos:</b>\n"
        "🎛 <code>/panel</code> - Tablero de Control (Ordenaa Digital)\n"
        "📅 <code>/rutina</code> - Oportunidades del día\n"
        "🔮 <code>/sugerir MEXT</code> - Crear pedido manual\n"
        "🔎 <code>/po P123</code> - Buscar orden\n\n"
        "<i>Arrastra un Excel de Komet o OPBASE para procesarlo.</i>",
        parse_mode="HTML"
    )

# --- 1. ROUTER GLOBAL DE BOTONES ---
# Decide a qué parte del cerebro va el clic
async def global_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = str(query.data)

    # Si el botón pertenece al PANEL (gestionar, categorías, editar campo, aprobar)
    # Agregamos 'cat_' que es el prefijo de las categorías nuevas
    if any(x in data for x in ["gest_po_", "edit_", "panel_", "approve_", "cat_", "view_"]):
        await router_panel(update, context)
    
    # Si no, asumimos que es del flujo de SUGERIR (aprob_, ajust_, cancel_)
    else:
        await procesar_callback_pedido(update, context)

# --- 2. ROUTER GLOBAL DE TEXTO ---
# Decide quién procesa lo que escribes
async def handle_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    # Prioridad A: Usuario editando PRECIO (Flujo /sugerir)
    if context.user_data.get('prediccion_activa_id'):
        await recibir_ajuste_precio(update, context)
        return

    # Prioridad B: Usuario editando AWB/CARRIER (Flujo /panel)
    if context.user_data.get('estado_panel'):
        await procesar_input_panel(update, context)
        return

    # Prioridad C: Chat normal con N8N (Default)
    await handle_message_n8n(update, context)

# --- 3. CONEXIÓN N8N (La función original renombrada) ---
async def handle_message_n8n(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user = update.message.from_user.username or update.message.from_user.first_name

    await update.message.reply_text("📤 Enviando tu mensaje a n8n...")

    try:
        response = requests.post(N8N_WEBHOOK_URL, json={
            "user": user,
            "message": text
        })

        if response.status_code == 200:
            try:
                data = response.json()
                reply_text = data.get("reply", "🤷‍♂️ n8n no envió respuesta.")
            except:
                reply_text = "⚠️ Recibí algo, pero no pude leer la respuesta JSON."
            await update.message.reply_text(reply_text)
        else:
            await update.message.reply_text(f"⚠️ Error n8n ({response.status_code}).")

    except Exception as e:
        await update.message.reply_text(f"💥 Error inesperado: {e}")

# Iniciar el bot
if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # --- Comandos ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", handle_help))
    app.add_handler(CommandHandler("po", handle_lookup))
    app.add_handler(CommandHandler("cliente", handle_cliente))
    app.add_handler(CommandHandler("finca", handle_finca))
    app.add_handler(CommandHandler("tabla", set_tabla))
    app.add_handler(CommandHandler("tablageneral", tablageneral))
    
    # --- Módulos Nuevos ---
    app.add_handler(CommandHandler("sugerir", comando_sugerir_pedido))
    app.add_handler(CommandHandler("rutina", comando_rutina_diaria))
    app.add_handler(CommandHandler("factura", comando_generar_factura))
    app.add_handler(CommandHandler("panel", comando_panel)) # <--- ¡AQUÍ ESTÁ!

    # --- Archivos ---
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    # --- Routers Maestros (Botones y Texto) ---
    app.add_handler(CallbackQueryHandler(global_callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_router))

    print("🤖 Bot iniciado... esperando mensajes.")
    app.run_polling()
