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

# --- NUEVO: PANEL DE CONTROL ---
# Asegúrate de haber creado el archivo handlers/panel_control.py con el código que te di antes
from handlers.panel_control import comando_panel, menu_gestion_orden

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

# --- EL ENRUTADOR DE BOTONES (ROUTER) ---
# Esta función decide a quién enviarle el clic del usuario
async def global_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = str(query.data)

    # 1. Botones del Panel de Control (Empiezan con gest_, awb_, prod_, docs_, panel_)
    if any(data.startswith(p) for p in ["gest_", "awb_", "prod_", "docs_", "panel_"]):
        await menu_gestion_orden(update, context)
    
    # 2. Botones de Gestión de Pedidos / Rutina (aprob_, ajust_, cancel_, auto_)
    else:
        await procesar_callback_pedido(update, context)

# Manejar mensajes normales (CONEXIÓN A N8N)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    # --- 🚦 SEMÁFORO DE PRIORIDAD ---
    # Si el usuario está editando un precio, N8N debe callarse.
    if context.user_data.get('prediccion_activa_id'):
        return 
    # --------------------------------

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

    # --- 1. Comandos Básicos ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", handle_help))

    # --- 2. Consultas ---
    app.add_handler(CommandHandler("po", handle_lookup))
    app.add_handler(CommandHandler("cliente", handle_cliente))
    app.add_handler(CommandHandler("finca", handle_finca))
    app.add_handler(CommandHandler("tabla", set_tabla))
    app.add_handler(CommandHandler("tablageneral", tablageneral))

    # --- 3. Archivos ---
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    # --- 4. INTELIGENCIA COMERCIAL ---
    app.add_handler(CommandHandler("sugerir", comando_sugerir_pedido))
    app.add_handler(CommandHandler("rutina", comando_rutina_diaria))
    app.add_handler(CommandHandler("factura", comando_generar_factura))
    
    # --- 5. NUEVO: PANEL DE CONTROL ---
    app.add_handler(CommandHandler("panel", comando_panel))

    # --- 6. ROUTER DE BOTONES (Reemplaza a los individuales) ---
    app.add_handler(CallbackQueryHandler(global_callback_router))

    # --- 7. HANDLERS DE TEXTO (Prioridad) ---
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, recibir_ajuste_precio), group=1)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🤖 Bot iniciado... esperando mensajes.")
    app.run_polling()
