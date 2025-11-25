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
# Importamos las funciones del cerebro comercial
from handlers.gestion_pedidos import comando_sugerir_pedido, procesar_callback_pedido, recibir_ajuste_precio

# Cargar variables del entorno
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

# Comando /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌼 Bienvenido al bot de J&G Flowers.\n"
        "Envía un mensaje o usa /po <codigo> para interactuar con n8n."
    )

# Manejar mensajes normales (CONEXIÓN A N8N)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    # --- 🚦 SEMÁFORO DE PRIORIDAD ---
    # Si el usuario está en "Modo Edición" (tiene un ID de predicción pendiente),
    # este handler se CALLA y deja que 'recibir_ajuste_precio' haga el trabajo.
    if context.user_data.get('prediccion_activa_id'):
        return  # Salimos inmediatamente, ignorando este mensaje para n8n
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
            # Intentar leer JSON devuelto por n8n
            try:
                data = response.json()
                reply_text = data.get("reply", "🤷‍♂️ n8n no envió respuesta.")
            except:
                reply_text = "⚠️ Recibí algo, pero no pude leer la respuesta JSON."

            await update.message.reply_text(reply_text)

        else:
            await update.message.reply_text(
                f"⚠️ Error al conectar con n8n ({response.status_code})."
            )

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

    # --- 4. INTELIGENCIA COMERCIAL (NUEVO) ---
    app.add_handler(CommandHandler("sugerir", comando_sugerir_pedido))
    app.add_handler(CallbackQueryHandler(procesar_callback_pedido))

    # --- 5. IMPORTANTE: HANDLERS DE TEXTO ---
    
    # A) Handler de AJUSTE DE PRECIO (Grupo 1 - Prioridad paralela)
    # Este capturará el precio cuando el usuario esté en modo edición
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, recibir_ajuste_precio), group=1)

    # B) Handler de N8N (Grupo 0 - Default)
    # Este capturará todo lo demás (porque tiene el semáforo adentro)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🤖 Bot iniciado... esperando mensajes.")
    app.run_polling()
