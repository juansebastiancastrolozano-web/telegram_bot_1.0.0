import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from handlers.lookup import handle_lookup
from handlers.cliente import handle_cliente
from handlers.finca import handle_finca
from handlers.help import handle_help
from handlers.archivos import handle_file
from handlers.tabla import set_tabla
from handlers.tablageneral import tablageneral
import requests

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

# Manejar mensajes normales
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("po", handle_lookup))
    app.add_handler(CommandHandler("cliente", handle_cliente))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("finca", handle_finca))
    app.add_handler(CommandHandler("help", handle_help))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    app.add_handler(CommandHandler("tabla", set_tabla))
    app.add_handler(CommandHandler("tablageneral", tablageneral))
   
   
    print("🤖 Bot iniciado... esperando mensajes.")
    app.run_polling()

