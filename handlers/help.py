from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Tomar todos los handlers registrados por el bot
    handlers = context.application.handlers

    comandos = []

    # Handlers suele organizarse por grupos (0: comandos, 1: mensajes, etc.)
    for grupo in handlers.values():
        for h in grupo:
            if isinstance(h, CommandHandler):
                # Cada CommandHandler puede tener varios comandos
                for cmd in h.commands:
                    comandos.append(f"/{cmd}")

    # Ordenar alfabéticamente
    comandos = sorted(set(comandos))

    texto = "📚 *Lista de comandos disponibles*\n\n"

    for cmd in comandos:
        # puedes definir descripciones más adelante
        texto += f"• `{cmd}`\n"

    texto += (
        "\nMás comandos se añadirán mientras seguimos desarrollando ⚙️🌱\n"
    )

    await update.message.reply_text(texto, parse_mode="Markdown")

