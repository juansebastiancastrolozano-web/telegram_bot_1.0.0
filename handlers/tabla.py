# handlers/tabla.py

user_tablas = {}

async def set_tabla(update, context):
    if not context.args:
        await update.message.reply_text("Indica la tabla: /tabla price_list")
        return

    tabla = context.args[0]
    user_id = update.message.from_user.id

    user_tablas[user_id] = tabla
    await update.message.reply_text(f"✔️ Tabla seleccionada: {tabla}")
