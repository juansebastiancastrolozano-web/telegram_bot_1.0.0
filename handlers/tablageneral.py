from telegram import Update
from telegram.ext import ContextTypes
from services.cliente_supabase import db_client, logger
from tabulate import tabulate

async def tablageneral(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handler para consultar contenido crudo de cualquier tabla.
    Refactorizado para usar API REST (Supabase-py) en lugar de SQL directo.
    """
    args = context.args
    
    if not args:
        await update.message.reply_text(
            "⚠️ *Error de Sintaxis*\nPor favor indica la tabla.\nEjemplo: `/tablageneral customers`",
            parse_mode="Markdown"
        )
        return

    nombre_tabla = args[0]
    # Feedback inmediato al usuario
    await update.message.reply_text(f"🔍 Consultando `{nombre_tabla}` via API...", parse_mode="Markdown")

    try:
        # 1. Consulta vía Cliente Oficial (Estable, puerto 443 HTTPS)
        # Limitamos a 15 para no saturar el chat
        response = db_client.table(nombre_tabla).select("*").limit(15).execute()
        
        datos = response.data

        if not datos:
            await update.message.reply_text(f"📭 La tabla `{nombre_tabla}` está vacía o no existe.", parse_mode="Markdown")
            return

        # 2. Procesamiento para Tabulate
        # Obtenemos las columnas dinámicamente del primer registro
        headers = list(datos[0].keys())
        
        # Convertimos la lista de diccionarios a lista de listas
        filas = [[fila.get(col, "") for col in headers] for fila in datos]

        # 3. Formateo Visual
        # Usamos 'simple' o 'plain' para ahorrar caracteres
        tabla_formateada = tabulate(filas, headers, tablefmt="simple")

        mensaje_final = f"```\n{tabla_formateada}\n```"
        
        # 4. Control de Límites de Telegram (4096 caracteres)
        if len(mensaje_final) > 4000:
            mensaje_final = mensaje_final[:3950] + "\n\n... [Truncado por límite de Telegram] ```"
            
        await update.message.reply_text(mensaje_final, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error en tablageneral: {e}")
        # Mensaje de error amigable pero técnico
        await update.message.reply_text(f"❌ Error de Consulta:\n`{str(e)}`", parse_mode="Markdown")
