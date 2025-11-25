# handlers/gestion_pedidos.py
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.motor_ventas import GestorPrediccionVentas

# Instanciamos el servicio de negocio
gestor_ventas = GestorPrediccionVentas()

async def comando_sugerir_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando: /sugerir <CODIGO_CLIENTE>
    Inicia el flujo de recomendación de orden basado en inteligencia comercial.
    """
    user = update.effective_user

    if not context.args:
        await update.message.reply_text(
            "⚠️ *Error de Sintaxis*\nPor favor ingrese el código del cliente.\nEjemplo: `/sugerir MEXT`",
            parse_mode="Markdown"
        )
        return

    codigo_cliente = context.args[0].upper().strip()
    await update.message.reply_text(f"📊 Analizando historial comercial para: *{codigo_cliente}*...", parse_mode="Markdown")

    # Invocación al servicio
    pred_id, sugerencia = gestor_ventas.generar_sugerencia_pedido(codigo_cliente)

    if not pred_id:
        error_msg = sugerencia.get("error", "Error desconocido")
        await update.message.reply_text(f"❌ No se pudo generar sugerencia: {error_msg}")
        return

    # Construcción de la respuesta formal
    texto_respuesta = (
        f"📋 *Resumen de Oportunidad Comercial*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 **Cliente:** {sugerencia.get('cliente_nombre', codigo_cliente)}\n"
        f"📈 **Estrategia:** {sugerencia['estrategia_aplicada']}\n"
        f"📝 **Análisis:** {sugerencia['justificacion_tecnica']}\n\n"
        f"🌺 **Producto Sugerido:** {sugerencia['producto_objetivo']}\n"
        f"💵 **Precio Objetivo:** ${sugerencia['precio_unitario']} USD\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"¿Cómo desea proceder con esta orden?"
    )

    # Botones de Acción
    keyboard = [
        [
            InlineKeyboardButton("✅ Confirmar Orden", callback_data=f"aprob_{pred_id}"),
            InlineKeyboardButton("📝 Ajustar Precio", callback_data=f"ajust_{pred_id}")
        ],
        [
            InlineKeyboardButton("❌ Descartar", callback_data=f"cancel_{pred_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(texto_respuesta, reply_markup=reply_markup, parse_mode="Markdown")

async def procesar_callback_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Manejador de eventos para los botones inline.
    """
    query = update.callback_query
    await query.answer() # Confirmar recepción del evento

    data = query.data
    accion, pred_id = data.split("_")

    if accion == "aprob":
        # Aquí se integraría con el sistema de generación de PO real
        await query.edit_message_text(
            f"✅ **Orden Confirmada** (Ref: {pred_id})\n"
            f"El pedido ha sido enviado a la cola de procesamiento logístico.",
            parse_mode="Markdown"
        )
        # TODO: Trigger n8n webhook for PO generation

    elif accion == "ajust":
        # Guardamos el ID en el contexto del usuario para esperar su input numérico
        context.user_data['prediccion_activa_id'] = pred_id
        print(f"📝 DEBUG: Modo edición activado para Pred ID: {pred_id}")
        await query.edit_message_text(
            f"📝 **Modo de Edición de Precio**\n\n"
            f"Por favor, ingrese el *Precio Unitario Real* de cierre (Ej: 0.38):",
            parse_mode="Markdown"
        )

    elif accion == "cancel":
        await query.edit_message_text("❌ Operación cancelada por el usuario.")

async def recibir_ajuste_precio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Captura el input de texto del usuario cuando está en modo de ajuste.
    Ahora con DEPURACIÓN RUIDOSA.
    """
    # 1. DEBUG: Imprimimos en la terminal qué está pasando
    user_msg = update.message.text
    print(f"🔍 DEBUG: Mensaje recibido: '{user_msg}'")
    
    # Intentamos recuperar el estado
    pred_id = context.user_data.get('prediccion_activa_id')
    print(f"🔍 DEBUG: Memoria del usuario (user_data): {context.user_data}")

    # 2. MANEJO DE SESIÓN PERDIDA
    if not pred_id:
        print("❌ DEBUG: No se encontró ID de predicción activo. Ignorando mensaje (o manejando como chat normal).")
        # Opcional: Si parece un precio, avisamos que la sesión expiró
        if user_msg.replace(".", "").replace(",", "").isdigit():
             await update.message.reply_text(
                 "⚠️ **Sesión de edición expirada.**\n"
                 "Por favor, vuelve a presionar el botón '📝 Ajustar Precio' arriba para reactivar la edición.",
                 parse_mode="Markdown"
             )
        return 

    texto_input = update.message.text.strip()

    # 3. Validación de tipo de dato
    try:
        # Reemplazamos coma por punto para decimales latinos/europeos
        precio_real = float(texto_input.replace(",", "."))
    except ValueError:
        await update.message.reply_text("⚠️ Eso no parece un precio válido. Ingresa solo el número (ej. 0.45).")
        return

    print(f"✅ DEBUG: Guardando ajuste para ID {pred_id} con precio {precio_real}")

    # 4. Registro en base de datos
    exito = gestor_ventas.registrar_ajuste_usuario(pred_id, precio_real)

    if exito:
        await update.message.reply_text(
            f"💾 **Ajuste Registrado**\n"
            f"Nuevo precio: ${precio_real} USD.\n"
            f"El sistema ha actualizado sus parámetros de aprendizaje.",
            parse_mode="Markdown"
        )
        context.user_data['prediccion_activa_id'] = None # Limpiar estado para evitar conflictos futuros
        print("🎉 DEBUG: Éxito total. Estado limpiado.")
    else:
        await update.message.reply_text("❌ Error interno al guardar en base de datos.")
