from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.motor_ventas import GestorPrediccionVentas
from services.calculadora import calculadora 

# Instanciamos el servicio de negocio
gestor_ventas = GestorPrediccionVentas()

async def comando_sugerir_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando: /sugerir <CODIGO_CLIENTE>
    Inicia el flujo de recomendación de orden basado en inteligencia comercial.
    """
    if not context.args:
        await update.message.reply_text(
            "⚠️ <b>Error de Sintaxis</b>\nPor favor ingrese el código del cliente.\nEjemplo: <code>/sugerir MEXT</code>",
            parse_mode="HTML"
        )
        return

    codigo_cliente = context.args[0].upper().strip()
    await update.message.reply_text(f"📊 Analizando historial comercial para: <b>{codigo_cliente}</b>...", parse_mode="HTML")

    # Invocación al servicio
    pred_id, sugerencia = gestor_ventas.generar_sugerencia_pedido(codigo_cliente)

    if not pred_id:
        error_msg = sugerencia.get("error", "Error desconocido")
        await update.message.reply_text(f"❌ No se pudo generar sugerencia: {error_msg}")
        return

    # Guardamos el nombre real del cliente en el contexto para usarlo al confirmar
    nombre_cliente = sugerencia.get('cliente_nombre') or codigo_cliente
    context.user_data['cliente_actual_nombre'] = nombre_cliente

    # Construcción de la respuesta formal (HTML)
    texto_respuesta = (
        f"📋 <b>Resumen de Oportunidad Comercial</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Cliente:</b> {nombre_cliente}\n"
        f"📈 <b>Estrategia:</b> {sugerencia['estrategia_aplicada']}\n"
        f"📝 <b>Análisis:</b> {sugerencia['justificacion_tecnica']}\n\n"
        f"🌺 <b>Producto Sugerido:</b> {sugerencia['producto_objetivo']}\n"
        f"💵 <b>Precio Objetivo:</b> ${sugerencia['precio_unitario']} USD\n"
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

    await update.message.reply_text(texto_respuesta, reply_markup=reply_markup, parse_mode="HTML")

async def procesar_callback_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Manejador de eventos para los botones inline.
    """
    query = update.callback_query
    await query.answer() # Confirmar recepción del evento

    data = query.data
    accion, pred_id = data.split("_")

    if accion == "aprob":
        # --- DATOS DE LA ORDEN (SIMULADOS/QUEMADOS PARA PRUEBA) ---
        cantidad = 5
        tipo_caja = "QB"
        tallos_ramo = 25
        ramos_full = 80
        precio = 0.45 # Precio final acordado
        producto = "Spray Rose Assorted 50cm (Bot)"
        
        cliente_nombre = context.user_data.get('cliente_actual_nombre', "MEXT")

        # 1. Cálculo Matemático
        resultado = calculadora.calcular_linea_pedido(
            cantidad_cajas=cantidad,
            tipo_caja=tipo_caja,
            tallos_por_ramo=tallos_ramo,
            ramos_por_caja_full=ramos_full,
            precio_unitario=precio
        )
        
        # 2. Materialización en Base de Datos (INSERT RELACIONAL)
        # IMPORTANTE: Pasamos valor_total_pedido para que la cabecera sepa el total $$$
        datos_db = {
            "producto_descripcion": producto,
            "cajas": cantidad,
            "tipo_caja": tipo_caja,
            "total_tallos": resultado['total_tallos'],
            "precio_unitario": precio,
            "cliente_nombre": cliente_nombre,
            "vendor": "BM",
            "valor_total_pedido": resultado['valor_total']  # <--- CRUCIAL PARA LA NUEVA ESTRUCTURA
        }
        
        po_nuevo = gestor_ventas.crear_orden_confirmada(datos_db)
        
        # 3. Respuesta Final (HTML)
        if po_nuevo:
            msg = (
                f"✅ <b>Orden Confirmada y Guardada</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>PO:</b> <code>{po_nuevo}</code>\n"
                f"📦 <b>Empaque:</b> {resultado['meta_data']}\n"
                f"🌹 <b>Total:</b> {resultado['total_tallos']} tallos\n"
                f"💰 <b>Valor:</b> ${resultado['valor_total']} USD\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>(Guardado en tablas sales_orders/sales_items)</i>"
            )
        else:
            msg = "❌ <b>Error crítico:</b> No se pudo guardar la PO en la base de datos."

        await query.edit_message_text(msg, parse_mode="HTML")

    elif accion == "ajust":
        # Guardamos el ID en el contexto del usuario para esperar su input numérico
        context.user_data['prediccion_activa_id'] = pred_id
        
        await query.edit_message_text(
            f"📝 <b>Modo de Edición de Precio</b>\n\n"
            f"Por favor, ingrese el <i>Precio Unitario Real</i> de cierre (Ej: 0.38):",
            parse_mode="HTML"
        )

    elif accion == "cancel":
        await query.edit_message_text("❌ Operación cancelada por el usuario.")

async def recibir_ajuste_precio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Captura el input de texto del usuario cuando está en modo de ajuste.
    """
    user_msg = update.message.text
    pred_id = context.user_data.get('prediccion_activa_id')

    if not pred_id:
        return 

    texto_input = user_msg.strip()

    try:
        precio_real = float(texto_input.replace(",", "."))
    except ValueError:
        await update.message.reply_text("⚠️ Formato inválido. Por favor ingrese solo el número (ej. 0.45).")
        return

    # Registro en base de datos
    exito = gestor_ventas.registrar_ajuste_usuario(pred_id, precio_real)

    if exito:
        await update.message.reply_text(
            f"💾 <b>Ajuste Registrado</b>\n"
            f"Nuevo precio: ${precio_real} USD.\n"
            f"El sistema ha actualizado sus parámetros de aprendizaje.",
            parse_mode="HTML"
        )
        context.user_data['prediccion_activa_id'] = None 
    else:
        await update.message.reply_text("❌ Error interno al guardar en base de datos.")
