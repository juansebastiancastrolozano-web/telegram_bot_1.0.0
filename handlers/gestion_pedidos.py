from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.motor_ventas import GestorPrediccionVentas
from services.calculadora import calculadora 

gestor_ventas = GestorPrediccionVentas()

async def comando_sugerir_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.args:
        await update.message.reply_text("⚠️ Error: Faltó el cliente. Ej: `/sugerir MEXT`", parse_mode="Markdown")
        return

    codigo_cliente = context.args[0].upper().strip()
    await update.message.reply_text(f"📊 Analizando historial para: *{codigo_cliente}*...", parse_mode="Markdown")

    pred_id, sugerencia = gestor_ventas.generar_sugerencia_pedido(codigo_cliente)

    if not pred_id:
        await update.message.reply_text(f"❌ Error: {sugerencia.get('error')}")
        return

    # Guardamos el nombre real del cliente en el contexto para usarlo al confirmar
    context.user_data['cliente_actual_nombre'] = sugerencia.get('cliente_nombre', codigo_cliente)

    texto = (
        f"📋 *Oportunidad Comercial*\n"
        f"👤 **Cliente:** {sugerencia.get('cliente_nombre')}\n"
        f"📈 **Estrategia:** {sugerencia['estrategia_aplicada']}\n"
        f"📝 **Razón:** {sugerencia['justificacion_tecnica']}\n\n"
        f"🌺 **Producto:** {sugerencia['producto_objetivo']}\n"
        f"💵 **Precio Sugerido:** ${sugerencia['precio_unitario']} USD\n"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Confirmar Orden", callback_data=f"aprob_{pred_id}"),
            InlineKeyboardButton("📝 Ajustar Precio", callback_data=f"ajust_{pred_id}")
        ],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"cancel_{pred_id}")]
    ]
    
    await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def procesar_callback_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() 

    data = query.data
    accion, pred_id = data.split("_")

    if accion == "aprob":
        # --- DATOS DE LA ORDEN (SIMULADOS/QUEMADOS PARA PRUEBA) ---
        # En el futuro, estos vendrán de la DB o selección del usuario
        cantidad = 5
        tipo_caja = "QB"
        tallos_ramo = 25
        ramos_full = 80
        precio = 0.45 # Precio final acordado
        producto = "Spray Rose Assorted 50cm (Bot)"
        
        # Recuperamos nombre del cliente del contexto (o fallback a MEXT)
        cliente_nombre = context.user_data.get('cliente_actual_nombre', "MEXT")

        # 1. Cálculo Matemático
        resultado = calculadora.calcular_linea_pedido(
            cantidad_cajas=cantidad,
            tipo_caja=tipo_caja,
            tallos_por_ramo=tallos_ramo,
            ramos_por_caja_full=ramos_full,
            precio_unitario=precio
        )
        
        # 2. Materialización en Base de Datos (INSERT)
        datos_db = {
            "producto_descripcion": producto,
            "cajas": cantidad,
            "tipo_caja": tipo_caja,
            "total_tallos": resultado['total_tallos'],
            "precio_unitario": precio,
            "cliente_nombre": cliente_nombre,
            "vendor": "BM" # Vendor por defecto según tu ejemplo
        }
        
        po_nuevo = gestor_ventas.crear_orden_confirmada(datos_db)
        
        # 3. Respuesta Final
        msg = (
            f"✅ **Orden Confirmada y Guardada**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 **PO:** `{po_nuevo}`\n"
            f"📦 **Empaque:** {resultado['meta_data']}\n"
            f"🌹 **Total:** {resultado['total_tallos']} tallos\n"
            f"💰 **Valor:** ${resultado['valor_total']} USD\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_(Guardado en tabla confirm_po)_"
        )
        if not po_nuevo:
            msg = "❌ Error crítico: No se pudo guardar la PO en la base de datos."

        await query.edit_message_text(msg, parse_mode="Markdown")

    elif accion == "ajust":
        context.user_data['prediccion_activa_id'] = pred_id
        await query.edit_message_text("📝 **Modo Edición:** Escribe el precio real (ej: 0.45):", parse_mode="Markdown")

    elif accion == "cancel":
        await query.edit_message_text("❌ Operación cancelada.")

async def recibir_ajuste_precio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pred_id = context.user_data.get('prediccion_activa_id')
    if not pred_id: return 

    try:
        precio = float(update.message.text.strip().replace(",", "."))
    except:
        await update.message.reply_text("⚠️ Número inválido.")
        return

    if gestor_ventas.registrar_ajuste_usuario(pred_id, precio):
        await update.message.reply_text(f"💾 Precio ajustado a ${precio}. Aprendizaje registrado.")
        context.user_data['prediccion_activa_id'] = None
    else:
        await update.message.reply_text("❌ Error guardando ajuste.")
