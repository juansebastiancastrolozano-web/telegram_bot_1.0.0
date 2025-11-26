from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from services.motor_ventas import GestorPrediccionVentas
from services.calculadora import calculadora 
from datetime import datetime

# Instanciamos el servicio de negocio
gestor_ventas = GestorPrediccionVentas()

# --- NUEVO: COMANDO RUTINA (Fase 3) ---
async def comando_rutina_diaria(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando: /rutina
    Busca qué clientes deberían comprar hoy.
    """
    await update.message.reply_text("📅 <b>Consultando el Cronograma Maestro...</b>", parse_mode="HTML")

    oportunidades = gestor_ventas.buscar_oportunidades_del_dia()

    if not oportunidades:
        dia_hoy = datetime.now().strftime('%A')
        await update.message.reply_text(
            f"😴 <b>Todo tranquilo por hoy ({dia_hoy}).</b>\n"
            f"No encontré Standing Orders programadas para este día.",
            parse_mode="HTML"
        )
        return

    keyboard = []
    resumen_texto = f"⚡ <b>Oportunidades Detectadas para Hoy:</b>\n\n"

    for op in oportunidades[:10]: 
        cliente = op['cliente']
        producto = op.get('producto_ejemplo', 'Producto Varios')
        caja = op.get('caja_tipica', 'QB')
        
        resumen_texto += f"🔹 <b>{cliente}</b> (Suele pedir {caja})\n"
        
        # Botón mágico que simula escribir /sugerir CLIENTE
        keyboard.append([
            InlineKeyboardButton(f"🚀 Atender a {cliente}", callback_data=f"auto_{cliente}")
        ])

    resumen_texto += "\n<i>Selecciona un cliente para generar su orden:</i>"
    
    await update.message.reply_text(resumen_texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# --- COMANDOS EXISTENTES (Fase 1 y 2) ---

async def comando_sugerir_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "⚠️ <b>Error de Sintaxis</b>\nPor favor ingrese el código del cliente.\nEjemplo: <code>/sugerir MEXT</code>",
            parse_mode="HTML"
        )
        return

    codigo_cliente = context.args[0].upper().strip()
    await update.message.reply_text(f"🧠 Consultando memoria para: <b>{codigo_cliente}</b>...", parse_mode="HTML")

    pred_id, sugerencia = gestor_ventas.generar_sugerencia_pedido(codigo_cliente)

    if not pred_id:
        await update.message.reply_text(f"❌ Error: {sugerencia.get('error')}")
        return

    context.user_data['sugerencia_actual'] = sugerencia
    logistica = sugerencia.get('logistica', {})
    
    texto = (
        f"📋 <b>Propuesta de Pedido</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Cliente:</b> {sugerencia.get('cliente_nombre')}\n"
        f"🌺 <b>Producto:</b> {sugerencia['producto_objetivo']}\n"
        f"💵 <b>Precio:</b> ${sugerencia['precio_unitario']}\n\n"
        f"📦 <b>Logística Aprendida:</b>\n"
        f"   • Caja: {logistica.get('tipo_caja')}\n"
        f"   • Config: {logistica.get('ramos_x_caja')} ramos x {logistica.get('tallos_x_ramo')} tallos\n"
        f"   • Marca: <i>{logistica.get('marcacion')}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"¿Procedemos?"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Confirmar (1 Caja)", callback_data=f"aprob_{pred_id}"),
            InlineKeyboardButton("📝 Ajustar Precio", callback_data=f"ajust_{pred_id}")
        ],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"cancel_{pred_id}")]
    ]
    
    await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def procesar_callback_pedido(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() 

    data = query.data

    # --- ENRUTADOR FASE 3 ---
    if data.startswith("auto_"):
        cliente_code = data.split("_")[1]
        context.args = [cliente_code] 
        await comando_sugerir_pedido(update, context)
        return

    try:
        accion, pred_id = data.split("_")
    except: return

    if accion == "aprob":
        sugerencia = context.user_data.get('sugerencia_actual')
        
        if not sugerencia:
            await query.edit_message_text("⚠️ Sesión expirada. Vuelve a usar /sugerir.")
            return

        logistica = sugerencia.get('logistica', {})
        
        # Datos reales aprendidos
        cantidad = 1 
        tipo_caja = logistica.get('tipo_caja', 'QB')
        tallos_ramo = int(logistica.get('tallos_x_ramo', 25))
        ramos_caja = int(logistica.get('ramos_x_caja', 10))
        
        factor_map = {'EB': 8, 'QB': 4, 'HB': 2}
        factor = factor_map.get(tipo_caja, 4)
        ramos_full_teorico = ramos_caja * factor 

        precio = float(sugerencia['precio_unitario'])
        
        # 1. Matemática
        resultado = calculadora.calcular_linea_pedido(
            cantidad_cajas=cantidad,
            tipo_caja=tipo_caja,
            tallos_por_ramo=tallos_ramo,
            ramos_por_caja_full=ramos_full_teorico,
            precio_unitario=precio
        )
        
        # 2. DB Insert
        datos_db = {
            "producto_descripcion": sugerencia['producto_objetivo'],
            "cajas": cantidad,
            "tipo_caja": tipo_caja,
            "total_tallos": resultado['total_tallos'],
            "precio_unitario": precio,
            "cliente_nombre": sugerencia['codigo_interno'],
            "vendor": "BM",
            "valor_total_pedido": resultado['valor_total'],
            "marcacion": logistica.get('marcacion')
        }
        
        po_nuevo = gestor_ventas.crear_orden_confirmada(datos_db)
        
        # 3. Respuesta
        if po_nuevo:
            msg = (
                f"✅ <b>Orden Creada con Éxito</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>PO:</b> <code>{po_nuevo}</code>\n"
                f"📦 <b>Config:</b> {resultado['meta_data']}\n"
                f"🏷️ <b>Marca:</b> {logistica.get('marcacion')}\n"
                f"💰 <b>Total:</b> ${resultado['valor_total']} USD\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
            )
        else:
            msg = "❌ Error crítico guardando PO."

        await query.edit_message_text(msg, parse_mode="HTML")

    elif accion == "ajust":
        context.user_data['prediccion_activa_id'] = pred_id
        await query.edit_message_text("📝 Escribe el nuevo precio (ej: 0.45):", parse_mode="HTML")

    elif accion == "cancel":
        await query.edit_message_text("❌ Cancelado.")

async def recibir_ajuste_precio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pred_id = context.user_data.get('prediccion_activa_id')
    if not pred_id: return 

    try:
        precio = float(update.message.text.strip().replace(",", "."))
    except:
        await update.message.reply_text("⚠️ Número inválido.")
        return

    if context.user_data.get('sugerencia_actual'):
        context.user_data['sugerencia_actual']['precio_unitario'] = precio

    if gestor_ventas.registrar_ajuste_usuario(pred_id, precio):
        await update.message.reply_text(
            f"💾 Precio ajustado a <b>${precio}</b>.\nSi deseas confirmar la orden con este precio, vuelve a usar /sugerir (por ahora).",
            parse_mode="HTML"
        )
        context.user_data['prediccion_activa_id'] = None
    else:
        await update.message.reply_text("❌ Error guardando ajuste.")
