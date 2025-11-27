import os
import logging
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes, 
    CallbackQueryHandler
)
import requests

# --- IMPORTACIONES DE TUS HANDLERS EXISTENTES ---
# Asumo que estos módulos son los satélites que orbitan tu núcleo
from handlers.lookup import handle_lookup
from handlers.cliente import handle_cliente
from handlers.finca import handle_finca
from handlers.help import handle_help
from handlers.archivos import handle_file
from handlers.tabla import set_tabla
from handlers.tablageneral import tablageneral

# --- CEREBRO COMERCIAL (La lógica de mercado) ---
from handlers.gestion_pedidos import (
    comando_sugerir_pedido, 
    procesar_callback_pedido, 
    recibir_ajuste_precio, 
    comando_rutina_diaria
)
from handlers.facturacion import comando_generar_factura

# --- EL NUEVO ORDEN: PANEL DE CONTROL (ORDENAA DIGITAL) ---
# Importamos la triada de control definida en el módulo anterior
# Nota: router_panel será el encargado de despachar los sub-eventos
from handlers.panel_control import comando_panel, router_panel, procesar_input_panel

# Configuración del entorno y logging para auditoría forense de errores
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    El Alfa. El inicio. La bienvenida al laberinto ordenado.
    """
    await update.message.reply_text(
        "🌼 <b>Sistema J&G Flowers: Inteligencia Líquida Activada.</b>\n\n"
        "<b>Comandos de Poder:</b>\n"
        "🎛 <code>/panel</code> - El Panóptico (Gestión total de Órdenes)\n"
        "📅 <code>/rutina</code> - Rituales diarios\n"
        "🔮 <code>/sugerir MEXT</code> - Inyección de entropía controlada (Manual)\n"
        "🔎 <code>/po P123</code> - Arqueología de datos\n\n"
        "<i>La burocracia ha muerto. Arrastra un Excel para comenzar el ritual.</i>",
        parse_mode="HTML"
    )

# --- 1. ROUTER GLOBAL DE BOTONES (El guardián del umbral) ---
async def global_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Decide el destino de cada clic. Es el 'Maxwell's Demon' que separa 
    las partículas rápidas (Panel) de las lentas (Pedidos).
    """
    query = update.callback_query
    data = str(query.data)

    # DEFINICIÓN DE PATRONES DEL NUEVO PANEL:
    # panel_   -> Navegación general y refresco
    # view_    -> Ver detalle de una orden (Drill-down)
    # menu_    -> Submenús (Logística, Finanzas, Empaque)
    # page_    -> Paginación de listados
    # action_  -> Ejecuciones críticas (Generar Invoice, PO Consecutivo)
    # create_  -> Creación manual de ítems
    # edit_    -> Modificación de campos específicos
    # gest_    -> Gestión heredada (si aplica)
    # cat_     -> Categorías
    
    patrones_panel = [
        "panel_", "view_", "menu_", "page_", "action_", 
        "create_", "edit_", "gest_", "cat_", "approve_"
    ]

    # Si la data del botón contiene cualquiera de las llaves del panel:
    if any(key in data for key in patrones_panel):
        # Delegamos la responsabilidad al router específico del módulo panel
        await router_panel(update, context)
    
    else:
        # Si no es del panel, asumimos que es del flujo comercial (Sugerencias/Pedidos)
        # Aquí caen: 'ajust_', 'cancel_', y otros callbacks legacy
        await procesar_callback_pedido(update, context)

# --- 2. ROUTER GLOBAL DE TEXTO (El escriba) ---
async def handle_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Interpreta el texto libre. ¿Es una orden, un dato o una charla con la IA?
    """
    
    # CASO A: El usuario está en medio de una negociación de PRECIO (/sugerir)
    if context.user_data.get('prediccion_activa_id'):
        await recibir_ajuste_precio(update, context)
        return

    # CASO B: El usuario está editando un campo del PANEL (AWB, HAWB, Carrier)
    # Esta bandera 'estado_panel' debe setearse en panel_control.py al pedir input
    if context.user_data.get('estado_panel'):
        await procesar_input_panel(update, context)
        return

    # CASO C: Ruido de fondo -> Se envía a N8N para procesamiento de lenguaje natural
    await handle_message_n8n(update, context)

# --- 3. CONEXIÓN N8N (El oráculo externo) ---
async def handle_message_n8n(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    # Obtenemos identidad de forma resiliente
    user = update.message.from_user.username or update.message.from_user.first_name

    # Feedback inmediato para reducir ansiedad del usuario
    msg_espera = await update.message.reply_text("⏳ Consultando a la red neuronal...")

    try:
        # Petición síncrona (bloqueante) pero necesaria para este flujo simple
        response = requests.post(N8N_WEBHOOK_URL, json={
            "user": user,
            "message": text
        }, timeout=10) # Timeout para evitar zombis

        if response.status_code == 200:
            try:
                data = response.json()
                reply_text = data.get("reply", "😶 El oráculo guardó silencio.")
            except ValueError:
                reply_text = "⚠️ La respuesta del oráculo es ininteligible (JSON Error)."
            
            # Eliminamos el mensaje de espera y respondemos
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_espera.message_id)
            await update.message.reply_text(reply_text)
        else:
            await update.message.reply_text(f"⚠️ Error en la Matrix ({response.status_code}).")

    except Exception as e:
        await update.message.reply_text(f"💥 Ruptura del tejido: {str(e)}")

# --- BOOTSTRAP (El arranque) ---
if __name__ == "__main__":
    # Construcción de la aplicación asíncrona
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # --- Registro de Comandos (Los hechizos verbales) ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", handle_help))
    
    # Comandos de búsqueda y referencia
    app.add_handler(CommandHandler("po", handle_lookup))
    app.add_handler(CommandHandler("cliente", handle_cliente))
    app.add_handler(CommandHandler("finca", handle_finca))
    app.add_handler(CommandHandler("tabla", set_tabla))
    app.add_handler(CommandHandler("tablageneral", tablageneral))
    
    # Comandos de Acción y Gestión
    app.add_handler(CommandHandler("sugerir", comando_sugerir_pedido))
    app.add_handler(CommandHandler("rutina", comando_rutina_diaria))
    app.add_handler(CommandHandler("factura", comando_generar_factura))
    app.add_handler(CommandHandler("panel", comando_panel)) # <--- El nuevo centro de mando

    # --- Manejo de Materia (Archivos) ---
    # Captura documentos para ingestión (Excel, CSV)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    # --- Routers Maestros ---
    # CallbackQueryHandler debe ir ANTES que MessageHandler de texto para evitar conflictos
    app.add_handler(CallbackQueryHandler(global_callback_router))
    
    # El manejador de texto captura todo lo que no sea comando
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_router))

    print("🦅 J&G Bot Operativo. Vigilando el flujo de datos...")
    
    # Ejecución infinita (Polling)
    app.run_polling()
