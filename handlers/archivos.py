import os
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe
from services.table_detector import obtener_columnas_tabla

# --- LOS CEREBROS ---
from services.ingestor_komet import ingestor_komet
from services.ingestor_so import ingestor_so
from services.ingestor_opbase import ingestor_opbase

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def _norm_generico(nombre: str) -> str:
    s = str(nombre).strip().lower()
    return s.replace(" ", "_")

def _convertir_entero_seguro(x):
    if pd.isna(x): return None
    s = str(x).strip().lower()
    if s in ("", "nan", "none", "<na>", "na"): return None
    try: return int(float(s))
    except Exception: return None

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    archivo = update.message.document
    file_name_lower = archivo.file_name.lower()
    
    tg_file = await context.bot.get_file(archivo.file_id)
    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"📥 Procesando `{archivo.file_name}`...", parse_mode="Markdown")

    try:
        # 1. CASO KOMET (Confirm POs)
        if "confirm" in file_name_lower and "po" in file_name_lower:
            await update.message.reply_text("⚙️ Detectado formato Komet. Iniciando limpieza...", parse_mode="Markdown")
            resultado = ingestor_komet.procesar_archivo(ruta)
            await update.message.reply_text(resultado, parse_mode="HTML")
            return

        # 2. CASO OPBASE (Memoria Histórica)
        if "opbase" in file_name_lower:
            await update.message.reply_text("🏛️ Detectado OPBASE. Ingestando memoria histórica...", parse_mode="Markdown")
            resultado = ingestor_opbase.procesar_memoria_historica(ruta)
            await update.message.reply_text(resultado, parse_mode="Markdown")
            return

        # 3. CASO ARCHIVO MAESTRO (Hoja SO)
        if "orde_de_pedido" in file_name_lower or "so" in file_name_lower:
            await update.message.reply_text("💰 Detectado Archivo Maestro. Auditando finanzas (Hoja SO)...", parse_mode="Markdown")
            resultado = ingestor_so.procesar_master_file(ruta)
            await update.message.reply_text(resultado, parse_mode="HTML")
            return

        # 4. MODO MANUAL (Legacy /tabla)
        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text("⚠️ No sé qué hacer con este archivo. Usa /tabla <nombre> primero.")
            return

        df = cargar_tabla(ruta)
        
        # Lógica legacy para tablas simples
        esquema = obtener_columnas_tabla(tabla_destino)
        if not esquema:
            # Si no hay esquema definido, intentamos inserción directa (peligroso pero flexible)
            pass 
        else:
            mapa_db = {_norm_generico(col["nombre"]): col["nombre"] for col in esquema}
            df.columns = [str(c).strip() for c in df.columns]
            columnas_originales = []
            renombrar = {}
            for c in df.columns:
                key = _norm_generico(c)
                if key in mapa_db:
                    columnas_originales.append(c)
                    renombrar[c] = mapa_db[key]
            
            if columnas_originales:
                df = df[columnas_originales].rename(columns=renombrar)
            else:
                # Si no coincide ninguna columna, abortamos
                await update.message.reply_text(f"❌ Las columnas del Excel no coinciden con la tabla '{tabla_destino}'.")
                return

        df = df.dropna(how="all").reset_index(drop=True)

        # Deduplicación básica para catálogos
        clave_unica = None
        if tabla_destino == "proveedores" and "codigo" in df.columns:
            clave_unica = "codigo"
            df = df.drop_duplicates(subset=["codigo"], keep="last")
        elif tabla_destino == "airlines" and "cod" in df.columns:
            clave_unica = "cod"
            df = df.drop_duplicates(subset=["cod"], keep="last")

        resultado = insertar_dataframe(
            tabla_destino,
            df,
            columna_unica=clave_unica,
        )
        await update.message.reply_text(f"✔️ Carga manual exitosa: {resultado}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error procesando archivo: {e}")
    finally:
        try: os.remove(ruta)
        except: pass
