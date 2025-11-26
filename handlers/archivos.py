import os
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe
from services.table_detector import obtener_columnas_tabla

# --- IMPORTACIONES DE LOS CEREBROS NUEVOS ---
from services.ingestor_komet import ingestor_komet
from services.ingestor_so import ingestor_so

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def _norm_generico(nombre: str) -> str:
    s = str(nombre).strip().lower()
    return s.replace(" ", "_")


def _convertir_entero_seguro(x):
    if pd.isna(x):
        return None
    s = str(x).strip().lower()
    if s in ("", "nan", "none", "<na>", "na"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    archivo = update.message.document
    file_name_lower = archivo.file_name.lower()
    
    tg_file = await context.bot.get_file(archivo.file_id)
    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"📥 Procesando `{archivo.file_name}`...", parse_mode="Markdown")

    # ============================================================
    # 🧠 INTELIGENCIA AUTOMÁTICA (Sin necesidad de /tabla)
    # ============================================================
    
    # 1. CASO KOMET (Confirm POs)
    if "confirm" in file_name_lower and "po" in file_name_lower:
        await update.message.reply_text("⚙️ Detectado formato Komet. Iniciando limpieza...", parse_mode="Markdown")
        try:
            resultado = ingestor_komet.procesar_archivo(ruta)
            await update.message.reply_text(resultado, parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"❌ Error Komet: {e}")
        finally:
            try: os.remove(ruta)
            except: pass
        return # Salimos para no ejecutar el resto

    # 2. CASO ARCHIVO MAESTRO (Hoja SO)
    elif "orde_de_pedido" in file_name_lower or "so" in file_name_lower:
        await update.message.reply_text("💰 Detectado Archivo Maestro. Auditando finanzas (Hoja SO)...", parse_mode="Markdown")
        try:
            resultado = ingestor_so.procesar_master_file(ruta)
            await update.message.reply_text(resultado, parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"❌ Error Auditoría SO: {e}")
        finally:
            try: os.remove(ruta)
            except: pass
        return # Salimos

    # ============================================================
    # 🦖 CEREBRO ANTIGUO: Lógica Manual (/tabla)
    # ============================================================
    try:
        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text("⚠️ No sé qué hacer con este archivo. Usa /tabla primero o sube un reporte conocido.")
            try: os.remove(ruta)
            except: pass
            return

        df = cargar_tabla(ruta)

        # CASO LEGACY: Confirm PO manual (si alguien insiste en usar /tabla confirm_po)
        if tabla_destino == "confirm_po":
            mapeo = {
                "po": "po_number",
                "vendor": "vendor",
                "ship_date": "ship_date",
                "product": "product",
                "qty_po": "boxes",
                "confirmed": "confirmed",
                "b_t": "box_type",
                "total_u": "total_units",
                "cost": "cost",
                "customer": "customer_name",
                "origin": "origin",
                "status": "status",
                "mark_code": "mark_code",
                "ship_country": "ship_country",
                "notes_for_the_vendor": "notes",
            }

            df.rename(columns=mapeo, inplace=True)

            for col in ("boxes", "confirmed", "total_units"):
                if col in df.columns:
                    df[col] = df[col].apply(_convertir_entero_seguro)

            for col in ("vendor", "product", "ship_date"):
                if col in df.columns:
                    df = df[df[col].notna()]

            columnas_validas = list(mapeo.values())
            df = df[[c for c in df.columns if c in columnas_validas]]

            df = df.dropna(how="all").reset_index(drop=True)

            # Limpieza de duplicados básica
            if "po_number" in df.columns:
                df = df.drop_duplicates(subset=["po_number", "product", "vendor"], keep="last")

            resultado = insertar_dataframe(
                tabla_destino,
                df,
                columna_unica="po_number,product,vendor" if "po_number" in df.columns else None,
            )
            
        # CASO GENERAL (Proveedores, Airlines, etc.)
        else:
            esquema = obtener_columnas_tabla(tabla_destino)
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
                df = df.iloc[0:0]

            df = df.dropna(how="all").reset_index(drop=True)

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
        await update.message.reply_text(f"❌ Error genérico: {e}")
    finally:
        # Limpieza final
        try: os.remove(ruta)
        except: pass
