import os
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe
from services.table_detector import obtener_columnas_tabla
# --- NUEVO IMPORT VITAL ---
from services.ingestor_komet import ingestor_komet

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
    tg_file = await context.bot.get_file(archivo.file_id)

    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"Procesando {archivo.file_name}…")

    # ============================================================
    # 🧠 CEREBRO NUEVO: Detección Automática de "Confirm POs"
    # ============================================================
    # Si el archivo parece un reporte sucio de Komet, lo limpiamos y cargamos relacionalmente.
    if "confirm" in archivo.file_name.lower() and "po" in archivo.file_name.lower():
        await update.message.reply_text("⚙️ Detectado formato Komet (Sucio). Iniciando limpieza y carga relacional...")
        
        try:
            # Llamamos al especialista
            resultado = ingestor_komet.procesar_archivo(ruta)
            await update.message.reply_text(resultado)
        except Exception as e:
            await update.message.reply_text(f"❌ Error en ingestor Komet: {e}")
        finally:
            # Limpiamos la evidencia
            try: os.remove(ruta)
            except: pass
        
        return  # ¡IMPORTANTE! Salimos aquí para que no ejecute la lógica antigua.

    # ============================================================
    # 🦖 CEREBRO ANTIGUO: Lógica Manual (/tabla)
    # ============================================================
    try:
        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text("Primero selecciona una tabla con /tabla …")
            # Borramos el archivo para no llenar el disco
            try: os.remove(ruta)
            except: pass
            return

        df = cargar_tabla(ruta)

        # ============================================================
        # CONFIRM PO (LEGACY - Solo si alguien fuerza la tabla manualmente)
        # ============================================================
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

            df = df.drop_duplicates(
                subset=["po_number", "product", "vendor"], keep="last"
            )

            resultado = insertar_dataframe(
                tabla_destino,
                df,
                columna_unica="po_number,product,vendor",
            )
        # ============================================================
        # GENERAL (proveedores, airlines, etc.)
        # ============================================================
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
                clave_unica = "codigo,proveedor,gerente"
                df = df.drop_duplicates(subset=["codigo"], keep="last")
            elif tabla_destino == "airlines" and "cod" in df.columns:
                clave_unica = "cod"
                df = df.drop_duplicates(subset=["cod"], keep="last")

            resultado = insertar_dataframe(
                tabla_destino,
                df,
                columna_unica=clave_unica,
            )

        await update.message.reply_text(f"✔️ {resultado}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        # Limpieza de cortesía
        try: os.remove(ruta)
        except: pass
