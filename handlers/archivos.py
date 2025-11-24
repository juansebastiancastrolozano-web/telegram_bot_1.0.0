import os
import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from handlers.tabla import user_tablas
from services.table_loader import cargar_tabla
from services.supabase_insert import insertar_dataframe

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ------------------ helpers específicos para confirm_po ------------------ #

def _procesar_confirm_po(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y mapea el archivo de Confirm POs al esquema de la tabla confirm_po.
    """

    # Excel -> columnas de Supabase
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

    # Renombrar si existen
    df = df.rename(columns=mapeo)

    # Filtrar solo columnas que realmente existen en la tabla confirm_po
    columnas_destino = [
        "po_number",
        "vendor",
        "ship_date",
        "product",
        "boxes",
        "confirmed",
        "box_type",
        "total_units",
        "cost",
        "customer_name",
        "origin",
        "status",
        "mark_code",
        "ship_country",
        "notes",
    ]
    df = df[[c for c in df.columns if c in columnas_destino]]

    # Normalizar enteros
    cols_int = ["boxes", "confirmed", "total_units"]

    def _to_int_or_none(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        if s in ("", "nan", "none", "<na>", "na"):
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    for col in cols_int:
        if col in df.columns:
            df[col] = df[col].apply(_to_int_or_none)

    # Limpiar cost (string tipo "$ 0.360")
    if "cost" in df.columns:
        def _limpiar_cost(x):
            if pd.isna(x):
                return None
            s = (
                str(x)
                .replace("$", "")
                .replace(",", "")
                .strip()
            )
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return None

        df["cost"] = df["cost"].apply(_limpiar_cost)

    # Filtrar filas de verdad (evitar "Report Explanation", etc.)
    if "po_number" in df.columns:
        df["po_number"] = df["po_number"].astype(str).str.strip()
        # nos quedamos solo con las que parecen PO reales tipo "P083638"
        mask_real = df["po_number"].str.match(r"^P\d+", na=False)
        df = df[mask_real]

    return df.reset_index(drop=True)


# --------------------------- handler principal --------------------------- #

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    archivo = update.message.document
    tg_file = await context.bot.get_file(archivo.file_id)

    ruta = os.path.join(UPLOAD_DIR, archivo.file_name)
    await tg_file.download_to_drive(ruta)

    await update.message.reply_text(f"Procesando {archivo.file_name}…")

    try:
        df = cargar_tabla(ruta)

        user_id = update.message.from_user.id
        tabla_destino = user_tablas.get(user_id)

        if not tabla_destino:
            await update.message.reply_text(
                "Primero selecciona una tabla con /tabla nombre_tabla"
            )
            return

        # ---------------- lógica por tabla ---------------- #

        if tabla_destino == "confirm_po":
            # Solo aquí aplicamos el mapeo especial
            df = _procesar_confirm_po(df)
            columna_unica = "po_number,product,vendor"

        else:
            # Para tablas como 'proveedores':
            # - asumimos que los encabezados del archivo ya coinciden
            #   (o que tu tabla se creó con esos nombres normalizados)
            # - no tocamos columnas
            columna_unica = None  # insert simple, sin ON CONFLICT

        # Si después de limpiar no queda nada, avisamos
        if df.empty:
            await update.message.reply_text(
                f"⚪ 0 filas (nada que insertar) en {tabla_destino}"
            )
            return

        # Inserción / upsert en Supabase
        resultado = insertar_dataframe(
            tabla_destino,
            df,
            columna_unica=columna_unica,
        )

        await update.message.reply_text(f"✔️ {resultado}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

