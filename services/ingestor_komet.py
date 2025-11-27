import pandas as pd
import logging
import uuid
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Ingesta optimizada para 'Confirm POs.xls'.
    """

    def _limpiar_numero(self, valor):
        try:
            if pd.isna(valor): return 0
            s = str(valor).strip().replace(',', '').replace('$', '').replace(' ', '')
            if not s: return 0
            return float(s)
        except: return 0
    
    def _limpiar_entero(self, valor):
        try: return int(self._limpiar_numero(valor))
        except: return 0

    def procesar_archivo(self, ruta_archivo: str):
        try:
            # 1. Lectura
            if ruta_archivo.lower().endswith('.csv'):
                try: df_raw = pd.read_csv(ruta_archivo, header=None, encoding='utf-8')
                except: df_raw = pd.read_csv(ruta_archivo, header=None, encoding='latin1')
            else:
                df_raw = pd.read_excel(ruta_archivo, header=None)

            # 2. Buscar Ancla (PO #, Vendor)
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po #" in row_str and "vendor" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla 'Confirm POs' (Busqué 'PO #' y 'Vendor')."

            # 3. Reconstrucción
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns]

            # 4. Filtrado
            col_po = 'PO #' # Nombre exacto en tu Excel
            
            # Verificar que exista la columna
            if col_po not in df.columns:
                # Intento de rescate si se llama diferente
                col_po = next((c for c in df.columns if 'PO' in c and '#' in c), None)
                if not col_po: return f"❌ Error de columnas. Encontré: {list(df.columns)}"

            df = df.dropna(subset=[col_po])
            # Filtramos basura (filas que no son POs reales)
            df = df[~df[col_po].astype(str).str.contains(':', na=False)]
            df = df[df[col_po].astype(str) != col_po] # Quitar header repetido

            registros = 0
            batch_id = str(uuid.uuid4())[:8]
            items_batch = []

            # 5. Iteración Exacta
            for idx, row in df.iterrows():
                try:
                    # Extracción directa basada en TU archivo
                    po_val = str(row.get(col_po, '')).strip()
                    if len(po_val) < 3: continue 

                    item = {
                        "po_komet": po_val,
                        "vendor": str(row.get('Vendor', '')),
                        "ship_date": pd.to_datetime(row.get('Ship Date'), errors='coerce').strftime('%Y-%m-%d') if pd.notna(row.get('Ship Date')) else datetime.now().strftime('%Y-%m-%d'),
                        "customer_code": str(row.get('Customer', '')),
                        "product_name": str(row.get('Product', '')),
                        
                        # Mapeo Numérico
                        "quantity_boxes": self._limpiar_entero(row.get('Qty PO')),
                        "confirmed_boxes": self._limpiar_entero(row.get('Confirmed')),
                        "box_type": str(row.get('B/T', 'QB')),
                        "total_stems": self._limpiar_entero(row.get('Total U')),
                        "unit_price_purchase": self._limpiar_numero(row.get('Cost')), # <--- AQUÍ FALLABA ANTES
                        
                        # Logística
                        "mark_code": str(row.get('Mark Code', '')),
                        "origin": str(row.get('Origin', '')),
                        "notes": str(row.get('Notes for the vendor', '')),
                        "status_komet": str(row.get('Status', '')),
                        
                        # Control
                        "status": "Pending",
                        "import_batch_id": batch_id,
                        "created_at": datetime.utcnow().isoformat()
                    }
                    
                    items_batch.append(item)

                except Exception as e:
                    logger.error(f"Error fila {idx}: {e}")
                    continue

            # 6. Inserción
            if items_batch:
                db_client.table("staging_komet").insert(items_batch).execute()
                registros = len(items_batch)

            return (
                f"📥 **Confirm POs Ingestado**\n"
                f"🔖 Lote: `{batch_id}`\n"
                f"📦 Filas: {registros}\n"
                f"👉 Datos guardados correctamente."
            )

        except Exception as e:
            return f"💥 Error crítico Ingestor Komet: {e}"

ingestor_komet = IngestorKomet()
