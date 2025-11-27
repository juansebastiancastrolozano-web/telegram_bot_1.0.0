import pandas as pd
import logging
import uuid
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    def _limpiar_numero(self, valor):
        try:
            if pd.isna(valor): return 0
            s = str(valor).strip().replace(',', '').replace('$', '').replace(' ', '')
            if not s: return 0
            return float(s)
        except: return 0

    def procesar_archivo(self, ruta_archivo: str):
        try:
            # 1. Lectura
            if ruta_archivo.lower().endswith('.csv'):
                try: df_raw = pd.read_csv(ruta_archivo, header=None, encoding='utf-8')
                except: df_raw = pd.read_csv(ruta_archivo, header=None, encoding='latin1')
            else:
                df_raw = pd.read_excel(ruta_archivo, header=None)

            # 2. Buscar dónde empieza la tabla (Ancla)
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po #" in row_str and "vendor" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None: return "❌ No encontré la tabla 'Confirm POs'."

            # 3. Armar la tabla limpia
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns]

            # 4. FILTROS DIRECTOS (Lo que pediste)
            # Quitar vacíos
            df = df.dropna(subset=['PO #'])
            # Quitar el encabezado si se repite
            df = df[df['PO #'].astype(str) != 'PO #']
            # ELIMINAR EL INFILTRADO ESPECÍFICO
            df = df[~df['PO #'].astype(str).str.contains("Report Explanation", case=False, na=False)]
            # Quitar totales o basura corta
            df = df[df['PO #'].astype(str).str.len() > 3]

            batch_id = str(uuid.uuid4())[:8]
            items_batch = []

            # 5. Mapeo Directo (Sin diccionarios raros)
            for idx, row in df.iterrows():
                try:
                    item = {
                        "import_batch_id": batch_id,
                        "status": "Pending",
                        "created_at": datetime.utcnow().isoformat(),
                        
                        # Mapeo 1 a 1: Excel -> DB
                        "po_komet": str(row.get('PO #', '')).strip(),
                        "vendor": str(row.get('Vendor', '')),
                        "ship_date": pd.to_datetime(row.get('Ship Date'), errors='coerce').strftime('%Y-%m-%d') if pd.notna(row.get('Ship Date')) else None,
                        "product_name": str(row.get('Product', '')),
                        "customer_code": str(row.get('Customer', '')),
                        
                        # Números
                        "quantity_boxes": int(self._limpiar_numero(row.get('Qty PO'))),
                        "confirmed_boxes": int(self._limpiar_numero(row.get('Confirmed'))),
                        "box_type": str(row.get('B/T', '')),
                        "total_stems": int(self._limpiar_numero(row.get('Total U'))),
                        "unit_price_purchase": self._limpiar_numero(row.get('Cost')),
                        
                        # Detalles
                        "origin": str(row.get('Origin', '')),
                        "status_komet": str(row.get('Status', '')),
                        "mark_code": str(row.get('Mark Code', '')),
                        "notes": str(row.get('Notes for the vendor', ''))
                    }
                    items_batch.append(item)
                except:
                    continue

            # 6. Guardar
            if items_batch:
                # Limpiamos lote anterior para no duplicar si subes el mismo archivo
                # (Opcional, si quieres acumular quita esta línea)
                # db_client.table("staging_komet").delete().neq("id", "0000").execute() 
                
                db_client.table("staging_komet").insert(items_batch).execute()

            return f"📥 **Komet Limpio:** {len(items_batch)} filas importadas."

        except Exception as e:
            return f"💥 Error: {e}"

ingestor_komet = IngestorKomet()
