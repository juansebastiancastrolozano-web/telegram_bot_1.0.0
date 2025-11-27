import pandas as pd
import logging
import uuid
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Ingesta 'Confirm POs' o 'ORDENAA' hacia la tabla staging_komet.
    Mapea todas las columnas críticas del negocio para alimentar el Panel de Control.
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
            # 1. Lectura Agnostic
            if ruta_archivo.lower().endswith('.csv'):
                try: df_raw = pd.read_csv(ruta_archivo, header=None, encoding='utf-8')
                except: df_raw = pd.read_csv(ruta_archivo, header=None, encoding='latin1')
            else:
                df_raw = pd.read_excel(ruta_archivo, header=None)

            # 2. Buscar Ancla (Flexible para aceptar formato Komet o formato ORDENAA)
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                # Buscamos palabras clave que suelen estar en ambos formatos
                if ("po" in row_str and "#" in row_str) and ("cust" in row_str or "vendor" in row_str):
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla (Falta fila con PO# / Customer)."

            # 3. Reconstrucción
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns] # Limpieza nombres

            # 4. Filtrado Basura
            col_po = next((c for c in df.columns if 'po' in c.lower() and '#' in c.lower()), 'PO#')
            
            if col_po not in df.columns:
                 return f"❌ No encontré columna de PO. Columnas detectadas: {list(df.columns)}"

            df = df.dropna(subset=[col_po])
            # Filtramos si el PO es muy corto (basura)
            df = df[df[col_po].astype(str).str.len() > 2]

            registros = 0
            batch_id = str(uuid.uuid4())[:8]
            items_batch = []

            # --- MAPEO DE COLUMNAS (EL DICCIONARIO MAESTRO) ---
            mapa_flexible = {
                'customer': 'customer_code',
                'cust': 'customer_code', 
                'po#': 'po_komet',
                'po #': 'po_komet',
                'status': 'status_komet',
                'code': 'product_code',
                'descrip': 'product_name',
                'product': 'product_name',
                'quantity': 'quantity_boxes',
                'qty po': 'quantity_boxes', 
                'uom': 'box_type',
                'b/t': 'box_type',          
                'flydate': 'fly_date',
                'ship date': 'ship_date',   
                'precio ': 'unit_price_sales', 
                'cost': 'unit_price_purchase', 
                'preciocompra': 'purchase_price', 
                'precio venta': 'sales_price',    
                'qty/box': 'bunches_per_box',
                'ramos por caja': 'bunches_per_box',
                'customer inv code': 'customer_inv_code',
                'mark code': 'mark_code',    
                'type': 'order_type',
                'comments': 'notes',
                'notes': 'notes',           
                'contents': 'contents',
                'bqt': 'bqt',
                'upc': 'upc',
                'size': 'size',
                'food': 'food',
                'sleeve': 'sleeve_type',
                'tallos': 'stems_per_bunch',
                'total tallos': 'total_stems',
                'total u': 'total_stems',    
                'awb': 'awb',
                'hija': 'hawb',
                'precio unt st': 'unit_price_stems',
                'tallos por cja': 'stems_per_box',
                'tipo de orden': 'order_kind',
                'flor': 'flower_type',
                'udv': 'udv',
                'pcuc': 'pcuc',
                'vc': 'vc',
                'pr': 'pr',
                'finca': 'farm_code',
                'vendor': 'vendor',          
                '1.25': 'factor_1_25',
                'sugerido': 'suggested_price',
                'po# consec': 'po_consecutive',
                'valor t': 'valor_t',
                'venta total': 'total_sales_value',
                'invoice': 'invoice_number',
                'fact finca': 'farm_invoice',
                'conseq': 'consecutive',
                'creditos': 'credits',
                'pago contado': 'cash_payment',
                'compra contado': 'cash_purchase'
            }

            # 5. ITERACIÓN Y LLENADO
            for idx, row in df.iterrows():
                try:
                    item = {
                        "status": "Pending",
                        "import_batch_id": batch_id,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    # Recorremos las columnas del Excel actual
                    for col_excel in df.columns:
                        col_lower = str(col_excel).lower()
                        valor_raw = row[col_excel]
                        
                        # Buscamos en el mapa
                        for key_map, col_db in mapa_flexible.items():
                            if key_map in col_lower:
                                # Si es numérico en DB, limpiamos
                                if col_db in ['quantity_boxes', 'bunches_per_box', 'stems_per_bunch', 'total_stems', 'stems_per_box']:
                                    item[col_db] = self._limpiar_entero(valor_raw)
                                elif col_db in ['unit_price_sales', 'unit_price_purchase', 'sales_price', 'purchase_price', 'suggested_price', 'valor_t', 'total_sales_value', 'pcuc', 'vc', 'pr', 'factor_1_25', 'credits', 'cash_payment', 'cash_purchase', 'unit_price_stems']:
                                    item[col_db] = self._limpiar_numero(valor_raw)
                                elif 'date' in col_db:
                                    try: item[col_db] = pd.to_datetime(valor_raw).strftime('%Y-%m-%d')
                                    except: pass
                                else:
                                    if pd.notna(valor_raw): item[col_db] = str(valor_raw)
                                
                                break 
                    
                    # Ajustes finales de lógica
                    if 'vendor' not in item and 'farm_code' in item: item['vendor'] = item['farm_code']
                    
                    # PO es obligatorio
                    if 'po_komet' not in item: 
                        val_po = row.get(col_po)
                        if pd.notna(val_po): item['po_komet'] = str(val_po)
                        else: continue 

                    items_batch.append(item)

                except Exception as e:
                    logger.error(f"Error fila {idx}: {e}")
                    continue

            # 6. INSERCIÓN
            if items_batch:
                chunk_size = 100
                for i in range(0, len(items_batch), chunk_size):
                    db_client.table("staging_komet").insert(items_batch[i:i+chunk_size]).execute()
                registros = len(items_batch)

            return (
                f"📥 **Ingesta Komet Exitosa**\n"
                f"🔖 Lote: `{batch_id}`\n"
                f"📦 Filas capturadas: {registros}\n"
                f"👉 Datos listos en 'staging_komet'."
            )

        except Exception as e:
            return f"💥 Error crítico Ingestor Komet: {e}"

ingestor_komet = IngestorKomet()
