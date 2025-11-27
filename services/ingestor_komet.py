import pandas as pd
import logging
import uuid
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Ingesta 'Confirm POs' con filtro ANTI-LEYENDAS Nuclear.
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

            # 2. Buscar Ancla
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po #" in row_str and "vendor" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla 'Confirm POs'."

            # 3. Reconstrucción
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns]

            # 4. FILTRO NUCLEAR (Anti-Basura)
            col_po = 'PO #' 
            if col_po not in df.columns:
                col_po = next((c for c in df.columns if 'PO' in c and '#' in c), None)
                if not col_po: return "❌ Error columnas."

            # A. Quitar vacíos
            df = df.dropna(subset=[col_po])
            
            # B. Quitar Header repetido
            df = df[df[col_po].astype(str) != col_po]

            # C. LISTA NEGRA DE PALABRAS CLAVE (Leyendas de Komet)
            # Si la columna PO contiene cualquiera de estas, es basura.
            blacklist = [
                "vendor:", "shipimport pandas as pd
import logging
import uuid
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Ingesta 'Confirm POs' con filtro ANTI-LEYENDAS Nuclear.
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

            # 2. Buscar Ancla
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po #" in row_str and "vendor" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla 'Confirm POs'."

            # 3. Reconstrucción
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns]

            # 4. FILTRO NUCLEAR (Anti-Basura)
            col_po = 'PO #' 
            if col_po not in df.columns:
                col_po = next((c for c in df.columns if 'PO' in c and '#' in c), None)
                if not col_po: return "❌ Error columnas."

            # A. Quitar vacíos
            df = df.dropna(subset=[col_po])
            
            # B. Quitar Header repetido
            df = df[df[col_po].astype(str) != col_po]

            # C. LISTA NEGRA DE PALABRAS CLAVE (Leyendas de Komet)
            # Si la columna PO contiene cualquiera de estas, es basura.
            blacklist = [
                "vendor:", "ship date:", "product:", "qty po:", "confirmed:", 
                "b/t:", "total u:", "cost:", "customer:", "origin:", 
                "status:", "mark code:", "ship country:", "location:", 
                "notes for", "report explanation", "total"
            ]
            
            # Convertimos a string minúsculas y filtramos
            pattern = '|'.join(blacklist)
            df = df[~df[col_po].astype(str).str.lower().str.contains(pattern, na=False)]
            
            # D. Filtro por longitud (POs reales > 3 chars)
            df = df[df[col_po].astype(str).str.len() > 3]

            registros = 0
            batch_id = str(uuid.uuid4())[:8]
            items_batch = []

            # 5. Mapeo (Mismo diccionario de antes)
            mapa_flexible = {
                'customer': 'customer_code', 'cust': 'customer_code', 
                'po#': 'po_komet', 'po #': 'po_komet',
                'status': 'status_komet', 'code': 'product_code',
                'descrip': 'product_name', 'product': 'product_name',
                'quantity': 'quantity_boxes', 'qty po': 'quantity_boxes', 
                'uom': 'box_type', 'b/t': 'box_type',          
                'flydate': 'fly_date', 'ship date': 'ship_date',   
                'precio ': 'unit_price_sales', 'cost': 'unit_price_purchase', 
                'preciocompra': 'purchase_price', 'precio venta': 'sales_price',    
                'qty/box': 'bunches_per_box', 'ramos por caja': 'bunches_per_box',
                'customer inv code': 'customer_inv_code', 'mark code': 'mark_code',    
                'type': 'order_type', 'comments': 'notes', 'notes': 'notes',           
                'contents': 'contents', 'bqt': 'bqt', 'upc': 'upc',
                'size': 'size', 'food': 'food', 'sleeve': 'sleeve_type',
                'tallos': 'stems_per_bunch', 'total tallos': 'total_stems',
                'total u': 'total_stems', 'awb': 'awb', 'hija': 'hawb',
                'precio unt st': 'unit_price_stems', 'tallos por cja': 'stems_per_box',
                'tipo de orden': 'order_kind', 'flor': 'flower_type',
                'udv': 'udv', 'pcuc': 'pcuc', 'vc': 'vc', 'pr': 'pr',
                'finca': 'farm_code', 'vendor': 'vendor',          
                '1.25': 'factor_1_25', 'sugerido': 'suggested_price',
                'po# consec': 'po_consecutive', 'valor t': 'valor_t',
                'venta total': 'total_sales_value', 'invoice': 'invoice_number',
                'fact finca': 'farm_invoice', 'conseq': 'consecutive',
                'creditos': 'credits', 'pago contado': 'cash_payment',
                'compra contado': 'cash_purchase'
            }

            # 5. ITERACIÓN
            for idx, row in df.iterrows():
                try:
                    item = {
                        "status": "Pending",
                        "import_batch_id": batch_id,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    for col_excel in df.columns:
                        col_lower = str(col_excel).lower()
                        valor_raw = row[col_excel]
                        
                        for key_map, col_db in mapa_flexible.items():
                            if key_map in col_lower:
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
                    
                    if 'vendor' not in item and 'farm_code' in item: item['vendor'] = item['farm_code']
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
                db_client.table("staging_komet").insert(items_batch).execute()
                registros = len(items_batch)

            return (
                f"📥 **Ingesta Komet Exitosa**\n"
                f"🔖 Lote: `{batch_id}`\n"
                f"📦 Filas LIMPIAS: {registros}\n"
                f"👉 Datos listos en 'staging_komet'."
            )

        except Exception as e:
            return f"💥 Error crítico Ingestor Komet: {e}"

ingestor_komet = IngestorKomet()import pandas as pd
import logging
import uuid
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Ingesta 'Confirm POs' con filtro ANTI-LEYENDAS Nuclear.
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

            # 2. Buscar Ancla
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po #" in row_str and "vendor" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla 'Confirm POs'."

            # 3. Reconstrucción
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns]

            # 4. FILTRO NUCLEAR (Anti-Basura)
            col_po = 'PO #' 
            if col_po not in df.columns:
                col_po = next((c for c in df.columns if 'PO' in c and '#' in c), None)
                if not col_po: return "❌ Error columnas."

            # A. Quitar vacíos
            df = df.dropna(subset=[col_po])
            
            # B. Quitar Header repetido
            df = df[df[col_po].astype(str) != col_po]

            # C. LISTA NEGRA DE PALABRAS CLAVE (Leyendas de Komet)
            # Si la columna PO contiene cualquiera de estas, es basura.
            blacklist = [
                "vendor:", "ship date:", "product:", "qty po:", "confirmed:", 
                "b/t:", "total u:", "cost:", "customer:", "origin:", 
                "status:", "mark code:", "ship country:", "location:", 
                "notes for", "report explanation", "total"
            ]
            
            # Convertimos a string minúsculas y filtramos
            pattern = '|'.join(blacklist)
            df = df[~df[col_po].astype(str).str.lower().str.contains(pattern, na=False)]
            
            # D. Filtro por longitud (POs reales > 3 chars)
            df = df[df[col_po].astype(str).str.len() > 3]

            registros = 0
            batch_id = str(uuid.uuid4())[:8]
            items_batch = []

            # 5. Mapeo (Mismo diccionario de antes)
            mapa_flexible = {
                'customer': 'customer_code', 'cust': 'customer_code', 
                'po#': 'po_komet', 'po #': 'po_komet',
                'status': 'status_komet', 'code': 'product_code',
                'descrip': 'product_name', 'product': 'product_name',
                'quantity': 'quantity_boxes', 'qty po': 'quantity_boxes', 
                'uom': 'box_type', 'b/t': 'box_type',          
                'flydate': 'fly_date', 'ship date': 'ship_date',   
                'precio ': 'unit_price_sales', 'cost': 'unit_price_purchase', 
                'preciocompra': 'purchase_price', 'precio venta': 'sales_price',    
                'qty/box': 'bunches_per_box', 'ramos por caja': 'bunches_per_box',
                'customer inv code': 'customer_inv_code', 'mark code': 'mark_code',    
                'type': 'order_type', 'comments': 'notes', 'notes': 'notes',           
                'contents': 'contents', 'bqt': 'bqt', 'upc': 'upc',
                'size': 'size', 'food': 'food', 'sleeve': 'sleeve_type',
                'tallos': 'stems_per_bunch', 'total tallos': 'total_stems',
                'total u': 'total_stems', 'awb': 'awb', 'hija': 'hawb',
                'precio unt st': 'unit_price_stems', 'tallos por cja': 'stems_per_box',
                'tipo de orden': 'order_kind', 'flor': 'flower_type',
                'udv': 'udv', 'pcuc': 'pcuc', 'vc': 'vc', 'pr': 'pr',
                'finca': 'farm_code', 'vendor': 'vendor',          
                '1.25': 'factor_1_25', 'sugerido': 'suggested_price',
                'po# consec': 'po_consecutive', 'valor t': 'valor_t',
                'venta total': 'total_sales_value', 'invoice': 'invoice_number',
                'fact finca': 'farm_invoice', 'conseq': 'consecutive',
                'creditos': 'credits', 'pago contado': 'cash_payment',
                'compra contado': 'cash_purchase'
            }

            # 5. ITERACIÓN
            for idx, row in df.iterrows():
                try:
                    item = {
                        "status": "Pending",
                        "import_batch_id": batch_id,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    for col_excel in df.columns:
                        col_lower = str(col_excel).lower()
                        valor_raw = row[col_excel]
                        
                        for key_map, col_db in mapa_flexible.items():
                            if key_map in col_lower:
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
                    
                    if 'vendor' not in item and 'farm_code' in item: item['vendor'] = item['farm_code']
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
                db_client.table("staging_komet").insert(items_batch).execute()
                registros = len(items_batch)

            return (
                f"📥 **Ingesta Komet Exitosa**\n"
                f"🔖 Lote: `{batch_id}`\n"
                f"📦 Filas LIMPIAS: {registros}\n"
                f"👉 Datos listos en 'staging_komet'."
            )

        except Exception as e:
            return f"💥 Error crítico Ingestor Komet: {e}"

ingestor_komet = IngestorKomet() date:", "product:", "qty po:", "confirmed:", 
                "b/t:", "total u:", "cost:", "customer:", "origin:", 
                "status:", "mark code:", "ship country:", "location:", 
                "notes for", "report explanation", "total"
            ]
            
            # Convertimos a string minúsculas y filtramos
            pattern = '|'.join(blacklist)
            df = df[~df[col_po].astype(str).str.lower().str.contains(pattern, na=False)]
            
            # D. Filtro por longitud (POs reales > 3 chars)
            df = df[df[col_po].astype(str).str.len() > 3]

            registros = 0
            batch_id = str(uuid.uuid4())[:8]
            items_batch = []

            # 5. Mapeo (Mismo diccionario de antes)
            mapa_flexible = {
                'customer': 'customer_code', 'cust': 'customer_code', 
                'po#': 'po_komet', 'po #': 'po_komet',
                'status': 'status_komet', 'code': 'product_code',
                'descrip': 'product_name', 'product': 'product_name',
                'quantity': 'quantity_boxes', 'qty po': 'quantity_boxes', 
                'uom': 'box_type', 'b/t': 'box_type',          
                'flydate': 'fly_date', 'ship date': 'ship_date',   
                'precio ': 'unit_price_sales', 'cost': 'unit_price_purchase', 
                'preciocompra': 'purchase_price', 'precio venta': 'sales_price',    
                'qty/box': 'bunches_per_box', 'ramos por caja': 'bunches_per_box',
                'customer inv code': 'customer_inv_code', 'mark code': 'mark_code',    
                'type': 'order_type', 'comments': 'notes', 'notes': 'notes',           
                'contents': 'contents', 'bqt': 'bqt', 'upc': 'upc',
                'size': 'size', 'food': 'food', 'sleeve': 'sleeve_type',
                'tallos': 'stems_per_bunch', 'total tallos': 'total_stems',
                'total u': 'total_stems', 'awb': 'awb', 'hija': 'hawb',
                'precio unt st': 'unit_price_stems', 'tallos por cja': 'stems_per_box',
                'tipo de orden': 'order_kind', 'flor': 'flower_type',
                'udv': 'udv', 'pcuc': 'pcuc', 'vc': 'vc', 'pr': 'pr',
                'finca': 'farm_code', 'vendor': 'vendor',          
                '1.25': 'factor_1_25', 'sugerido': 'suggested_price',
                'po# consec': 'po_consecutive', 'valor t': 'valor_t',
                'venta total': 'total_sales_value', 'invoice': 'invoice_number',
                'fact finca': 'farm_invoice', 'conseq': 'consecutive',
                'creditos': 'credits', 'pago contado': 'cash_payment',
                'compra contado': 'cash_purchase'
            }

            # 5. ITERACIÓN
            for idx, row in df.iterrows():
                try:
                    item = {
                        "status": "Pending",
                        "import_batch_id": batch_id,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    for col_excel in df.columns:
                        col_lower = str(col_excel).lower()
                        valor_raw = row[col_excel]
                        
                        for key_map, col_db in mapa_flexible.items():
                            if key_map in col_lower:
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
                    
                    if 'vendor' not in item and 'farm_code' in item: item['vendor'] = item['farm_code']
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
                db_client.table("staging_komet").insert(items_batch).execute()
                registros = len(items_batch)

            return (
                f"📥 **Ingesta Komet Exitosa**\n"
                f"🔖 Lote: `{batch_id}`\n"
                f"📦 Filas LIMPIAS: {registros}\n"
                f"👉 Datos listos en 'staging_komet'."
            )

        except Exception as e:
            return f"💥 Error crítico Ingestor Komet: {e}"

ingestor_komet = IngestorKomet()
