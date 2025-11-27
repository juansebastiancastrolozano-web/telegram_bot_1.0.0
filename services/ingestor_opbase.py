import pandas as pd
import numpy as np
import logging
from datetime import datetime
from services.cliente_supabase import db_client
from services.ai_helper import analizar_texto_con_ia

logger = logging.getLogger(__name__)

class IngestorOPBASE:
    """
    El Historiador Inteligente (Versión Corregida - Mapeo Limpio).
    """

    def _limpiar_numero(self, valor):
        if pd.isna(valor): return 0.0
        s = str(valor).strip()
        if not s: return 0.0
        s = s.replace('$', '').replace(' ', '')
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            s = s.replace(',', '.')
        try: return float(s)
        except: return 0.0

    def _limpiar_fecha(self, valor):
        if pd.isna(valor): return datetime.now().strftime('%Y-%m-%d')
        s = str(valor).strip()
        try:
            return pd.to_datetime(s).strftime('%Y-%m-%d')
        except:
            return datetime.now().strftime('%Y-%m-%d')

    def procesar_memoria_historica(self, ruta_archivo: str):
        try:
            # 1. LECTURA
            try:
                if ruta_archivo.endswith('.csv'):
                    df_raw = pd.read_csv(ruta_archivo, header=None)
                else:
                    df_raw = pd.read_excel(ruta_archivo, sheet_name='OPBASE', header=None) 
            except ValueError:
                return "⚠️ No encontré la hoja 'OPBASE'."

            # 2. ESCÁNER DE ANCLA
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "customer" in row_str and "code" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la cabecera en OPBASE."

            # 3. RECONSTRUCCIÓN
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(c).strip() for c in df.columns]
            
            col_cust = next((c for c in df.columns if 'cust' in c.lower()), None)
            if not col_cust: return "❌ Error: Sin columna Customer."

            df = df.dropna(subset=[col_cust])
            
            registros_procesados = 0
            errores_log = []
            productos_limpiados_ia = 0

            # 4. MAPEO (SOLO COLUMNAS QUE EXISTEN EN SALES_ITEMS)
            # Quitamos po_number, status, awb, hawb porque van en sales_orders
            mapeo_columnas = {
                "product_code": ["Code"],
                "boxes": ["Quantity", "Cajas"],
                "box_type": ["UOM"],
                "sales_price": ["precio", "PRECIO VENTA"],
                "purchase_price": ["PreciocOMPRA", "Precio Compra"],
                "bunches_per_box": ["Qty/Box ramos por caja", "Qty/Box"],
                "customer_inv_code": ["Customer Inv Code"],
                "order_type": ["Type"],
                "comments": ["Comments"],
                "contents": ["Contents"],
                "bqt": ["BQT"],
                "upc": ["UPC"],
                "size": ["Size"],
                "food": ["Food"],
                "sleeve_type": ["Carton //Sleeve", "Sleeve"],
                "stems_per_bunch": ["tallos"],
                "total_units": ["total tallos"],
                "unit_price_stems": ["precio unt st"],
                "stems_per_box": ["tallos por cja"],
                "order_kind": ["tipo de orden"],
                "udv": ["UDV"],
                "pcuc": ["PCUC"],
                "vc": ["vc"],
                "pr": ["pr"],
                "farm_code": ["finca"],
                "factor_1_25": ["1.25"],
                "suggested_price": ["sugerido"],
                "po_consecutive": ["po# consec"],
                "valor_t": ["VALOR T"],
                "total_sales_value": ["venta total"],
                "farm_invoice": ["fact finca"],
                "consecutive": ["CONSEQ"],
                "credits": ["CREDITOS"],
                "cash_payment": ["Pago contado"],
                "cash_purchase": ["compra contado"],
                "customer_code": ["Customer", "Cust"]
            }

            col_invoice = next((c for c in df.columns if 'invoice' in c.lower()), None)
            if not col_invoice: col_invoice = next((c for c in df.columns if 'po' in c.lower() and '#' in c.lower()), 'PO#')

            grupos = df.groupby(col_invoice)

            for invoice_num, grupo in grupos:
                try:
                    primera = grupo.iloc[0]
                    
                    # HEADER DATA
                    col_fly = next((c for c in df.columns if 'fly' in c.lower()), None)
                    col_finca = next((c for c in df.columns if 'finca' in c.lower()), None)
                    col_awb = next((c for c in df.columns if 'awb' in c.lower()), None)
                    col_hija = next((c for c in df.columns if 'hija' in c.lower()), None)
                    col_qty = next((c for c in df.columns if 'quan' in c.lower()), None)
                    col_total = next((c for c in df.columns if 'venta total' in c.lower()), None)
                    col_po = next((c for c in df.columns if 'po' in c.lower()), 'PO#') # PO Real
                    
                    total_cajas = int(sum([self._limpiar_numero(x) for x in grupo[col_qty]])) if col_qty else 0
                    total_valor = float(sum([self._limpiar_numero(x) for x in grupo[col_total]])) if col_total else 0.0

                    # Usamos el PO real si existe, si no inventamos uno HIST
                    po_real = str(primera.get(col_po, '')).strip()
                    if not po_real or po_real.lower() == 'nan':
                        po_real = f"HIST-{invoice_num}"

                    cabecera = {
                        "po_number": po_real, 
                        "invoice_number": str(invoice_num),
                        "vendor": str(primera.get(col_finca, 'VARIOUS')),
                        "customer_name": str(primera.get(col_cust, 'UNKNOWN')),
                        "ship_date": self._limpiar_fecha(primera.get(col_fly)),
                        "flight_date": self._limpiar_fecha(primera.get(col_fly)),
                        "awb": str(primera.get(col_awb, '')),
                        "hawb": str(primera.get(col_hija, '')), 
                        "origin": "BOG",
                        "status": "Archived",
                        "is_historical": True,
                        "source_file": "OPBASE_Import",
                        "total_boxes": total_cajas,
                        "total_value": total_valor
                    }
                    
                    db_client.table("sales_orders").upsert(cabecera, on_conflict="po_number").execute()
                    
                    res_search = db_client.table("sales_orders").select("id").eq("po_number", po_real).execute()
                    
                    if not res_search.data: 
                        errores_log.append(f"No se pudo recuperar ID para {po_real}")
                        continue
                        
                    order_id = res_search.data[0]['id']

                    # ITEM DATA
                    items_batch = []
                    col_desc = next((c for c in df.columns if 'desc' in c.lower()), 'Descrip')
                    col_flor = next((c for c in df.columns if 'flor' in c.lower()), 'flor')

                    for _, row in grupo.iterrows():
                        item = {"order_id": order_id}
                        
                        nombre_prod = str(row.get(col_desc, ''))
                        tipo_flor = str(row.get(col_flor, ''))
                        item["product_name"] = nombre_prod
                        item["flower_type"] = tipo_flor

                        if (not tipo_flor or len(tipo_flor) < 3) and registros_procesados < 10: 
                            datos_ia = analizar_texto_con_ia(nombre_prod, "producto")
                            if datos_ia:
                                item["variety"] = datos_ia.get("variety")
                                item["color"] = datos_ia.get("color")
                                item["grade"] = datos_ia.get("grade")
                                if not item["flower_type"]: 
                                    item["flower_type"] = datos_ia.get("flower_type")
                                productos_limpiados_ia += 1

                        for campo_sql, posibles in mapeo_columnas.items():
                            val_final = None
                            for nombre_excel in posibles:
                                if nombre_excel in df.columns:
                                    raw = row[nombre_excel]
                                    if campo_sql in ['boxes', 'total_units', 'stems_per_bunch', 'bunches_per_box', 'stems_per_box']:
                                        val_final = int(self._limpiar_numero(raw))
                                    elif campo_sql in ['sales_price', 'purchase_price', 'total_sales_value', 'credits', 'pcuc', 'vc', 'pr', 'factor_1_25', 'valor_t', 'suggested_price', 'unit_price_stems', 'cash_payment', 'cash_purchase']:
                                        val_final = float(self._limpiar_numero(raw))
                                    else:
                                        if pd.notna(raw): val_final = str(raw)
                                    break
                            
                            if val_final is not None:
                                item[campo_sql] = val_final

                        items_batch.append(item)

                    if items_batch:
                        db_client.table("sales_items").delete().eq("order_id", order_id).execute()
                        db_client.table("sales_items").insert(items_batch).execute()
                        registros_procesados += len(items_batch)

                except Exception as e:
                    errores_log.append(f"Fallo en Invoice {invoice_num}: {str(e)}")
                    continue

            msg_error = ""
            if errores_log:
                msg_error =f"\n⚠️ Último error: {errores_log[-1]}"

            return (
                f"🏛️ **Carga Histórica Finalizada**\n"
                f"📄 Facturas: {len(grupos)}\n"
                f"💾 Items Guardados: {registros_procesados}\n"
                f"🧠 IA Usada: {productos_limpiados_ia}{msg_error}"
            )

        except Exception as e:
            logger.error(f"Error crítico OPBASE: {e}")
            return f"💥 Fallo total: {e}"

ingestor_opbase = IngestorOPBASE()
