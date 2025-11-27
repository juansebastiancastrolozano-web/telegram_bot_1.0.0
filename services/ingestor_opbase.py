import pandas as pd
import numpy as np
import logging
from datetime import datetime
from services.cliente_supabase import db_client
from services.ai_helper import analizar_texto_con_ia

logger = logging.getLogger(__name__)

class IngestorOPBASE:
    """
    El Historiador Inteligente.
    1. Busca la tabla real (ignora basura inicial).
    2. Mapea columnas exóticas a SQL.
    3. Usa IA para limpiar productos si es necesario.
    """

    def procesar_memoria_historica(self, ruta_archivo: str):
        try:
            # 1. LECTURA CRUDA (Sin asumir headers)
            try:
                if ruta_archivo.endswith('.csv'):
                    df_raw = pd.read_csv(ruta_archivo, header=None)
                else:
                    df_raw = pd.read_excel(ruta_archivo, sheet_name='OPBASE', header=None) 
            except ValueError:
                return "⚠️ No encontré la hoja 'OPBASE'. Verifique el nombre de la pestaña."

            # 2. ESCÁNER DE ANCLA (Buscamos la fila de títulos)
            indice_header = None
            for i, row in df_raw.iterrows():
                # Convertimos fila a texto minúscula
                row_str = " ".join([str(x) for x in row.values]).lower()
                
                # Huella digital de OPBASE: Debe tener 'customer', 'code' y 'descrip'
                if "customer" in row_str and "code" in row_str and "descrip" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la cabecera en OPBASE. (Faltan columnas Customer/Code/Descrip)"

            # 3. RECONSTRUCCIÓN DE LA TABLA
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            
            # Limpieza de nombres de columnas
            df.columns = [str(c).strip() for c in df.columns]
            
            # Validamos que exista la columna Customer ahora sí
            col_cust = next((c for c in df.columns if 'cust' in c.lower()), None)
            if not col_cust:
                return "❌ Error: Encontré la tabla pero no la columna 'Customer'."

            df = df.dropna(subset=[col_cust]) # Borrar filas sin cliente
            
            registros_procesados = 0
            items_batch = []
            errores = 0
            productos_limpiados_ia = 0

            # 4. MAPEO INTELIGENTE
            mapeo_columnas = {
                "po_number": ["PO#", "PO"],
                "status": ["Status"],
                "product_code": ["Code"],
                "boxes": ["Quantity", "Cajas"],
                "box_type": ["UOM"],
                "fly_date": ["FlyDate"],
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
                "awb": ["awb", "AWB"],
                "hawb": ["hija", "HAWB"],
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
                "invoice_number": ["INVOICE", "Factura"],
                "farm_invoice": ["fact finca"],
                "consecutive": ["CONSEQ"],
                "credits": ["CREDITOS"],
                "cash_payment": ["Pago contado"],
                "cash_purchase": ["compra contado"],
                "customer_code": ["Customer", "Cust"]
            }

            # Buscar columna pivote para agrupar (Invoice o PO)
            col_invoice = next((c for c in df.columns if 'invoice' in c.lower()), None)
            if not col_invoice: 
                 col_invoice = next((c for c in df.columns if 'po' in c.lower() and '#' in c.lower()), 'PO#')

            grupos = df.groupby(col_invoice)

            # 5. PROCESAMIENTO DE GRUPOS
            for invoice_num, grupo in grupos:
                try:
                    primera = grupo.iloc[0]
                    
                    # -- Cabecera --
                    col_fly = next((c for c in df.columns if 'fly' in c.lower()), 'FlyDate')
                    col_finca = next((c for c in df.columns if 'finca' in c.lower()), 'finca')
                    col_awb = next((c for c in df.columns if 'awb' in c.lower()), 'awb')
                    col_hija = next((c for c in df.columns if 'hija' in c.lower()), 'hija')

                    fly_date = primera.get(col_fly)
                    try: fecha_vuelo = pd.to_datetime(fly_date).strftime('%Y-%m-%d')
                    except: fecha_vuelo = datetime.now().strftime('%Y-%m-%d')

                    col_qty = next((c for c in df.columns if 'quan' in c.lower()), 'Quantity')
                    col_total = next((c for c in df.columns if 'venta total' in c.lower()), 'VALOR T')
                    
                    total_cajas = int(pd.to_numeric(grupo[col_qty], errors='coerce').sum())
                    total_valor = float(pd.to_numeric(grupo[col_total], errors='coerce').sum())

                    cabecera = {
                        "po_number": f"HIST-{invoice_num}", 
                        "invoice_number": str(invoice_num),
                        "vendor": str(primera.get(col_finca, 'VARIOUS')),
                        "customer_name": str(primera.get(col_cust, 'UNKNOWN')),
                        "ship_date": fecha_vuelo,
                        "flight_date": fecha_vuelo,
                        "awb": str(primera.get(col_awb, '')),
                        "hawb": str(primera.get(col_hija, '')), 
                        "origin": "BOG",
                        "status": "Archived",
                        "is_historical": True,
                        "source_file": "OPBASE_AI_Import",
                        "total_boxes": total_cajas,
                        "total_value": total_valor
                    }
                    
                    # Upsert Cabecera
                    res_head = db_client.table("sales_orders").upsert(cabecera, on_conflict="po_number").execute()
                    # Supabase suele devolver data, si no, buscamos el ID
                    if res_head.data:
                        order_id = res_head.data[0]['id']
                    else:
                        # Fallback: buscar el ID si el upsert no devolvió data
                        res_search = db_client.table("sales_orders").select("id").eq("po_number", cabecera["po_number"]).execute()
                        if res_search.data: order_id = res_search.data[0]['id']
                        else: continue

                    col_desc = next((c for c in df.columns if 'desc' in c.lower()), 'Descrip')
                    col_flor = next((c for c in df.columns if 'flor' in c.lower()), 'flor')

                    # -- Items --
                    for _, row in grupo.iterrows():
                        item = {"order_id": order_id}
                        
                        # IA Limpieza
                        nombre_original = str(row.get(col_desc, ''))
                        tipo_flor_excel = str(row.get(col_flor, ''))
                        
                        item["product_name"] = nombre_original
                        item["flower_type"] = tipo_flor_excel

                        if (not tipo_flor_excel or len(tipo_flor_excel) < 3) and registros_procesados < 20: 
                            # Limite de 20 para prueba rápida de IA
                            datos_ia = analizar_texto_con_ia(nombre_original, "producto")
                            if datos_ia:
                                item["variety"] = datos_ia.get("variety")
                                item["color"] = datos_ia.get("color")
                                item["grade"] = datos_ia.get("grade")
                                if not item["flower_type"]: 
                                    item["flower_type"] = datos_ia.get("flower_type")
                                productos_limpiados_ia += 1

                        # Mapeo Columnas
                        for campo_sql, posibles_nombres_excel in mapeo_columnas.items():
                            valor = None
                            for nombre_excel in posibles_nombres_excel:
                                if nombre_excel in df.columns:
                                    raw_val = row[nombre_excel]
                                    if pd.isna(raw_val) or str(raw_val).strip() == '':
                                        valor = None
                                    else:
                                        if campo_sql in ['boxes', 'total_units', 'stems_per_bunch', 'bunches_per_box']:
                                            try: valor = int(float(raw_val))
                                            except: valor = 0
                                        elif campo_sql in ['sales_price', 'purchase_price', 'total_sales_value', 'credits', 'pcuc', 'vc', 'pr', 'factor_1_25']:
                                            try: valor = float(raw_val)
                                            except: valor = 0.0
                                        else:
                                            valor = str(raw_val)
                                    break
                            if valor is not None:
                                item[campo_sql] = valor

                        items_batch.append(item)

                except Exception:
                    errores += 1
                    continue

            # Inserción por lotes
            if items_batch:
                chunk_size = 100
                for i in range(0, len(items_batch), chunk_size):
                    chunk = items_batch[i:i + chunk_size]
                    try:
                        db_client.table("sales_items").insert(chunk).execute()
                        registros_procesados += len(chunk)
                    except Exception as e:
                        logger.error(f"Error insertando lote OPBASE: {e}")

            return (
                f"🏛️ **Carga Histórica OPBASE (+IA) Finalizada**\n"
                f"📄 Facturas Procesadas: {len(grupos)}\n"
                f"💾 Registros Detallados: {registros_procesados}\n"
                f"🧠 Productos Enriquecidos por IA: {productos_limpiados_ia}\n"
                f"⚠️ Errores de agrupación: {errores}"
            )

        except Exception as e:
            logger.error(f"Error crítico OPBASE: {e}")
            return f"💥 Fallo total: {e}"

ingestor_opbase = IngestorOPBASE()
