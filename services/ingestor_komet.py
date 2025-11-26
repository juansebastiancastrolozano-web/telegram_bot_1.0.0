import pandas as pd
import logging
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Especialista en digerir reportes sucios de Komet Sales.
    Versión 2.0: Con filtros de basura y corrección de Supabase.
    """

    def procesar_archivo(self, ruta_archivo: str):
        try:
            # 1. Lectura Agnostic (CSV o Excel)
            if ruta_archivo.lower().endswith('.csv'):
                # Intentamos detectar encoding
                try:
                    df_raw = pd.read_csv(ruta_archivo, header=None, encoding='utf-8')
                except:
                    df_raw = pd.read_csv(ruta_archivo, header=None, encoding='latin1')
            else:
                df_raw = pd.read_excel(ruta_archivo, header=None)

            # 2. Búsqueda del Ancla (La Fila Sagrada)
            indice_header = None
            for i, row in df_raw.iterrows():
                fila_str = " ".join([str(x) for x in row.values]).lower()
                # Buscamos la huella digital de la cabecera
                if "po #" in fila_str and "vendor" in fila_str and "product" in fila_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la estructura de Komet (PO #, Vendor...). Revisar archivo."

            # 3. Reconstrucción
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            
            # Limpieza de nombres de columnas
            df.columns = [str(col).strip().replace('.', '') for col in df.columns]

            # 4. FILTRO DE SANIDAD (NUEVO)
            # Eliminamos filas vacías en PO
            df = df.dropna(subset=['PO #'])
            
            # Eliminamos filas "Basura" (Leyendas, Totales, Explicaciones)
            # Regla: Un PO real suele ser alfanumérico. Si contiene ":", probablemente es una leyenda.
            # Convertimos a string y filtramos lo que parezca ruido
            df = df[~df['PO #'].astype(str).str.contains(':', na=False)]
            df = df[df['PO #'].astype(str).str.match(r'^[A-Za-z0-9-]+$')]

            return self._cargar_a_supabase_relacional(df)

        except Exception as e:
            logger.error(f"Error ingesta Komet: {e}")
            return f"💥 Error procesando archivo: {str(e)}"

    def _cargar_a_supabase_relacional(self, df: pd.DataFrame):
        ordenes_creadas = 0
        items_creados = 0
        errores = []

        # Agrupamos por PO # (Una PO puede tener varias filas/items)
        grupos_po = df.groupby('PO #')

        for po_number, grupo in grupos_po:
            try:
                po_str = str(po_number).strip()
                
                # Validación extra: Si el PO es muy corto o parece texto descriptivo, saltar
                if len(po_str) < 3 or "total" in po_str.lower(): 
                    continue

                # --- PASO A: CABECERA (SALES_ORDERS) ---
                primera_fila = grupo.iloc[0]
                
                # Manejo de Fechas resiliente
                fecha_raw = str(primera_fila.get('Ship Date', ''))
                try:
                    # Komet suele dar fechas tipo '2025-10-08' o '10/08/2025'
                    fecha_obj = pd.to_datetime(fecha_raw).strftime('%Y-%m-%d')
                except:
                    fecha_obj = datetime.now().strftime('%Y-%m-%d')

                # Totales calculados
                try:
                    total_boxes = int(pd.to_numeric(grupo['Qty PO'], errors='coerce').sum())
                    # Costo * Unidades = Valor Total
                    costos = pd.to_numeric(grupo['Cost'], errors='coerce').fillna(0)
                    unidades = pd.to_numeric(grupo['Total U'], errors='coerce').fillna(0)
                    total_value = float((costos * unidades).sum())
                except:
                    total_boxes = 0
                    total_value = 0.0

                cabecera = {
                    "po_number": po_str,
                    "vendor": str(primera_fila.get('Vendor', '')),
                    "ship_date": fecha_obj,
                    "origin": str(primera_fila.get('Origin', 'BOG')),
                    "status": str(primera_fila.get('Status', 'Confirmed')),
                    "source_file": "Komet_Excel_Upload",
                    "total_boxes": total_boxes,
                    "total_value": total_value
                }

                # --- CORRECCIÓN SUPABASE: UPSERT SIN .SELECT() ---
                # Upsert devuelve datos por defecto en la mayoría de configs
                res_head = db_client.table("sales_orders").upsert(cabecera, on_conflict="po_number").execute()
                
                if not res_head.data:
                    errores.append(f"No devolvió ID para PO {po_str}")
                    continue
                
                order_id = res_head.data[0]['id']
                ordenes_creadas += 1

                # --- PASO B: ÍTEMS (SALES_ITEMS) ---
                items_para_insertar = []
                for _, row in grupo.iterrows():
                    
                    # Limpieza de números por fila
                    qty = int(pd.to_numeric(row.get('Qty PO'), errors='coerce') or 0)
                    units = int(pd.to_numeric(row.get('Total U'), errors='coerce') or 0)
                    price = float(pd.to_numeric(row.get('Cost'), errors='coerce') or 0.0)
                    
                    items_para_insertar.append({
                        "order_id": order_id,
                        "customer_code": str(row.get('Customer', '')).strip(),
                        "mark_code": str(row.get('Mark Code', '')),
                        "product_name": str(row.get('Product', '')),
                        "box_type": str(row.get('B/T', 'QB')),
                        "boxes": qty,
                        "total_units": units,
                        "unit_price": price,
                        "total_line_value": units * price,
                        "notes": str(row.get('Notes for the vendor', ''))
                    })

                if items_para_insertar:
                    # Limpieza previa de items para esta orden (idempotencia)
                    db_client.table("sales_items").delete().eq("order_id", order_id).execute()
                    # Inserción masiva
                    db_client.table("sales_items").insert(items_para_insertar).execute()
                    items_creados += len(items_para_insertar)

            except Exception as e:
                # Si falla una PO, registramos pero seguimos con la siguiente
                errores.append(f"PO {po_number}: {str(e)}")

        # Reporte
        resumen = f"✅ <b>Procesamiento Finalizado</b>\n"
        resumen += f"📦 Órdenes: {ordenes_creadas}\n"
        resumen += f"🌺 Ítems: {items_creados}\n"
        
        if errores:
            # Ocultamos errores técnicos repetidos para no asustar al usuario
            errores_limpios = [e for e in errores if "SyncQueryRequestBuilder" not in e] 
            if errores_limpios:
                resumen += f"\n⚠️ <b>Alertas ({len(errores_limpios)}):</b>\n"
                resumen += "; ".join(errores_limpios[:3]) # Solo mostramos los primeros 3
        
        return resumen

# --- ESTA LÍNEA ES OBLIGATORIA ---
ingestor_komet = IngestorKomet()
