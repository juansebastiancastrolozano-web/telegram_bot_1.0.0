import pandas as pd
import logging
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorKomet:
    """
    Especialista en digerir reportes sucios de Komet Sales (XLS/CSV).
    Estrategia: Búsqueda de Ancla Dinámica.
    """

    def procesar_archivo(self, ruta_archivo: str):
        """
        Lee el archivo, encuentra la tabla real, limpia los datos y los carga a Supabase.
        """
        try:
            # 1. Carga Cruda (Leemos todo como texto primero para no romper formatos)
            # Usamos header=None para leer desde la fila 0 absoluta
            df_raw = pd.read_csv(ruta_archivo, header=None) if ruta_archivo.endswith('.csv') else pd.read_excel(ruta_archivo, header=None)

            # 2. Búsqueda del Ancla (El Francotirador)
            indice_header = None
            for i, row in df_raw.iterrows():
                # Convertimos la fila a string y buscamos las palabras clave
                fila_str = " ".join([str(x) for x in row.values]).lower()
                if "po #" in fila_str and "vendor" in fila_str and "product" in fila_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla de órdenes. ¿Es el formato correcto?"

            # 3. Reconstrucción de la Tabla
            # Tomamos la fila del ancla como nombres de columna
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            
            # 4. Limpieza Profunda (Ducha de Datos)
            # Eliminamos filas vacías o basura del footer
            df = df.dropna(subset=['PO #']) # Si no tiene PO, es basura
            
            # Normalizamos nombres de columnas (quitamos espacios y puntos)
            df.columns = [str(col).strip().replace('.', '') for col in df.columns]

            return self._cargar_a_supabase_relacional(df)

        except Exception as e:
            logger.error(f"Error ingesta Komet: {e}")
            return f"💥 Error procesando archivo: {str(e)}"

    def _cargar_a_supabase_relacional(self, df: pd.DataFrame):
        """
        Transforma el DataFrame plano en estructura Relacional (Header -> Items)
        y carga a Supabase.
        """
        ordenes_creadas = 0
        items_creados = 0
        errores = []

        # Agrupamos por PO #. 
        # Komet repite el PO # en cada fila. Nosotros necesitamos 1 Header por PO.
        grupos_po = df.groupby('PO #')

        for po_number, grupo in grupos_po:
            try:
                # --- PASO A: CREAR CABECERA (SALES_ORDERS) ---
                # Tomamos los datos de la primera fila del grupo (son comunes)
                primera_fila = grupo.iloc[0]
                
                # Convertir fecha "10/08/2025" a ISO YYYY-MM-DD
                fecha_raw = str(primera_fila.get('Ship Date', ''))
                try:
                    fecha_obj = pd.to_datetime(fecha_raw).strftime('%Y-%m-%d')
                except:
                    fecha_obj = datetime.now().strftime('%Y-%m-%d') # Fallback hoy

                cabecera = {
                    "po_number": str(po_number),
                    "vendor": str(primera_fila.get('Vendor', '')),
                    "ship_date": fecha_obj,
                    "origin": str(primera_fila.get('Origin', 'BOG')),
                    "status": str(primera_fila.get('Status', 'Confirmed')),
                    "source_file": "Komet_Import_XLS",
                    # Sumamos todo el grupo para tener totales en la cabecera
                    "total_boxes": int(pd.to_numeric(grupo['Qty PO'], errors='coerce').sum()),
                    # Asumiendo que Cost * Total U es el valor total, o similar. Ajusta según tu lógica financiera.
                    "total_value": float((pd.to_numeric(grupo['Cost'], errors='coerce') * pd.to_numeric(grupo['Total U'], errors='coerce')).sum())
                }

                # Insertamos Cabecera (upsert para no duplicar si subes el archivo 2 veces)
                # .select() devuelve el ID (existente o nuevo)
                res_head = db_client.table("sales_orders").upsert(cabecera, on_conflict="po_number").select().execute()
                
                if not res_head.data:
                    errores.append(f"Fallo header PO {po_number}")
                    continue
                
                order_id = res_head.data[0]['id']
                ordenes_creadas += 1

                # --- PASO B: CREAR ITEMS (SALES_ITEMS) ---
                items_para_insertar = []
                for _, row in grupo.iterrows():
                    
                    total_units = int(pd.to_numeric(row.get('Total U'), errors='coerce') or 0)
                    cost_unit = float(pd.to_numeric(row.get('Cost'), errors='coerce') or 0)
                    
                    items_para_insertar.append({
                        "order_id": order_id,
                        "customer_code": str(row.get('Customer', '')), # ¡Aquí está el cliente real! (C-WDG)
                        "mark_code": str(row.get('Mark Code', '')),
                        "product_name": str(row.get('Product', '')),
                        "box_type": str(row.get('B/T', 'QB')), # Box Type
                        "boxes": int(pd.to_numeric(row.get('Qty PO'), errors='coerce') or 0),
                        "total_units": total_units,
                        "unit_price": cost_unit,
                        "total_line_value": total_units * cost_unit,
                        "notes": str(row.get('Notes for the vendor', ''))
                    })

                # Insertamos todos los items de esta PO de golpe
                # Primero borramos los anteriores de esta PO para evitar duplicados al re-subir
                db_client.table("sales_items").delete().eq("order_id", order_id).execute()
                
                # Insertamos los nuevos
                db_client.table("sales_items").insert(items_para_insertar).execute()
                items_creados += len(items_para_insertar)

            except Exception as e:
                errores.append(f"Error en PO {po_number}: {str(e)}")

        # Reporte Final
        resumen = f"✅ Procesamiento Komet Finalizado:\n"
        resumen += f"📦 Órdenes (Headers): {ordenes_creadas}\n"
        resumen += f"🌺 Ítems (Detalles): {items_creados}\n"
        if errores:
            resumen += f"\n⚠️ Errores ({len(errores)}): {'; '.join(errores[:3])}..."
        
        return resumen

# Instancia lista para usar
ingestor_komet = IngestorKomet()
