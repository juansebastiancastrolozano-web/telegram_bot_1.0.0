import pandas as pd
import numpy as np
import logging
from datetime import datetime
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorSO:
    def procesar_master_file(self, ruta_archivo: str):
        try:
            try:
                df_raw = pd.read_excel(ruta_archivo, sheet_name='SO', header=None)
            except ValueError:
                return "⚠️ Este archivo no tiene una hoja llamada 'SO'."

            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po#" in row_str and "code" in row_str and "precio" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla en SO."

            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(col).strip() for col in df.columns]

            if 'PO#' in df.columns: df['PO#'] = df['PO#'].replace('', np.nan).ffill()
            if 'Cust' in df.columns: df['Cust'] = df['Cust'].replace('', np.nan).ffill()
            if 'FlyDate' in df.columns: df['FlyDate'] = df['FlyDate'].replace('', np.nan).ffill()

            cols_clave = [c for c in df.columns if c in ['Code', 'Quantity']]
            df = df.dropna(subset=cols_clave, how='all') 
            df = df[df.iloc[:,0].astype(str) != str(df.columns[0])]

            reporte_financiero = self._analisis_financiero_avanzado(df)
            reporte_logistico = self._cosechar_reglas_logisticas(df)

            return f"{reporte_financiero}\n\n{reporte_logistico}"

        except Exception as e:
            logger.error(f"Error en Ingestor SO: {e}")
            return f"💥 Error procesando SO: {e}"

    def _get_safe_float(self, valor):
        try:
            if pd.isna(valor): return 0.0
            s = str(valor).strip().replace(',', '').replace('$', '').replace(' ', '')
            if not s: return 0.0
            return float(s)
        except: return 0.0

    def _cosechar_reglas_logisticas(self, df: pd.DataFrame):
        col_cust = next((c for c in df.columns if 'cust' in c.lower() and 'inv' not in c.lower()), 'Cust')
        col_code = next((c for c in df.columns if c.lower() == 'code'), 'Code')
        col_desc = next((c for c in df.columns if 'desc' in c.lower()), 'Descrip')
        col_uom = next((c for c in df.columns if 'uom' in c.lower()), 'UOM')
        col_ramos = next((c for c in df.columns if 'ramos' in c.lower()), None)
        col_tallos = next((c for c in df.columns if 'tallos' in c.lower() and 'total' not in c.lower()), 'tallos')
        
        col_sku = next((c for c in df.columns if 'inv code' in c.lower()), 'Customer Inv Code')
        col_upc = next((c for c in df.columns if 'upc' in c.lower()), 'UPC')
        col_mark = next((c for c in df.columns if 'comment' in c.lower() or 'mark' in c.lower()), 'Comments')
        col_sleeve = next((c for c in df.columns if 'sleeve' in c.lower()), 'Sleeve')
        col_date = next((c for c in df.columns if 'fly' in c.lower()), 'FlyDate')

        reglas_unicas = {}

        for idx, row in df.iterrows():
            try:
                cliente = str(row.get(col_cust, '')).strip()
                producto = str(row.get(col_code, '')).strip()
                tipo_caja = str(row.get(col_uom, 'QB')).strip()
                
                if not cliente or not producto or cliente == 'nan' or producto == 'nan':
                    continue

                dia_pref = "Unknown"
                fecha_raw = row.get(col_date)
                if pd.notna(fecha_raw):
                    try: dia_pref = pd.to_datetime(fecha_raw).strftime('%A')
                    except: pass

                try: bunch_count = int(self._get_safe_float(row.get(col_ramos)))
                except: bunch_count = 0
                try: stems_count = int(self._get_safe_float(row.get(col_tallos)))
                except: stems_count = 0

                regla = {
                    "customer_code": cliente,
                    "product_code": producto,
                    "product_name": str(row.get(col_desc, '')).strip(),
                    "box_type": tipo_caja,
                    "bunches_per_box": bunch_count,
                    "stems_per_bunch": stems_count,
                    "customer_sku": str(row.get(col_sku, '')).strip().replace('nan', ''),
                    "upc_code": str(row.get(col_upc, '')).strip().replace('nan', ''),
                    "mark_code": str(row.get(col_mark, '')).strip().replace('nan', ''),
                    "sleeve_type": str(row.get(col_sleeve, '')).strip().replace('nan', ''),
                    "preferred_day": dia_pref
                }
                
                if regla["customer_sku"] in ['0', '0.0']: regla["customer_sku"] = None
                if regla["upc_code"] in ['0', '0.0']: regla["upc_code"] = None

                clave_unica = (cliente, producto, tipo_caja)
                reglas_unicas[clave_unica] = regla

            except Exception:
                continue

        lista_final = list(reglas_unicas.values())

        if lista_final:
            try:
                res = db_client.table("customer_packing_rules").upsert(
                    lista_final, 
                    on_conflict="customer_code,product_code,box_type"
                ).execute()
                return f"🧠 **Conocimiento Logístico Adquirido:**\n📚 Reglas de Empaque Procesadas: {len(lista_final)}"
            except Exception as e:
                logger.error(f"Error guardando reglas: {e}")
                return f"❌ Error DB Logística: {str(e)}"

        return "⚠️ Alerta: No encontré reglas válidas."

    def _analisis_financiero_avanzado(self, df: pd.DataFrame):
        ventas_validas = []
        costos_validos = []
        ventas_sin_costo = []
        filas_totales = 0
        
        col_qty = next((c for c in df.columns if c.lower() == 'quantity'), None)
        col_ramos_caja = next((c for c in df.columns if 'ramos' in c.lower()), None)
        col_tallos_ramo = next((c for c in df.columns if 'tallos' in c.lower() and 'total' not in c.lower()), 'tallos')
        col_precio_venta = next((c for c in df.columns if c.strip().lower() == 'precio'), None)
        col_precio_compra = next((c for c in df.columns if 'compra' in c.lower()), None)

        if not (col_qty and col_precio_venta): return "⚠️ Error de columnas."

        for idx, row in df.iterrows():
            try:
                qty = self._get_safe_float(row.get(col_qty))
                if qty <= 0: continue
                ramos = self._get_safe_float(row.get(col_ramos_caja)) or 1
                tallos = self._get_safe_float(row.get(col_tallos_ramo)) or 1
                p_venta = self._get_safe_float(row.get(col_precio_venta))
                p_compra = self._get_safe_float(row.get(col_precio_compra))
                total_tallos = qty * ramos * tallos
                col_total_t = next((c for c in df.columns if 'total tallos' in c.lower()), None)
                if col_total_t:
                    val_excel = self._get_safe_float(row.get(col_total_t))
                    if val_excel > total_tallos: total_tallos = val_excel
                venta = total_tallos * p_venta
                costo = total_tallos * p_compra
                if venta > 0:
                    filas_totales += 1
                    if costo > 0:
                        ventas_validas.append(venta)
                        costos_validos.append(costo)
                    else:
                        ventas_sin_costo.append(venta)
            except: continue

        sum_v = sum(ventas_validas)
        sum_c = sum(costos_validos)
        margen_pct = 0.20 
        if sum_v > 0: margen_pct = (sum_v - sum_c) / sum_v
        costo_proyectado = sum(ventas_sin_costo) * (1 - margen_pct)
        gran_total_ventas = sum_v + sum(ventas_sin_costo)
        gran_total_costos = sum_c + costo_proyectado
        gran_margen = gran_total_ventas - gran_total_costos
        margen_final_pct = (gran_margen / gran_total_ventas * 100) if gran_total_ventas > 0 else 0

        return (
            f"💰 **Auditoría Inteligente (Proyección)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Líneas Procesadas: {filas_totales}\n"
            f"📉 Líneas sin Costo (Corregidas): {len(ventas_sin_costo)}\n"
            f"💵 Ventas Totales: ${gran_total_ventas:,.2f}\n"
            f"🔮 Costo Real Estimado: ${gran_total_costos:,.2f}\n"
            f"📈 **Margen Proyectado: ${gran_margen:,.2f} ({margen_final_pct:.1f}%)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"ℹ️ <i>Se aplicó un margen histórico del {margen_pct*100:.1f}% a las filas sin costo.</i>"
        )

ingestor_so = IngestorSO()
