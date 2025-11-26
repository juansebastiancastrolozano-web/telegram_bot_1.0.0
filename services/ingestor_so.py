import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class IngestorSO:
    """
    El Auditor Financiero - Versión 'Inteligencia Estadística'.
    1. Rescata datos con Forward Fill.
    2. Calcula el margen real de las filas válidas.
    3. Imputa costos a las filas vacías basándose en el promedio histórico.
    """

    def procesar_master_file(self, ruta_archivo: str):
        try:
            # 1. CIRUGÍA: Abrir hoja SO
            try:
                df_raw = pd.read_excel(ruta_archivo, sheet_name='SO', header=None)
            except ValueError:
                return "⚠️ Este archivo no tiene una hoja llamada 'SO'."

            # 2. ESCÁNER DE ANCLA
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po#" in row_str and "code" in row_str and "precio" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla en SO."

            # 3. RECONSTRUCCIÓN
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            df.columns = [str(col).strip() for col in df.columns]

            # --- 4. TÉCNICA DE RESCATE (Forward Fill) ---
            if 'PO#' in df.columns:
                df['PO#'] = df['PO#'].replace('', np.nan).ffill()
            if 'Cust' in df.columns:
                df['Cust'] = df['Cust'].replace('', np.nan).ffill()

            # 5. FILTRO INTELIGENTE
            cols_clave = [c for c in df.columns if c in ['Code', 'Quantity']]
            df = df.dropna(subset=cols_clave, how='all') 
            df = df[df.iloc[:,0].astype(str) != str(df.columns[0])]

            return self._analisis_financiero_avanzado(df)

        except Exception as e:
            logger.error(f"Error en Ingestor SO: {e}")
            return f"💥 Error procesando SO: {e}"

    def _get_safe_float(self, valor):
        try:
            if pd.isna(valor): return 0.0
            s = str(valor).strip().replace(',', '').replace('$', '').replace(' ', '')
            if not s: return 0.0
            return float(s)
        except:
            return 0.0

    def _analisis_financiero_avanzado(self, df: pd.DataFrame):
        """
        Calcula métricas reales y proyectadas para corregir los costos cero.
        """
        # Listas para almacenar datos y hacer cálculos vectoriales
        ventas_validas = []
        costos_validos = []
        
        ventas_sin_costo = []
        
        col_qty = next((c for c in df.columns if c.lower() == 'quantity'), None)
        col_ramos_caja = next((c for c in df.columns if 'ramos' in c.lower()), None)
        col_tallos_ramo = next((c for c in df.columns if 'tallos' in c.lower() and 'total' not in c.lower()), 'tallos')
        col_precio_venta = next((c for c in df.columns if c.strip().lower() == 'precio'), None)
        col_precio_compra = next((c for c in df.columns if 'compra' in c.lower()), None)

        if not (col_qty and col_precio_venta):
            return "⚠️ Error de columnas."

        filas_totales = 0
        
        # --- FASE 1: EXTRACCIÓN Y CLASIFICACIÓN ---
        for idx, row in df.iterrows():
            try:
                qty = self._get_safe_float(row.get(col_qty))
                if qty <= 0: continue

                # Factores
                ramos = self._get_safe_float(row.get(col_ramos_caja)) or 1
                tallos = self._get_safe_float(row.get(col_tallos_ramo)) or 1
                
                p_venta = self._get_safe_float(row.get(col_precio_venta))
                p_compra = self._get_safe_float(row.get(col_precio_compra))

                # Cálculo Masa
                total_tallos = qty * ramos * tallos
                
                # Fallback con columna 'total tallos'
                col_total_t = next((c for c in df.columns if 'total tallos' in c.lower()), None)
                if col_total_t:
                    val_excel = self._get_safe_float(row.get(col_total_t))
                    if val_excel > total_tallos: total_tallos = val_excel

                venta_total = total_tallos * p_venta
                costo_total = total_tallos * p_compra

                if venta_total > 0:
                    filas_totales += 1
                    if costo_total > 0:
                        # Datos Sanos
                        ventas_validas.append(venta_total)
                        costos_validos.append(costo_total)
                    else:
                        # Datos Enfermos (Sin costo)
                        ventas_sin_costo.append(venta_total)
            
            except:
                continue

        # --- FASE 2: ESTADÍSTICA INFERENCIAL ---
        
        sum_ventas_validas = sum(ventas_validas)
        sum_costos_validos = sum(costos_validos)
        sum_ventas_huerfanas = sum(ventas_sin_costo)
        
        # 1. Calcular Margen Promedio Real (de lo que sí tiene datos)
        margen_promedio_pct = 0.20 # Default conservador (20%)
        if sum_ventas_validas > 0:
            margen_promedio_pct = (sum_ventas_validas - sum_costos_validos) / sum_ventas_validas

        # 2. Imputar Costos a las huérfanas
        # Si vendí $100 y mi margen promedio es 20%, mi costo estimado es $80.
        costo_proyectado_huerfanas = sum_ventas_huerfanas * (1 - margen_promedio_pct)

        # 3. Totales Consolidados
        gran_total_ventas = sum_ventas_validas + sum_ventas_huerfanas
        gran_total_costos_proyectado = sum_costos_validos + costo_proyectado_huerfanas
        gran_margen_usd = gran_total_ventas - gran_total_costos_proyectado
        
        # Formateo
        margen_final_pct = (gran_margen_usd / gran_total_ventas * 100) if gran_total_ventas > 0 else 0

        resumen = (
            f"💰 **Auditoría Inteligente (Proyección)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Líneas Procesadas: {filas_totales}\n"
            f"📉 Líneas sin Costo (Corregidas): {len(ventas_sin_costo)}\n"
            f"💵 Ventas Totales: ${gran_total_ventas:,.2f}\n"
            f"🔮 Costo Real Estimado: ${gran_total_costos_proyectado:,.2f}\n"
            f"📈 **Margen Proyectado: ${gran_margen_usd:,.2f} ({margen_final_pct:.1f}%)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"ℹ️ <i>Se aplicó un margen histórico del {margen_promedio_pct*100:.1f}% a las filas sin costo para corregir la distorsión.</i>"
        )
        
        return resumen

ingestor_so = IngestorSO()
