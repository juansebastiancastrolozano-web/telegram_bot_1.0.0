import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class IngestorSO:
    """
    El Auditor Financiero - Versión Rescate Profundo.
    Utiliza técnicas de 'Forward Fill' para recuperar filas huérfanas por celdas combinadas.
    """

    def procesar_master_file(self, ruta_archivo: str):
        try:
            # 1. CIRUGÍA: Abrir solo la hoja 'SO'
            try:
                # Leemos SIN header primero para no perder nada
                df_raw = pd.read_excel(ruta_archivo, sheet_name='SO', header=None)
            except ValueError:
                return "⚠️ Este archivo no tiene una hoja llamada 'SO'."

            # 2. ESCÁNER DE ANCLA
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                # Buscamos la fila que contiene los títulos clave
                if "po#" in row_str and "quantity" in row_str and "code" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla (Fila de encabezados no detectada)."

            # 3. RECONSTRUCCIÓN
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            
            # Limpieza de nombres de columnas
            df.columns = [str(col).strip() for col in df.columns]

            # --- LA MAGIA DE LA RECUPERACIÓN (FFILL) ---
            # Si la columna PO# está vacía, toma el valor de arriba (para celdas combinadas)
            if 'PO#' in df.columns:
                df['PO#'] = df['PO#'].replace('', np.nan).ffill()
            
            # Si el Cliente (Cust) está vacío, también lo arrastramos
            if 'Cust' in df.columns:
                df['Cust'] = df['Cust'].replace('', np.nan).ffill()

            # 4. FILTRO INTELIGENTE
            # No borramos si falta PO (ya lo llenamos).
            # Borramos solo si NO HAY PRODUCTO ni CANTIDAD. Es decir, si la fila está vacía de verdad.
            # Buscamos columnas clave que indiquen "Aquí hay una flor"
            cols_clave = [c for c in df.columns if c in ['Code', 'Descrip', 'Quantity']]
            df = df.dropna(subset=cols_clave, how='all') 
            
            # Filtramos la fila repetida del header si existe
            df = df[df.iloc[:,0].astype(str) != str(df.columns[0])]

            return self._analisis_financiero(df)

        except Exception as e:
            logger.error(f"Error en Ingestor SO: {e}")
            return f"💥 Error procesando SO: {e}"

    def _get_safe_float(self, valor):
        """Convierte lo que sea en un float o devuelve 0.0"""
        try:
            if pd.isna(valor): return 0.0
            s = str(valor).strip().replace(',', '').replace('$', '')
            if not s: return 0.0
            return float(s)
        except:
            return 0.0

    def _analisis_financiero(self, df: pd.DataFrame):
        total_ventas = 0.0
        total_costos = 0.0
        
        ordenes_analizadas = 0
        filas_omitidas = 0
        alertas_margen = 0

        # Mapeo de columnas (Más flexible)
        col_qty = 'Quantity'
        col_ramos_caja = 'Qty/Box ramos por caja'
        # A veces Komet cambia nombres, intentamos variantes
        col_tallos_ramo = next((c for c in df.columns if 'tallos' in c.lower() and 'total' not in c.lower()), 'tallos')
        col_precio_venta = next((c for c in df.columns if c.strip() == 'precio'), 'precio')
        col_precio_compra = next((c for c in df.columns if 'compra' in c.lower()), 'precio compra')

        for idx, row in df.iterrows():
            try:
                # Extracción
                qty = self._get_safe_float(row.get(col_qty))
                
                # Si cantidad es 0, probablemente es una fila de nota o basura
                if qty <= 0: 
                    filas_omitidas += 1
                    continue

                ramos_x_caja = self._get_safe_float(row.get(col_ramos_caja))
                tallos_x_ramo = self._get_safe_float(row.get(col_tallos_ramo))
                
                p_venta = self._get_safe_float(row.get(col_precio_venta))
                p_compra = self._get_safe_float(row.get(col_precio_compra))

                # LA MATEMÁTICA
                # Si faltan factores de conversión, asumimos 1 para no matar el cálculo
                if ramos_x_caja == 0: ramos_x_caja = 1
                if tallos_x_ramo == 0: tallos_x_ramo = 1

                total_tallos = qty * ramos_x_caja * tallos_x_ramo
                
                # A veces 'total tallos' ya viene calculado en el Excel
                col_total_t = next((c for c in df.columns if 'total tallos' in c.lower()), None)
                if col_total_t:
                    val_excel = self._get_safe_float(row.get(col_total_t))
                    if val_excel > 0: total_tallos = val_excel # Confiamos en el Excel si lo tiene

                venta_total = total_tallos * p_venta
                costo_total = total_tallos * p_compra
                
                margen_usd = venta_total - costo_total
                margen_pct = (margen_usd / venta_total * 100) if venta_total > 0 else 0

                total_ventas += venta_total
                total_costos += costo_total
                ordenes_analizadas += 1

                if margen_pct < 10 and venta_total > 0:
                    alertas_margen += 1
            
            except Exception:
                filas_omitidas += 1
                continue

        # Resumen Final
        margen_global = total_ventas - total_costos
        margen_global_pct = (margen_global / total_ventas * 100) if total_ventas > 0 else 0

        resumen = (
            f"💰 **Auditoría Profunda SO**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Filas Procesadas: {ordenes_analizadas}\n"
            f"🗑️ Filas Basura Omitidas: {filas_omitidas}\n"
            f"💵 Ventas Totales: ${total_ventas:,.2f}\n"
            f"💸 Costos Totales: ${total_costos:,.2f}\n"
            f"📈 **Margen: ${margen_global:,.2f} ({margen_global_pct:.1f}%)**\n"
            f"⚠️ Alertas (Margen <10%): {alertas_margen}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_(Datos recuperados con técnica Forward Fill)_"
        )
        
        return resumen

ingestor_so = IngestorSO()
