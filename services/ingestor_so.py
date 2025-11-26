import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class IngestorSO:
    """
    El Auditor Financiero - Versión 'Rescate Profundo'.
    Analiza la hoja SO, ignora la basura, recupera datos de celdas combinadas
    y audita la rentabilidad real (Venta vs Compra).
    """

    def procesar_master_file(self, ruta_archivo: str):
        try:
            # 1. CIRUGÍA: Abrir solo la hoja 'SO'
            try:
                # Leemos SIN header para encontrar el ancla manualmente
                df_raw = pd.read_excel(ruta_archivo, sheet_name='SO', header=None)
            except ValueError:
                return "⚠️ Este archivo no tiene una hoja llamada 'SO'."

            # 2. ESCÁNER DE ANCLA (Buscamos dónde empieza la verdad)
            indice_header = None
            for i, row in df_raw.iterrows():
                # Convertimos la fila a texto y buscamos palabras clave únicas de esa tabla
                row_str = " ".join([str(x) for x in row.values]).lower()
                
                # La fila clave debe tener 'po#', 'code' y 'precio compra'
                if "po#" in row_str and "code" in row_str and "precio" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla en SO (Falta fila de encabezados clave)."

            # 3. RECONSTRUCCIÓN DE LA TABLA
            # Cortamos la basura de arriba
            df = df_raw.iloc[indice_header + 1:].copy()
            # Asignamos los nombres de columna correctos
            df.columns = df_raw.iloc[indice_header].values
            
            # Limpieza de nombres de columnas (quitar espacios extra)
            df.columns = [str(col).strip() for col in df.columns]

            # --- 4. TÉCNICA DE RESCATE (Forward Fill) ---
            # Si la celda de PO# está vacía (celda combinada), hereda el valor de arriba
            if 'PO#' in df.columns:
                df['PO#'] = df['PO#'].replace('', np.nan).ffill()
            
            if 'Cust' in df.columns:
                df['Cust'] = df['Cust'].replace('', np.nan).ffill()

            # 5. FILTRO INTELIGENTE
            # Solo nos interesan filas que tengan un CÓDIGO DE PRODUCTO o CANTIDAD
            # Esto elimina los totales del final y las filas vacías
            cols_clave = [c for c in df.columns if c in ['Code', 'Quantity']]
            df = df.dropna(subset=cols_clave, how='all') 
            
            # Filtramos si se coló alguna fila repetida del header
            df = df[df.iloc[:,0].astype(str) != str(df.columns[0])]

            return self._analisis_financiero(df)

        except Exception as e:
            logger.error(f"Error en Ingestor SO: {e}")
            return f"💥 Error procesando SO: {e}"

    def _get_safe_float(self, valor):
        """Convierte texto sucio, monedas o nulos a float. Si falla, devuelve 0.0"""
        try:
            if pd.isna(valor): return 0.0
            # Limpieza de caracteres sucios ($ , espacios)
            s = str(valor).strip().replace(',', '').replace('$', '').replace(' ', '')
            if not s: return 0.0
            return float(s)
        except:
            return 0.0

    def _analisis_financiero(self, df: pd.DataFrame):
        total_ventas = 0.0
        total_costos = 0.0
        
        ordenes_analizadas = 0
        filas_omitidas = 0
        alertas_margen = 0      # Margen bajo (<10%)
        alertas_sin_costo = 0   # Costo 0 (Peligro de ilusión)

        # --- MAPEO DE COLUMNAS (INTELIGENCIA DE NOMBRES) ---
        # Buscamos las columnas aunque cambien un poco el nombre en el Excel
        col_qty = next((c for c in df.columns if c.lower() == 'quantity'), None)
        
        # Buscamos algo que diga "ramos" y "caja"
        col_ramos_caja = next((c for c in df.columns if 'ramos' in c.lower() and 'caja' in c.lower()), None)
        
        # Buscamos "tallos" pero que NO sea "total tallos"
        col_tallos_ramo = next((c for c in df.columns if 'tallos' in c.lower() and 'total' not in c.lower()), 'tallos')
        
        # Precio venta (el que dice "precio" a secas o "precio ")
        col_precio_venta = next((c for c in df.columns if c.strip().lower() == 'precio'), None)
        
        # Precio compra (el vital)
        col_precio_compra = next((c for c in df.columns if 'compra' in c.lower()), None)

        if not (col_qty and col_precio_venta):
            return "⚠️ No pude identificar las columnas de Cantidad o Precio Venta."

        # --- CICLO MATEMÁTICO ---
        for idx, row in df.iterrows():
            try:
                qty = self._get_safe_float(row.get(col_qty))
                
                # Si cantidad es 0, no es venta real (o es nota)
                if qty <= 0: 
                    filas_omitidas += 1
                    continue

                # Factores de conversión (Si faltan, asumimos 1 para no multiplicar por cero)
                ramos_x_caja = self._get_safe_float(row.get(col_ramos_caja))
                if ramos_x_caja == 0: ramos_x_caja = 1 # Default

                tallos_x_ramo = self._get_safe_float(row.get(col_tallos_ramo))
                if tallos_x_ramo == 0: tallos_x_ramo = 1 # Default

                # Precios
                p_venta = self._get_safe_float(row.get(col_precio_venta))
                p_compra = self._get_safe_float(row.get(col_precio_compra))

                # CÁLCULO DE MASA (Total Tallos)
                total_tallos = qty * ramos_x_caja * tallos_x_ramo
                
                # Fallback: A veces el Excel ya trae 'total tallos' calculado
                col_total_t = next((c for c in df.columns if 'total tallos' in c.lower()), None)
                if col_total_t:
                    val_excel = self._get_safe_float(row.get(col_total_t))
                    # Si nuestro cálculo dio muy bajo y el Excel dice otra cosa, confiamos en el Excel
                    if val_excel > total_tallos: 
                        total_tallos = val_excel

                # CÁLCULO DE ENERGÍA ($$$)
                venta_linea = total_tallos * p_venta
                costo_linea = total_tallos * p_compra
                
                margen_usd = venta_linea - costo_linea
                
                # Análisis de Anomalías
                if venta_linea > 0:
                    margen_pct = (margen_usd / venta_linea * 100)
                    
                    if p_compra == 0:
                        alertas_sin_costo += 1 # ¡Alerta! Ganancia falsa
                    elif margen_pct < 10:
                        alertas_margen += 1    # ¡Alerta! Negocio malo
                
                # Acumuladores
                total_ventas += venta_linea
                total_costos += costo_linea
                ordenes_analizadas += 1
            
            except Exception:
                filas_omitidas += 1
                continue

        # --- RESULTADO FINAL ---
        margen_global = total_ventas - total_costos
        margen_global_pct = (margen_global / total_ventas * 100) if total_ventas > 0 else 0

        resumen = (
            f"💰 **Auditoría Profunda SO**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Líneas Procesadas: {ordenes_analizadas}\n"
            f"💵 Ventas Totales: ${total_ventas:,.2f}\n"
            f"💸 Costos Totales: ${total_costos:,.2f}\n"
            f"📈 **Margen Real: ${margen_global:,.2f} ({margen_global_pct:.1f}%)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚨 **Alertas de Calidad:**\n"
            f"• Sin Costo Registrado: {alertas_sin_costo} (Inflan la ganancia)\n"
            f"• Margen Crítico (<10%): {alertas_margen}\n"
            f"• Filas Ignoradas: {filas_omitidas}"
        )
        
        return resumen

# Instancia vital para importación
ingestor_so = IngestorSO()
