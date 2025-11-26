import pandas as pd
import logging
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class IngestorSO:
    """
    El Auditor Financiero.
    Extrae la hoja 'SO' del archivo maestro, ignora la basura y calcula márgenes.
    """

    def procesar_master_file(self, ruta_archivo: str):
        try:
            # 1. CIRUGÍA: Abrir solo la hoja 'SO'
            # No importa cuántas hojas tenga el archivo, solo leemos esta.
            try:
                df_raw = pd.read_excel(ruta_archivo, sheet_name='SO', header=None)
            except ValueError:
                return "⚠️ Este archivo no tiene una hoja llamada 'SO'. ¿Es el archivo correcto?"

            # 2. ESCÁNER DE ANCLA: Buscamos dónde empiezan los datos reales
            # Buscamos la fila que tenga "PO#" y "precio compra" (o "precio ")
            indice_header = None
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if "po#" in row_str and "code" in row_str and "flydate" in row_str:
                    indice_header = i
                    break
            
            if indice_header is None:
                return "❌ No encontré la tabla de datos en la hoja SO. (Falta fila de encabezados)"

            # 3. RECONSTRUCCIÓN
            # Establecemos la fila encontrada como encabezado
            df = df_raw.iloc[indice_header + 1:].copy()
            df.columns = df_raw.iloc[indice_header].values
            
            # Limpieza de columnas (quitamos espacios molestos como "precio ")
            df.columns = [str(col).strip() for col in df.columns]

            # 4. FILTRADO DE BASURA
            # Si no hay PO#, no es una orden, es basura o totales
            df = df.dropna(subset=['PO#'])
            # Filtramos filas vacías o repetidas del encabezado
            df = df[df['PO#'].astype(str) != 'PO#']

            return self._analisis_financiero(df)

        except Exception as e:
            logger.error(f"Error en Ingestor SO: {e}")
            return f"💥 Error procesando SO: {e}"

    def _analisis_financiero(self, df: pd.DataFrame):
        """
        Aquí ocurre la magia matemática. Calculamos Profit y Margen.
        """
        reporte = []
        total_ventas = 0
        total_costos = 0
        
        # Contadores para el resumen
        ordenes_analizadas = 0
        alertas_margen = 0

        # Mapeo de columnas (Basado en tu CSV)
        # Asegúrate que estos nombres coincidan EXACTAMENTE con la fila 24 de tu Excel
        col_qty = 'Quantity'
        col_ramos_caja = 'Qty/Box ramos por caja'
        col_tallos_ramo = 'tallos' # Ojo: a veces se llama 'Stems/Bunch'
        col_precio_venta = 'precio' # A veces tiene un espacio al final "precio "
        col_precio_compra = 'precio compra'

        for _, row in df.iterrows():
            try:
                # Extracción segura de números
                qty = float(pd.to_numeric(row.get(col_qty), errors='coerce') or 0)
                ramos_x_caja = float(pd.to_numeric(row.get(col_ramos_caja), errors='coerce') or 0)
                tallos_x_ramo = float(pd.to_numeric(row.get(col_tallos_ramo), errors='coerce') or 0)
                
                p_venta = float(pd.to_numeric(row.get(col_precio_venta), errors='coerce') or 0)
                p_compra = float(pd.to_numeric(row.get(col_precio_compra), errors='coerce') or 0)

                # LA MATEMÁTICA
                total_tallos = qty * ramos_x_caja * tallos_x_ramo
                
                venta_total = total_tallos * p_venta
                costo_total = total_tallos * p_compra
                
                margen_usd = venta_total - costo_total
                
                # Evitar división por cero
                margen_porcentaje = (margen_usd / venta_total * 100) if venta_total > 0 else 0

                # Acumuladores
                total_ventas += venta_total
                total_costos += costo_total
                ordenes_analizadas += 1

                # Detección de Anomalías (Margen negativo o muy bajo)
                if margen_porcentaje < 10 and venta_total > 0:
                    alertas_margen += 1
                    # Aquí podrías guardar en una tabla de 'alertas' en Supabase
            
            except Exception:
                continue

        # Resultado Final
        margen_global = total_ventas - total_costos
        margen_global_pct = (margen_global / total_ventas * 100) if total_ventas > 0 else 0

        resumen = (
            f"💰 **Auditoría de Standing Orders (SO)**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 Órdenes Analizadas: {ordenes_analizadas}\n"
            f"💵 Ventas Totales: ${total_ventas:,.2f}\n"
            f"💸 Costos Totales: ${total_costos:,.2f}\n"
            f"📈 **Margen Global: ${margen_global:,.2f} ({margen_global_pct:.1f}%)**\n"
            f"⚠️ Alertas de Margen Bajo (<10%): {alertas_margen}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"_(Datos extraídos de la hoja SO)_"
        )
        
        return resumen

# Instancia para importar
ingestor_so = IngestorSO()
