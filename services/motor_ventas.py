# services/motor_ventas.py
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from services.cliente_supabase import db_client, logger

class GestorPrediccionVentas:
    """
    Controlador encargado de analizar el comportamiento histórico de los clientes
    y generar sugerencias de pedidos basadas en métricas RFM.
    """

    def __init__(self):
        self.db = db_client

    def generar_sugerencia_pedido(self, codigo_cliente: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Analiza el perfil del cliente y sugiere una estrategia de venta.
        
        Args:
            codigo_cliente (str): Código único del cliente (ej. 'MEXT').

        Returns:
            Tuple[str, Dict]: ID de la predicción generada y diccionario con detalles de la sugerencia.
        """
        try:
            # 1. Consultar vista analítica de clientes (RFM)
            response = self.db.table("v_customer_rfm")\
                .select("*")\
                .ilike("customer_name", f"%{codigo_cliente}%")\
                .execute()

            if not response.data:
                logger.warning(f"Cliente no encontrado en vista RFM: {codigo_cliente}")
                return None, {"error": "Cliente no encontrado o sin historial reciente."}

            perfil_cliente = response.data[0]
            
            # 2. Análisis de Métricas (Lógica de Negocio)
            dias_inactividad = perfil_cliente.get('days_since_last_order') or 0
            ticket_promedio = float(perfil_cliente.get('avg_order_value') or 0)
            
            # Definición de Estrategia Comercial basada en Recencia
            if dias_inactividad > 45:
                estrategia = "REACTIVACION"
                precio_sugerido = ticket_promedio * 0.95  # 5% Descuento por fidelización
                observacion = f"Cliente en riesgo de fuga (Churn). Inactivo hace {dias_inactividad} días."
            elif dias_inactividad < 7:
                estrategia = "EXPANSION"
                precio_sugerido = ticket_promedio * 1.02  # Ajuste incremental por alta demanda
                observacion = "Cliente activo y recurrente. Oportunidad de Upselling."
            else:
                estrategia = "SOSTENIMIENTO"
                precio_sugerido = ticket_promedio
                observacion = "Comportamiento de compra estándar."

            # Estructura del Pedido Sugerido (JSON Schema)
            detalle_sugerencia = {
                "producto_objetivo": "Freedom Red - 50cm (Sugerido)", # TODO: Conectar con Stock Real
                "precio_unitario": round(precio_sugerido, 2),
                "moneda": "USD",
                "estrategia_aplicada": estrategia,
                "justificacion_tecnica": observacion,
                "metricas_base": {
                    "dias_sin_compra": dias_inactividad,
                    "promedio_historico": ticket_promedio
                }
            }

            # 3. Persistencia: Registrar la sugerencia para futuro entrenamiento (Machine Learning)
            registro_auditoria = {
                "client_id": perfil_cliente.get('customer_id'),
                "input_context": detalle_sugerencia["metricas_base"],
                "bot_suggestion": detalle_sugerencia,
                "user_correction": None,  # Pendiente de validación humana
                "created_at": datetime.utcnow().isoformat()
            }

            insert_response = self.db.table("prediction_history").insert(registro_auditoria).execute()
            
            if insert_response.data:
                prediction_id = insert_response.data[0]['id']
                logger.info(f"Predicción generada exitosamente. ID: {prediction_id}")
                return prediction_id, detalle_sugerencia
            
            return None, {"error": "Fallo al registrar auditoría de predicción."}

        except Exception as e:
            logger.error(f"Excepción en motor de ventas: {e}")
            return None, {"error": f"Error interno del servidor: {str(e)}"}

    def registrar_ajuste_usuario(self, prediction_id: str, precio_real: float) -> bool:
        """
        Registra la corrección del usuario (Human-in-the-loop) para mejorar el modelo.
        """
        try:
            payload = {
                "user_correction": {
                    "precio_cierre": precio_real,
                    "fecha_ajuste": datetime.utcnow().isoformat()
                }
                # Aquí se podría añadir lógica para recalcular embeddings
            }
            
            self.db.table("prediction_history")\
                .update(payload)\
                .eq("id", prediction_id)\
                .execute()
            
            logger.info(f"Ajuste de usuario registrado para predicción {prediction_id}")
            return True
        except Exception as e:
            logger.error(f"Error registrando ajuste: {e}")
            return False
