# services/motor_ventas.py
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from services.cliente_supabase import db_client, logger

class GestorPrediccionVentas:
    """
    Motor de inteligencia comercial.
    Convierte datos crudos (RFM) en estrategias de venta accionables.
    """

    def __init__(self):
        self.db = db_client

    def generar_sugerencia_pedido(self, codigo_cliente: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        try:
            # 1. Consultar Vista RFM
            # Usamos 'ilike' para flexibilidad en la búsqueda
            response = self.db.table("v_customer_rfm")\
                .select("*")\
                .ilike("customer_name", f"%{codigo_cliente}%")\
                .execute()

            if not response.data:
                logger.warning(f"Cliente no encontrado: {codigo_cliente}")
                return None, {"error": "Cliente inexistente en base de datos."}

            perfil = response.data[0]
            
            # 2. Saneamiento de Datos (Manejo de NULLs)
            # Si es None, asumimos 0 o un valor centinela negativo
            dias_inactividad = perfil.get('days_since_last_order')
            dias_inactividad = int(dias_inactividad) if dias_inactividad is not None else -1
            
            ticket_promedio = perfil.get('avg_order_value')
            ticket_promedio = float(ticket_promedio) if ticket_promedio is not None else 0.0

            lifetime_orders = perfil.get('lifetime_orders')
            lifetime_orders = int(lifetime_orders) if lifetime_orders is not None else 0

            # 3. Lógica de Segmentación y Estrategia
            # Caso A: Cliente Nuevo o Sin Historial (Los NULLs)
            if dias_inactividad == -1 or lifetime_orders == 0:
                estrategia = "PROSPECCION"
                producto = "Mix de Muestras (Intro)"
                precio_sugerido = 0.30 # Precio gancho para nuevos
                observacion = "Cliente sin historial reciente. Se sugiere envío de portafolio o muestras."

            # Caso B: Cliente en Riesgo (Churn) - EJEMPLO: Mex Y Can (48 días)
            elif dias_inactividad > 45:
                estrategia = "REACTIVACION"
                producto = "Freedom Red (Oferta Retorno)"
                # Aplicamos descuento agresivo del 8% sobre su histórico para recuperarlo
                precio_sugerido = ticket_promedio * 0.92 
                observacion = f"⚠️ ALERTA DE FUGA: Inactivo hace {dias_inactividad} días. Requiere incentivo económico."

            # Caso C: Cliente Activo/Leal
            elif dias_inactividad <= 15:
                estrategia = "FIDELIZACION"
                producto = "Novedades / Tinturados"
                precio_sugerido = ticket_promedio * 1.05 # Upselling
                observacion = f"Cliente activo (hace {dias_inactividad} días). Ofrecer productos premium."

            # Caso D: Cliente Estándar
            else:
                estrategia = "MANTENIMIENTO"
                producto = "Pedido Estándar"
                precio_sugerido = ticket_promedio
                observacion = "Ciclo de compra regular."

            # 4. Construcción del Objeto de Sugerencia
            detalle_sugerencia = {
                "cliente_nombre": perfil.get('customer_name'),
                "estrategia_aplicada": estrategia,
                "producto_objetivo": producto,
                "precio_unitario": round(precio_sugerido, 2),
                "justificacion_tecnica": observacion,
                "metricas_base": {
                    "dias_sin_compra": dias_inactividad,
                    "promedio_historico": ticket_promedio,
                    "ordenes_totales": lifetime_orders
                }
            }

            # 5. Persistencia (Guardar el pensamiento para futuro entrenamiento)
            self._registrar_auditoria(perfil.get('customer_id'), detalle_sugerencia)
            
            # Retornamos un ID temporal generado (o el ID de la auditoría si lo prefieres)
            # Por simplicidad simulamos un ID hash corto aquí, pero deberías usar el ID de la tabla prediction_history
            prediction_id = f"PRED-{perfil.get('customer_id')[:4]}-{int(datetime.now().timestamp())}"
            
            return prediction_id, detalle_sugerencia

        except Exception as e:
            logger.error(f"Fallo crítico en motor de ventas: {e}")
            return None, {"error": str(e)}

    def _registrar_auditoria(self, client_id: str, sugerencia: dict):
        """Método privado para escribir en prediction_history"""
        try:
            payload = {
                "client_id": client_id,
                "input_context": sugerencia["metricas_base"],
                "bot_suggestion": sugerencia,
                "created_at": datetime.utcnow().isoformat()
            }
            self.db.table("prediction_history").insert(payload).execute()
        except Exception as e:
            logger.error(f"No se pudo guardar historial: {e}")

    def registrar_ajuste_usuario(self, prediction_id: str, precio_real: float) -> bool:
        # (Lógica idéntica a la anterior, sin cambios)
        return True
