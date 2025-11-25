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

    def _obtener_perfil_cliente(self, codigo_cliente: str):
        """
        Método auxiliar para resolver la identidad del cliente.
        """
        try:
            # 1. Resolver ID basado en Código (MEXT -> UUID)
            res_id = self.db.table("customers")\
                .select("id, name, code")\
                .or_(f"code.eq.{codigo_cliente},customer_code.eq.{codigo_cliente}")\
                .execute()

            if not res_id.data:
                return None, None

            cliente_maestro = res_id.data[0]
            uuid_cliente = cliente_maestro['id']

            # 2. Consultar Métricas RFM usando el ID exacto
            res_rfm = self.db.table("v_customer_rfm")\
                .select("*")\
                .eq("customer_id", uuid_cliente)\
                .execute()
            
            perfil_rfm = res_rfm.data[0] if res_rfm.data else {}
            
            # Fusionamos la identidad con la estadística
            perfil_completo = {**cliente_maestro, **perfil_rfm}
            return perfil_completo, None

        except Exception as e:
            return None, str(e)

    def generar_sugerencia_pedido(self, codigo_cliente: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        try:
            # Paso 1: Identificación Robusta
            perfil, error = self._obtener_perfil_cliente(codigo_cliente)
            
            if error:
                logger.error(f"Error DB: {error}")
                return None, {"error": "Error de conexión."}
            
            if not perfil:
                return None, {"error": f"El código '{codigo_cliente}' no existe en el maestro de clientes."}

            # Paso 2: Extracción y Saneamiento de Métricas
            dias_inactividad = perfil.get('days_since_last_order')
            ticket_promedio = float(perfil.get('avg_order_value') or 0.0)
            lifetime_orders = int(perfil.get('lifetime_orders') or 0)

            # Paso 3: Lógica de Negocio
            if dias_inactividad is None:
                estrategia = "PROSPECCION"
                producto = "Mix de Muestras"
                precio_sugerido = 0.0
                observacion = "Cliente registrado pero sin historial de compras visible."

            elif dias_inactividad > 45:
                estrategia = "REACTIVACION"
                producto = "Freedom Red (Oferta Retorno)"
                precio_sugerido = ticket_promedio * 0.92 
                observacion = f"⚠️ ALERTA: Inactivo hace {dias_inactividad} días. Riesgo alto de pérdida."

            else:
                estrategia = "MANTENIMIENTO"
                producto = "Pedido Recurrente"
                precio_sugerido = ticket_promedio
                observacion = f"Cliente saludable. Última compra hace {dias_inactividad} días."

            # Paso 4: Respuesta Formal
            detalle_sugerencia = {
                "cliente_nombre": perfil.get('name'),
                "codigo_interno": perfil.get('code'),
                "estrategia_aplicada": estrategia,
                "producto_objetivo": producto,
                "precio_unitario": round(precio_sugerido, 2),
                "justificacion_tecnica": observacion,
                "metricas_base": {
                    "dias_sin_compra": dias_inactividad if dias_inactividad is not None else "N/A",
                    "promedio_historico": ticket_promedio
                }
            }

            # Paso 5: Auditoría y obtención de ID REAL
            # IMPORTANTE: Obtenemos el ID real de la base de datos, no uno inventado.
            prediction_id = self._registrar_auditoria(perfil.get('id'), detalle_sugerencia)
            
            return prediction_id, detalle_sugerencia

        except Exception as e:
            logger.error(f"Fallo crítico: {e}")
            return None, {"error": str(e)}

    def _registrar_auditoria(self, client_id: str, sugerencia: dict):
        """Escribe en prediction_history y retorna el UUID generado"""
        try:
            payload = {
                "client_id": client_id,
                "input_context": sugerencia["metricas_base"],
                "bot_suggestion": sugerencia,
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Ejecutamos insert. Supabase devuelve data por defecto si la tabla tiene permisos.
            # No encadenamos .select() para evitar problemas de versión.
            response = self.db.table("prediction_history").insert(payload).execute()
            
            # Verificamos si hay datos en la respuesta
            if response.data and len(response.data) > 0:
                return response.data[0]['id'] # Retornamos el UUID real
            
            logger.error("Se insertó pero no devolvió ID. Revisar permisos RLS en Supabase.")
            return None

        except Exception as e:
            logger.error(f"No se pudo guardar historial: {e}")
            # Fallback: Generamos un ID temporal si falla la base de datos para no romper el flujo del bot
            return f"TEMP-{int(datetime.now().timestamp())}"

    def registrar_ajuste_usuario(self, prediction_id: str, precio_real: float) -> bool:
        """
        Registra la corrección del usuario (Human-in-the-loop).
        """
        try:
            # Si es un ID temporal, no podemos guardar en DB
            if str(prediction_id).startswith("TEMP-"):
                logger.warning("Intento de actualizar un ID temporal. Ignorando.")
                return False

            payload = {
                "user_correction": {
                    "precio_cierre": precio_real,
                    "fecha_ajuste": datetime.utcnow().isoformat()
                }
            }
            
            # Actualizamos usando el ID
            self.db.table("prediction_history")\
                .update(payload)\
                .eq("id", prediction_id)\
                .execute()
            
            logger.info(f"Ajuste guardado para ID {prediction_id}")
            return True
        except Exception as e:
            logger.error(f"Error registrando ajuste en DB: {e}")
            return False
