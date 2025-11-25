# services/motor_ventas.py
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from services.cliente_supabase import db_client, logger

class GestorPrediccionVentas:
    def __init__(self):
        self.db = db_client

    def _obtener_perfil_cliente(self, codigo_cliente: str):
        """
        Método auxiliar para resolver la identidad del cliente.
        Busca en la tabla maestra 'customers' usando el código (ej. MEXT).
        """
        try:
            # 1. Resolver ID basado en Código (MEXT -> UUID)
            # Nota: Tu JSON muestra campos 'code' y 'customer_code'. 
            # Buscamos en ambos por seguridad usando OR lógico.
            res_id = self.db.table("customers")\
                .select("id, name, code")\
                .or_(f"code.eq.{codigo_cliente},customer_code.eq.{codigo_cliente}")\
                .execute()

            if not res_id.data:
                return None, None

            cliente_maestro = res_id.data[0]
            uuid_cliente = cliente_maestro['id']
            nombre_real = cliente_maestro['name']

            # 2. Consultar Métricas RFM usando el ID exacto (Mucho más rápido que ilike)
            res_rfm = self.db.table("v_customer_rfm")\
                .select("*")\
                .eq("customer_id", uuid_cliente)\
                .execute()
            
            # Si no hay datos RFM (cliente nuevo sin ventas), devolvemos perfil vacío pero con nombre
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
            # Si 'days_since_last_order' no existe en el perfil fusionado, es un cliente virgen.
            dias_inactividad = perfil.get('days_since_last_order')
            ticket_promedio = float(perfil.get('avg_order_value') or 0.0)
            lifetime_orders = int(perfil.get('lifetime_orders') or 0)

            # Paso 3: Lógica de Negocio (El Cerebro)
            
            # ESCENARIO 1: CLIENTE NUEVO (Sin historial RFM)
            if dias_inactividad is None:
                estrategia = "PROSPECCION"
                producto = "Mix de Muestras"
                precio_sugerido = 0.0 # A definir manualmente
                observacion = "Cliente registrado pero sin historial de compras visible."

            # ESCENARIO 2: RIESGO DE FUGA (Churn)
            elif dias_inactividad > 45:
                estrategia = "REACTIVACION"
                producto = "Freedom Red (Oferta Retorno)"
                precio_sugerido = ticket_promedio * 0.92 
                observacion = f"⚠️ ALERTA: Inactivo hace {dias_inactividad} días. Riesgo alto de pérdida."

            # ESCENARIO 3: CLIENTE ACTIVO
            else:
                estrategia = "MANTENIMIENTO"
                producto = "Pedido Recurrente"
                precio_sugerido = ticket_promedio
                observacion = f"Cliente saludable. Última compra hace {dias_inactividad} días."

            # Paso 4: Respuesta Formal
            detalle_sugerencia = {
                "cliente_nombre": perfil.get('name'), # Usamos el nombre real de la tabla customers
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

            # Paso 5: Auditoría
            prediction_id = f"PRED-{perfil['id'][:8]}-{int(datetime.now().timestamp())}"
            
            # Aquí iría tu lógica de guardar en prediction_history...
            # self._registrar_auditoria(...) 

            return prediction_id, detalle_sugerencia

        except Exception as e:
            logger.error(f"Fallo crítico: {e}")
            return None, {"error": str(e)}
