import uuid
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from services.cliente_supabase import db_client, logger

class GestorPrediccionVentas:
    """
    Motor de inteligencia comercial y materialización de ventas.
    """

    def __init__(self):
        self.db = db_client

    def _obtener_perfil_cliente(self, codigo_cliente: str):
        """Método auxiliar para resolver la identidad del cliente."""
        try:
            # 1. Resolver ID basado en Código
            res_id = self.db.table("customers")\
                .select("id, name, code")\
                .or_(f"code.eq.{codigo_cliente},customer_code.eq.{codigo_cliente}")\
                .execute()

            if not res_id.data:
                return None, None

            cliente_maestro = res_id.data[0]
            uuid_cliente = cliente_maestro['id']

            # 2. Consultar Métricas RFM
            res_rfm = self.db.table("v_customer_rfm")\
                .select("*")\
                .eq("customer_id", uuid_cliente)\
                .execute()
            
            perfil_rfm = res_rfm.data[0] if res_rfm.data else {}
            return {**cliente_maestro, **perfil_rfm}, None

        except Exception as e:
            return None, str(e)

    def generar_sugerencia_pedido(self, codigo_cliente: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        try:
            perfil, error = self._obtener_perfil_cliente(codigo_cliente)
            
            if error:
                logger.error(f"Error DB: {error}")
                return None, {"error": "Error de conexión."}
            
            if not perfil:
                return None, {"error": f"El código '{codigo_cliente}' no existe."}

            # Métricas
            dias_inactividad = perfil.get('days_since_last_order')
            ticket_promedio = float(perfil.get('avg_order_value') or 0.0)
            lifetime_orders = int(perfil.get('lifetime_orders') or 0)

            # Lógica de Negocio
            if dias_inactividad is None:
                estrategia = "PROSPECCION"
                producto = "Mix de Muestras"
                precio_sugerido = 0.0
                observacion = "Cliente nuevo sin historial."
            elif dias_inactividad > 45:
                estrategia = "REACTIVACION"
                producto = "Freedom Red (Oferta Retorno)"
                precio_sugerido = ticket_promedio * 0.92 
                observacion = f"⚠️ ALERTA: Inactivo hace {dias_inactividad} días."
            else:
                estrategia = "MANTENIMIENTO"
                producto = "Pedido Recurrente"
                precio_sugerido = ticket_promedio
                observacion = "Cliente saludable."

            # Respuesta
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

            prediction_id = self._registrar_auditoria(perfil.get('id'), detalle_sugerencia)
            return prediction_id, detalle_sugerencia

        except Exception as e:
            logger.error(f"Fallo crítico: {e}")
            return None, {"error": str(e)}

    def _registrar_auditoria(self, client_id: str, sugerencia: dict):
        """Guarda en prediction_history. Maneja FKs rotas."""
        payload = {
            "client_id": client_id,
            "input_context": sugerencia["metricas_base"],
            "bot_suggestion": sugerencia,
            "created_at": datetime.utcnow().isoformat()
        }
        try:
            response = self.db.table("prediction_history").insert(payload).execute()
            if response.data: return response.data[0]['id']
        except Exception as e:
            error_msg = str(e)
            if "23503" in error_msg or "foreign key" in error_msg:
                payload["client_id"] = None
                try:
                    response = self.db.table("prediction_history").insert(payload).execute()
                    if response.data: return response.data[0]['id']
                except: pass
            return f"TEMP-{int(datetime.now().timestamp())}"

    def registrar_ajuste_usuario(self, prediction_id: str, precio_real: float) -> bool:
        """Registra corrección humana."""
        try:
            if str(prediction_id).startswith("TEMP-"): return False
            payload = {
                "user_correction": {
                    "precio_cierre": precio_real,
                    "fecha_ajuste": datetime.utcnow().isoformat()
                }
            }
            self.db.table("prediction_history").update(payload).eq("id", prediction_id).execute()
            return True
        except Exception as e:
            logger.error(f"Error ajuste: {e}")
            return False

    # --- VERSIÓN DEFINITIVA: ESTRUCTURA RELACIONAL (HEADER + ITEMS) ---
    def crear_orden_confirmada(self, datos_orden: dict) -> str:
        """
        Crea una orden estructurada en 'sales_orders' y 'sales_items'.
        """
        try:
            # 1. Generamos PO Number Único
            po_number = f"P{int(datetime.now().timestamp())}"
            
            # 2. Preparar Cabecera (La Logística)
            cabecera = {
                "po_number": po_number,
                "vendor": datos_orden.get("vendor", "BM"),
                "ship_date": datetime.now().strftime("%Y-%m-%d"),
                "origin": "BOG",
                "status": "Confirmed",
                "source_file": "Bot_Telegram_V1",
                "total_boxes": int(datos_orden["cajas"]),
                "total_value": float(datos_orden["valor_total_pedido"])
            }

            # 3. Insertar Cabecera
            # Usamos insert().select() porque en tablas nuevas con RLS default suele funcionar bien
            # Si falla, prueba quitar .select() como hicimos antes
            res_head = self.db.table("sales_orders").insert(cabecera).execute()
            
            if not res_head.data:
                logger.error("Fallo al crear cabecera sales_orders")
                return None
            
            order_uuid = res_head.data[0]['id']

            # 4. Preparar Ítem (El Producto Específico)
            item = {
                "order_id": order_uuid,
                "customer_code": datos_orden["cliente_nombre"], # El cliente va en el item
                "product_name": datos_orden["producto_descripcion"],
                "box_type": datos_orden["tipo_caja"],
                "boxes": int(datos_orden["cajas"]),
                "total_units": int(datos_orden["total_tallos"]),
                "unit_price": float(datos_orden["precio_unitario"]),
                "total_line_value": float(datos_orden["valor_total_pedido"]),
                "notes": "Generado vía Bot"
            }

            # 5. Insertar Ítem
            self.db.table("sales_items").insert(item).execute()
            
            logger.info(f"✅ Orden Relacional Creada: {po_number}")
            return po_number

        except Exception as e:
            # Imprimir error real en consola para debug inmediato
            print(f"🔴 ERROR DB: {e}")
            logger.error(f"Error fatal creando Orden Relacional: {e}")
            return None
