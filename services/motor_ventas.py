import uuid
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from services.cliente_supabase import db_client, logger

class GestorPrediccionVentas:
    """
    Motor de inteligencia comercial y materialización de ventas.
    Conecta el RFM (Financiero) con las Reglas de Empaque (Logístico).
    """

    def __init__(self):
        self.db = db_client

    def _obtener_perfil_cliente(self, codigo_cliente: str):
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

    def _obtener_regla_empaque(self, codigo_cliente: str, nombre_producto: str = ""):
        """
        Consulta la Memoria Logística (customer_packing_rules).
        """
        try:
            # 1. Intento Exacto: Cliente + Producto
            res = self.db.table("customer_packing_rules")\
                .select("*")\
                .eq("customer_code", codigo_cliente)\
                .ilike("product_name", f"%{nombre_producto.split(' ')[0]}%")\
                .limit(1)\
                .execute()
            
            if res.data:
                return res.data[0]

            # 2. Intento Genérico: Última regla usada
            res_gen = self.db.table("customer_packing_rules")\
                .select("*")\
                .eq("customer_code", codigo_cliente)\
                .order("last_updated", desc=True)\
                .limit(1)\
                .execute()
            
            if res_gen.data:
                return res_gen.data[0]
            
            # 3. Fallback
            return {
                "box_type": "QB",
                "bunches_per_box": 10,
                "stems_per_bunch": 25,
                "mark_code": "Standard"
            }
        except Exception:
            return {"box_type": "QB", "bunches_per_box": 10, "stems_per_bunch": 25}

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

            # --- CONEXIÓN CON LA MEMORIA LOGÍSTICA ---
            regla_empaque = self._obtener_regla_empaque(perfil.get('code'), producto)

            # Respuesta Completa
            detalle_sugerencia = {
                "cliente_nombre": perfil.get('name'),
                "codigo_interno": perfil.get('code'),
                "estrategia_aplicada": estrategia,
                "producto_objetivo": producto,
                "precio_unitario": round(precio_sugerido, 2),
                "justificacion_tecnica": observacion,
                # Datos Logísticos Reales
                "logistica": {
                    "tipo_caja": regla_empaque.get("box_type", "QB"),
                    "ramos_x_caja": regla_empaque.get("bunches_per_box", 10),
                    "tallos_x_ramo": regla_empaque.get("stems_per_bunch", 25),
                    "marcacion": regla_empaque.get("mark_code", "Standard"),
                    "upc": regla_empaque.get("upc_code", "")
                },
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

    def crear_orden_confirmada(self, datos_orden: dict) -> str:
        """
        Crea una orden estructurada en 'sales_orders' y 'sales_items'.
        """
        try:
            # 1. PO Number Único
            po_number = f"P{int(datetime.now().timestamp())}"
            
            # 2. Cabecera
            cabecera = {
                "po_number": po_number,
                "vendor": datos_orden.get("vendor", "BM"),
                "ship_date": datetime.now().strftime("%Y-%m-%d"),
                "origin": "BOG",
                "status": "Confirmed",
                "source_file": "Bot_Telegram_V2_Smart",
                "total_boxes": int(datos_orden["cajas"]),
                "total_value": float(datos_orden["valor_total_pedido"])
            }

            res_head = self.db.table("sales_orders").insert(cabecera).execute()
            
            if not res_head.data:
                logger.error("Fallo al crear cabecera sales_orders")
                return None
            
            order_uuid = res_head.data[0]['id']

            # 3. Ítem (Detalle con Logística Real)
            item = {
                "order_id": order_uuid,
                "customer_code": datos_orden["cliente_nombre"], 
                "product_name": datos_orden["producto_descripcion"],
                "box_type": datos_orden["tipo_caja"],
                "boxes": int(datos_orden["cajas"]),
                "total_units": int(datos_orden["total_tallos"]),
                "unit_price": float(datos_orden["precio_unitario"]),
                "total_line_value": float(datos_orden["valor_total_pedido"]),
                "notes": "Generado vía Bot",
                "mark_code": datos_orden.get("marcacion", "") # Aquí guardamos la marca que aprendimos
            }

            self.db.table("sales_items").insert(item).execute()
            
            logger.info(f"✅ Orden Relacional Creada: {po_number}")
            return po_number

        except Exception as e:
            print(f"🔴 ERROR DB: {e}")
            logger.error(f"Error fatal creando Orden Relacional: {e}")
            return None
