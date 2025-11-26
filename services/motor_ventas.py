import uuid  # <--- IMPORTACIÓN VITAL AÑADIDA
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
            # Buscamos por código o customer_code
            res_id = self.db.table("customers")\
                .select("id, name, code")\
                .or_(f"code.eq.{codigo_cliente},customer_code.eq.{codigo_cliente}")\
                .execute()

            if not res_id.data:
                return None, None

            cliente_maestro = res_id.data[0]
            uuid_cliente = cliente_maestro['id']

            # Consultamos RFM
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

    # --- MÉTODO CORREGIDO: CON GENERACIÓN DE UUID MANUAL ---
    def crear_orden_confirmada(self, datos_orden: dict) -> str:
        """
        Materializa la orden en 'confirm_po'. Genera UUID manual para evitar errores.
        """
        try:
            # 1. Generamos identidad única (PO y UUID)
            po_generado = f"BOT-{int(datetime.now().timestamp())}"
            nuevo_id = str(uuid.uuid4()) # <--- LA CURA DEL ERROR

            # 2. Mapeo robusto
            registro = {
                "id": nuevo_id, # Obligatorio si la DB no tiene auto-gen
                "po_number": po_generado,
                "vendor": datos_orden.get("vendor", "BM"),
                "ship_date": datetime.now().strftime("%Y-%m-%d"),
                "product": datos_orden["producto_descripcion"],
                "boxes": int(datos_orden["cajas"]),
                "confirmed": int(datos_orden["cajas"]),
                "box_type": datos_orden["tipo_caja"],
                "total_units": int(datos_orden["total_tallos"]),
                "cost": float(datos_orden["precio_unitario"]),
                "customer_name": datos_orden["cliente_nombre"],
                "origin": "BOG",
                "status": "Confirmed",
                "notes": "Generado por Bot Telegram",
                "source_file": "Telegram_API",
                "created_at": datetime.utcnow().isoformat()
            }

            print(f"📤 Intentando insertar en confirm_po: {po_generado}") # Debug visual

            # 3. Inserción
            res = self.db.table("confirm_po").insert(registro).execute()
            
            if res.data:
                logger.info(f"✅ PO Creada exitosamente: {po_generado}")
                return po_generado
            
            return None

        except Exception as e:
            # Si falla, esto saldrá en tu terminal para que sepamos QUÉ pasó
            print(f"🔴 ERROR FATAL CREANDO PO: {e}") 
            logger.error(f"Error fatal creando PO: {e}")
            return None
