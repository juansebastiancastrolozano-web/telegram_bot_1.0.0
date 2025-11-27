from services.cliente_supabase import db_client, logger
from datetime import datetime

class OrquestadorPedidos:
    """
    El Director de Orquesta.
    Coordina: Ingesta -> Edición -> Facturación -> Despacho.
    """

    def obtener_resumen_pendientes(self):
        """
        Analiza 'sales_orders' que están en estado 'Confirmed' pero SIN Factura.
        Equivalente a mirar la hoja ORDENAA y ver qué falta procesar.
        """
        try:
            # Buscamos órdenes confirmadas que no tienen ID de factura (is null)
            # Nota: Supabase filter 'is' se usa como .is_('column', 'null')
            res = db_client.table("sales_orders")\
                .select("*, sales_items(*)")\
                .eq("status", "Confirmed")\
                .is_("invoice_id", "null")\
                .execute()
            
            ordenes = res.data or []
            
            if not ordenes:
                return "✅ Todo está al día. No hay órdenes pendientes de facturar.", []

            # Agrupamos por Cliente para mostrar bonito
            resumen_texto = "📊 **Tablero de Control (Pendientes)**\n\n"
            grupos_cliente = {}
            
            for orden in ordenes:
                cliente = orden.get('customer_name', 'Varios')
                if cliente not in grupos_cliente: grupos_cliente[cliente] = []
                grupos_cliente[cliente].append(orden)

            botones_data = []

            for cliente, lista_ordenes in grupos_cliente.items():
                total_cajas = sum(o['total_boxes'] for o in lista_ordenes)
                total_plata = sum(o['total_value'] for o in lista_ordenes)
                cant_pos = len(lista_ordenes)
                
                resumen_texto += (
                    f"👤 <b>{cliente}</b>\n"
                    f"   📦 {cant_pos} POs pendientes ({total_cajas} cajas)\n"
                    f"   💰 Valor Aprox: ${total_plata:,.2f}\n"
                    f"   <i>/facturar_{cliente.replace(' ', '_')}</i>\n\n"
                )
                
                # Guardamos datos para generar botones después
                botones_data.append({"cliente": cliente, "cantidad": cant_pos})

            return resumen_texto, botones_data

        except Exception as e:
            logger.error(f"Error orquestador: {e}")
            return f"💥 Error consultando pendientes: {e}", []

orquestador = OrquestadorPedidos()
