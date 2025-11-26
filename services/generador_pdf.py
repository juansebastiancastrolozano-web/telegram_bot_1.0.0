import os
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
from services.cliente_supabase import db_client
import logging

logger = logging.getLogger(__name__)

class GeneradorFactura:
    def __init__(self):
        self.env = Environment(loader=FileSystemLoader('templates'))

    def generar_pdf_orden(self, po_number: str, output_path: str):
        """
        Genera un PDF para una PO específica y lo guarda en la ruta indicada.
        """
        try:
            # 1. Buscar Cabecera
            res_head = db_client.table("sales_orders").select("*").eq("po_number", po_number).execute()
            if not res_head.data:
                return False, "PO no encontrada."
            orden = res_head.data[0]

            # 2. Buscar Ítems
            res_items = db_client.table("sales_items").select("*").eq("order_id", orden['id']).execute()
            items = res_items.data

            # 3. Buscar Cliente (Para la dirección)
            # Tomamos el cliente del primer ítem (asumiendo 1 cliente por factura para simplificar)
            cliente_code = items[0]['customer_code'] if items else "UNKNOWN"
            res_cust = db_client.table("customers").select("*").or_(f"code.eq.{cliente_code},customer_code.eq.{cliente_code}").execute()
            cliente = res_cust.data[0] if res_cust.data else {"name": cliente_code, "address": "N/A", "city": "", "country": ""}

            # 4. Renderizar HTML con Jinja2
            template = self.env.get_template('factura.html')
            html_string = template.render(
                orden=orden,
                items=items,
                cliente=cliente
            )

            # 5. Convertir a PDF
            HTML(string=html_string).write_pdf(output_path)
            
            return True, output_path

        except Exception as e:
            logger.error(f"Error generando PDF: {e}")
            return False, str(e)

generador_pdf = GeneradorFactura()
