import os
import logging
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class GeneradorFactura:
    def __init__(self):
        self.width, self.height = LETTER

    def generar_pdf_orden(self, po_number: str, output_path: str):
        try:
            # 1. DATOS
            res_head = db_client.table("sales_orders").select("*").eq("po_number", po_number).execute()
            if not res_head.data: return False, "PO no encontrada."
            orden = res_head.data[0]

            res_items = db_client.table("sales_items").select("*").eq("order_id", orden['id']).execute()
            items = res_items.data

            cliente_info = {}
            if items:
                cust_code = items[0].get('customer_code')
                res_cust = db_client.table("customers").select("*").or_(f"code.eq.{cust_code},customer_code.eq.{cust_code}").execute()
                cliente_info = res_cust.data[0] if res_cust.data else {"name": cust_code}

            # 2. LIENZO
            c = canvas.Canvas(output_path, pagesize=LETTER)
            
            # --- ENCABEZADO ---
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, 750, "J&G Specialty Crops LLC")
            
            c.setFont("Helvetica", 10)
            c.drawString(50, 735, "1712 Pioneer Ave, Suite # 1017")
            c.drawString(50, 720, "Cheyenne, WY 82001 - 4409")
            c.drawString(50, 705, "USA")
            c.drawString(50, 690, "Tel: 573 12 376 9076")

            # Caja Factura
            c.rect(400, 700, 180, 60)
            c.setFont("Helvetica-Bold", 12)
            c.drawCentredString(490, 745, "COMMERCIAL INVOICE")
            c.setFont("Helvetica", 10)
            c.drawString(410, 725, f"No. {orden['po_number']}")
            c.drawString(410, 710, f"Date: {orden['ship_date']}")

            # --- CLIENTE ---
            c.rect(50, 600, 250, 70)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(55, 660, "Customer - Consignee:")
            c.setFont("Helvetica", 9)
            c.drawString(55, 645, str(cliente_info.get('name', '')))
            c.drawString(55, 630, str(cliente_info.get('address', '')))
            city = cliente_info.get('city', '')
            country = cliente_info.get('country', '')
            c.drawString(55, 615, f"{city}, {country}")

            # --- TABLA ---
            y = 550
            # Headers
            headers = ["Box Type", "Qty", "Product Description", "Mark Code", "Stems/Box", "Total Stems", "Unit Price", "Total Value"]
            x_positions = [50, 110, 150, 300, 370, 430, 490, 550]
            
            c.setFillColor(colors.lightgrey)
            c.rect(40, y, 540, 15, fill=1)
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 8)
            
            for i, h in enumerate(headers):
                c.drawString(x_positions[i], y+4, h)
            
            y -= 20
            c.setFont("Helvetica", 8)

            for item in items:
                boxes = item.get('boxes', 0) or 1
                stems = item.get('total_units', 0)
                stems_box = stems // boxes if boxes > 0 else 0
                
                c.drawString(x_positions[0], y, str(item.get('box_type', '')))
                c.drawString(x_positions[1], y, str(boxes))
                c.drawString(x_positions[2], y, str(item.get('product_name', ''))[:30]) # Truncar si es largo
                c.drawString(x_positions[3], y, str(item.get('mark_code', ''))[:10])
                c.drawString(x_positions[4], y, str(stems_box))
                c.drawString(x_positions[5], y, str(stems))
                c.drawString(x_positions[6], y, f"${item.get('unit_price', 0):.2f}")
                c.drawString(x_positions[7], y, f"${item.get('total_line_value', 0):.2f}")
                
                y -= 15

            # --- TOTALES ---
            y -= 20
            c.line(450, y, 580, y)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(450, y-15, "TOTAL USD:")
            c.drawString(540, y-15, f"${orden.get('total_value', 0):.2f}")

            # --- PIE ---
            c.setFont("Helvetica", 8)
            c.drawString(50, 100, "INCOTERM: FCA BOGOTA")
            c.drawString(50, 85, "Payment Terms: 30 Days")
            
            legal_text = "Payment shall be performed upon receipt of the flowers. If not paid on time, conduct will be considered delinquent."
            c.drawString(50, 60, legal_text)
            
            c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(300, 30, "PLEASE WIRE PAYMENT TO: HELM BANK USA - Account: 123456789")

            c.save()
            return True, "OK"

        except Exception as e:
            logger.error(f"Error PDF: {e}")
            return False, str(e)

generador_pdf = GeneradorFactura()
