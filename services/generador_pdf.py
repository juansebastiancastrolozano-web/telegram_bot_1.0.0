import os
import logging
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from services.cliente_supabase import db_client

logger = logging.getLogger(__name__)

class GeneradorDocumentos:
    def __init__(self):
        self.width, self.height = LETTER

    def _dibujar_encabezado_base(self, c, titulo, orden):
        """Dibuja el logo y datos comunes"""
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, 750, "J&G Specialty Crops LLC")
        c.setFont("Helvetica", 9)
        c.drawString(50, 735, "1712 Pioneer Ave, Suite # 1017 - Cheyenne, WY 82001")
        c.drawString(50, 720, "USA - Tel: 573 12 376 9076")

        # Caja Titulo
        c.setLineWidth(1)
        c.rect(400, 710, 180, 50)
        c.setFont("Helvetica-Bold", 12)
        c.drawCentredString(490, 745, titulo)
        c.setFont("Helvetica", 10)
        c.drawString(410, 725, f"No. {orden['po_number']}")
        c.drawString(410, 713, f"Fecha: {orden['ship_date']}")

    def generar_factura_cliente(self, po_number: str, output_path: str):
        """Genera la FACTURA COMERCIAL (Con Precios de Venta)"""
        try:
            datos = self._obtener_datos(po_number)
            if not datos: return False, "PO no encontrada"
            orden, items, cliente = datos

            c = canvas.Canvas(output_path, pagesize=LETTER)
            self._dibujar_encabezado_base(c, "COMMERCIAL INVOICE", orden)

            # Info Cliente
            c.rect(50, 630, 300, 60)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(55, 675, "SOLD TO / VENDIDO A:")
            c.setFont("Helvetica", 9)
            c.drawString(55, 660, str(cliente.get('name', '')))
            c.drawString(55, 645, str(cliente.get('address', '')))
            c.drawString(55, 630, f"{cliente.get('city','')}, {cliente.get('country','')}")

            # Tabla (CON PRECIOS)
            y = 580
            headers = ["Box", "Qty", "Description", "Mark Code", "Stems", "Unit Price", "Total"]
            x_pos = [50, 90, 130, 280, 380, 450, 520]
            
            self._dibujar_tabla(c, headers, x_pos, y, items, mostrar_precio=True)
            
            # Totales y Pie
            c.setFont("Helvetica-Bold", 10)
            c.drawString(450, 150, f"TOTAL USD: ${orden.get('total_value', 0):.2f}")
            
            c.setFont("Helvetica", 8)
            c.drawCentredString(300, 30, "PLEASE WIRE PAYMENT TO: HELM BANK USA")
            
            c.save()
            return True, "OK"
        except Exception as e:
            return False, str(e)

    def generar_po_finca(self, po_number: str, output_path: str):
        """Genera la ORDEN DE COMPRA (Para la Finca - Enfocada en Logística)"""
        try:
            datos = self._obtener_datos(po_number)
            if not datos: return False, "PO no encontrada"
            orden, items, cliente = datos

            c = canvas.Canvas(output_path, pagesize=LETTER)
            self._dibujar_encabezado_base(c, "PURCHASE ORDER (FINCA)", orden)

            # Info Proveedor (Vendor)
            c.rect(50, 630, 300, 60)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(55, 675, "VENDOR / CULTIVO:")
            c.setFont("Helvetica", 12)
            c.drawString(55, 655, str(orden.get('vendor', 'BM'))) # El código de la finca
            c.setFont("Helvetica", 9)
            c.drawString(55, 640, "Origen: BOG - Colombia")

            # Instrucciones Especiales
            c.rect(360, 630, 220, 60)
            c.drawString(365, 675, "INSTRUCCIONES:")
            c.setFont("Helvetica", 8)
            c.drawString(365, 660, "• Marcar cajas en ambos lados.")
            c.drawString(365, 645, "• Usar capuchón y comida según especif.")
            c.drawString(365, 630, "• Confirmar recepción.")

            # Tabla (SIN PRECIOS DE VENTA - Solo Logística)
            y = 580
            # Nota: Quitamos Precio y Total. Agregamos más espacio a Descripción y Marca.
            headers = ["Caja", "Cant", "Producto / Variedad", "MARCACIÓN (Mark Code)", "Total Tallos"]
            x_pos = [50, 90, 130, 350, 500]
            
            self._dibujar_tabla(c, headers, x_pos, y, items, mostrar_precio=False)
            
            # Pie de Finca
            c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(300, 50, "*** FAVOR CONFIRMAR DESPACHO ANTES DE LAS 10 AM ***")
            
            c.save()
            return True, "OK"
        except Exception as e:
            return False, str(e)

    def _obtener_datos(self, po_number):
        res_head = db_client.table("sales_orders").select("*").eq("po_number", po_number).execute()
        if not res_head.data: return None
        orden = res_head.data[0]

        res_items = db_client.table("sales_items").select("*").eq("order_id", orden['id']).execute()
        items = res_items.data

        # Cliente (Para dirección)
        cliente_info = {}
        if items:
            cust_code = items[0].get('customer_code')
            res_cust = db_client.table("customers").select("*").or_(f"code.eq.{cust_code},customer_code.eq.{cust_code}").execute()
            cliente_info = res_cust.data[0] if res_cust.data else {"name": cust_code}
        
        return orden, items, cliente_info

    def _dibujar_tabla(self, c, headers, x_pos, y_start, items, mostrar_precio=True):
        # Header Gráfico
        c.setFillColor(colors.lightgrey)
        c.rect(40, y_start, 540, 15, fill=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 8)
        
        for i, h in enumerate(headers):
            c.drawString(x_pos[i], y_start+4, h)
        
        y = y_start - 20
        c.setFont("Helvetica", 9)

        for item in items:
            c.drawString(x_pos[0], y, str(item.get('box_type', '')))
            c.drawString(x_pos[1], y, str(item.get('boxes', 0)))
            c.drawString(x_pos[2], y, str(item.get('product_name', ''))[:35])
            
            # Lógica especial para Finca vs Cliente
            if mostrar_precio:
                c.drawString(x_pos[3], y, str(item.get('mark_code', ''))[:15])
                c.drawString(x_pos[4], y, str(item.get('total_units', 0)))
                c.drawString(x_pos[5], y, f"${item.get('unit_price', 0):.2f}")
                c.drawString(x_pos[6], y, f"${item.get('total_line_value', 0):.2f}")
            else:
                # En PO Finca, damos más espacio a la Marcación que es vital
                c.setFont("Helvetica-Bold", 9) # Negrita para que el operario lo vea bien
                c.drawString(x_pos[3], y, str(item.get('mark_code', 'NO MARK')))
                c.setFont("Helvetica", 9)
                c.drawString(x_pos[4], y, str(item.get('total_units', 0)))

            y -= 15
            if y < 100: # Salto de página simple
                c.showPage()
                y = 700

generador_documentos = GeneradorDocumentos()
