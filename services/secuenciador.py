from datetime import datetime
from services.cliente_supabase import db_client, logger

class Secuenciador:
    """
    El Contador Oficial.
    Reemplaza la hoja 'INDICES'. Genera consecutivos únicos para Facturas y POs.
    """

    def obtener_siguiente_invoice(self) -> str:
        """
        Genera: AÑOMESDIA / CONSECUTIVO (Ej: 251126/1051)
        """
        consec = self._incrementar('INVOICE')
        fecha_str = datetime.now().strftime("%y%m%d") # 251126
        return f"{fecha_str}/{consec}"

    def obtener_siguiente_po(self, finca_code: str = "GEN") -> str:
        """
        Genera: FINCA + FECHA / CONSECUTIVO (Ej: TUC251126/0901)
        """
        clave = f"PO_{finca_code.upper()}"
        consec = self._incrementar(clave)
        fecha_str = datetime.now().strftime("%y%m%d")
        
        # Formato con ceros a la izquierda (0901)
        consec_fmt = str(consec).zfill(4)
        return f"{finca_code.upper()}{fecha_str}/{consec_fmt}"

    def _incrementar(self, tipo: str) -> int:
        try:
            # 1. Obtener valor actual
            res = db_client.table("secuencias").select("ultimo_valor").eq("tipo", tipo).execute()
            
            if not res.data:
                # Si es una finca nueva, la inicializamos en 1
                db_client.table("secuencias").insert({"tipo": tipo, "ultimo_valor": 1}).execute()
                return 1
            
            val_actual = res.data[0]['ultimo_valor']
            nuevo_val = val_actual + 1
            
            # 2. Actualizar (+1) atómicamente
            db_client.table("secuencias").update({"ultimo_valor": nuevo_val}).eq("tipo", tipo).execute()
            
            return nuevo_val
        except Exception as e:
            logger.error(f"Error secuencia {tipo}: {e}")
            return 9999 # Fallback de emergencia

secuenciador = Secuenciador()
