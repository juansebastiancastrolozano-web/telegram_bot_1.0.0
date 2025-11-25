# services/calculadora.py
from decimal import Decimal, ROUND_HALF_UP
import logging

logger = logging.getLogger(__name__)

class CalculadoraFloral:
    """
    El ábaco digital. 
    Convierte la ambigüedad de las 'Cajas' en la certeza de los 'Dólares'.
    Reemplaza las fórmulas de las columnas V, W, X de tu Excel.
    """

    # Constantes de Factores de Empaque (Heurística basada en tu Excel)
    # Si estos cambian por finca, deberían venir de la base de datos.
    # Por ahora, estandarizamos la realidad.
    FACTORES_CAJA = {
        "EB": 0.125, # Eighth Box (Octavo)
        "QB": 0.25,  # Quarter Box (Cuarto)
        "HB": 0.50,  # Half Box (Media)
        "FB": 1.0    # Full Box (Tabaco/Grande)
    }

    def calcular_linea_pedido(self, cantidad_cajas: int, tipo_caja: str, tallos_por_ramo: int, ramos_por_caja_full: int, precio_unitario: float):
        """
        Realiza la transmutación matemática de la orden.
        
        Args:
            cantidad_cajas: Número de cajas físicas.
            tipo_caja: 'HB', 'QB', 'EB' (La UOM).
            tallos_por_ramo: Generalmente 20 o 25 (Carnation vs Rose).
            ramos_por_caja_full: Cuántos ramos caben en una caja FULL teórica (Tabaco).
            precio_unitario: Precio por tallo (o por ramo, depende de tu negocio, asumo tallo).
        
        Returns:
            Diccionario con la verdad matemática desglosada.
        """
        try:
            # 1. Normalización de Decimales (Para evitar errores de coma flotante $0.0000001)
            precio = Decimal(str(precio_unitario))
            
            # 2. Determinar el factor de volumen de la caja
            factor = self.FACTORES_CAJA.get(tipo_caja.upper(), 0.25) # Default a QB si no se sabe
            
            # 3. Calcular Ramos Reales por esa caja específica
            # En tu Excel: Qty/Box (Column K en ORDENAA)
            # Si una Full hace 80 ramos, una QB hace 20.
            ramos_por_caja_fisica = int(ramos_por_caja_full * factor)
            
            # 4. Calcular Total de Tallos (La Masa)
            # Fórmula Excel: Quantity * Qty/Box * Stems/Bunch
            total_ramos = cantidad_cajas * ramos_por_caja_fisica
            total_tallos = total_ramos * tallos_por_ramo
            
            # 5. Calcular Dinero (La Energía)
            # Fórmula Excel: Total Tallos * Precio Unitario
            valor_total = Decimal(total_tallos) * precio
            
            return {
                "total_tallos": total_tallos,
                "total_ramos": total_ramos,
                "ramos_por_caja": ramos_por_caja_fisica,
                "valor_total": float(valor_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
                "meta_data": f"{cantidad_cajas} x {tipo_caja} ({ramos_por_caja_fisica} bch/box)"
            }

        except Exception as e:
            logger.error(f"Error matemático: {e}")
            return None

# Instancia singleton para usar en todo el bot
calculadora = CalculadoraFloral()
