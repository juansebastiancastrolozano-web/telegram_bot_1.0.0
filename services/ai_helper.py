import os
import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

# Iniciamos el cliente. Si no hay Key, no explota, solo avisa.
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key) if api_key else None

def analizar_texto_con_ia(texto_sucio: str, contexto: str = "producto"):
    """
    Usa GPT-4o-mini para limpiar y estructurar datos caóticos.
    
    Args:
        texto_sucio: Lo que dice el Excel (ej: "MONDIAL 50CM WHT")
        contexto: Qué tipo de dato es ("producto", "direccion", "nota")
    
    Returns:
        Un diccionario JSON limpio o None si falla.
    """
    if not client:
        logger.warning("⚠️ OpenAI API Key no configurada. Saltando IA.")
        return None
    
    if not texto_sucio or len(str(texto_sucio)) < 3:
        return None

    # Definimos la personalidad del modelo según el contexto
    if contexto == "producto":
        system_prompt = (
            "Eres un experto botánico y logístico de flores. "
            "Tu misión es recibir descripciones sucias de productos y devolver un JSON estricto con: "
            "flower_type (Rose, Carnation, etc), variety (Freedom, Mondial, etc), "
            "color (Red, White, etc), grade (40cm, 50cm, Select, etc). "
            "Si no sabes algo, pon null. No inventes."
        )
    elif contexto == "direccion":
        system_prompt = (
            "Eres un experto en geografía y logística. "
            "Normaliza esta dirección en JSON: address, city, state, zip_code, country."
        )
    else:
        system_prompt = "Eres un asistente útil que extrae datos estructurados en JSON."

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini", # El modelo económico y rápido
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Analiza esto: {texto_sucio}"}
            ],
            response_format={"type": "json_object"}, # ¡Magia! Obliga a responder JSON
            temperature=0 # Creatividad 0, queremos precisión robótica
        )
        
        contenido = response.choices[0].message.content
        return json.loads(contenido)

    except Exception as e:
        logger.error(f"Error cerebral (OpenAI): {e}")
        return None

# Prueba rápida si ejecutas este archivo directo
if __name__ == "__main__":
    prueba = "rosas rojas freedom de 50 cm"
    print(f"Probando con: {prueba}")
    print(analizar_texto_con_ia(prueba, "producto"))
