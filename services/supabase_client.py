import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Cargamos las variables de entorno, esas verdades ocultas en el .env
load_dotenv()

# Invocamos las coordenadas del destino
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

# Validación existencial: Sin llaves, no hay puerta
if not url or not key:
    raise ValueError("❌ Error Crítico: No se encontraron SUPABASE_URL o SUPABASE_KEY en el archivo .env")

# La materialización del cliente. 
# Esta variable 'supabase' es la que panel_control.py está buscando desesperadamente.
supabase: Client = create_client(url, key)

# Un pequeño susurro al log para confirmar la vida (opcional)
print("💧 Inteligencia Líquida conectada a Supabase.")
