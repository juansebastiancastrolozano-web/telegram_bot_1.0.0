# services/cliente_supabase.py
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import logging

# Configuración de Logging para auditoría
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Validación estricta de credenciales
_URL = os.getenv("SUPABASE_URL")
_KEY = os.getenv("SUPABASE_KEY")

if not _URL or not _KEY:
    logger.critical("Error Crítico: Credenciales de Supabase no configuradas.")
    raise EnvironmentError("Faltan SUPABASE_URL o SUPABASE_KEY en variables de entorno.")

try:
    # Instancia Singleton del cliente
    db_client: Client = create_client(_URL, _KEY)
    logger.info("Conexión a Supabase establecida correctamente.")
except Exception as e:
    logger.error(f"Error al conectar con Supabase: {e}")
    raise
