import os
import psycopg2
from psycopg2 import sql
from telegram import Update
from telegram.ext import ContextTypes
from tabulate import tabulate

# Asumiendo que 'tabulate' está en tu requirements.txt
# La realidad es líquida, pero las tablas necesitan estructura rígida.

async def tablageneral(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handler para escupir el contenido de una tabla arbitraria.
    Uso: /tablageneral <nombre_tabla>
    """
    args = context.args
    
    if not args:
        await update.message.reply_text("Error: Falta el nombre de la tabla.")
        return

    nombre_tabla = args[0]
    limite = 10  # No invoquemos al infinito sin necesidad.

    conn = None
    try:
        # Conexión efímera, como la existencia misma.
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cursor = conn.cursor()

        # Usamos sql.Identifier para evitar que el código sea
        # un colador de inyecciones SQL. Seguridad mínima en el caos.
        query = sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(nombre_tabla))
        
        cursor.execute(query, (limite,))
        filas = cursor.fetchall()
        
        if not filas:
            await update.message.reply_text("La tabla está vacía.")
            return

        # Obtenemos los nombres de las columnas, los huesos del esqueleto.
        headers = [desc[0] for desc in cursor.description]

        # Tabulamos para que el ojo humano no sangre al leer.
        tabla_formateada = tabulate(filas, headers, tablefmt="plain")

        # Telegram tiene límites, cortamos el cordón umbilical si es muy largo.
        mensaje = f"```\n{tabla_formateada}\n```"
        
        if len(mensaje) > 4096:
             await update.message.reply_text("Error: El resultado excede el límite de caracteres.")
        else:
             await update.message.reply_text(mensaje, parse_mode='MarkdownV2')

    except psycopg2.Error as e:
        # El error es la única certeza en la programación.
        await update.message.reply_text(f"Error DB: {str(e).splitlines()[0]}")
    
    except Exception as ex:
        await update.message.reply_text("Error interno del servidor.")

    finally:
        if conn:
            cursor.close()
            conn.close()
