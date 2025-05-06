import json
import os
import boto3
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.logger import custom_logger
from aje_libs.common.utils import DecimalEncoder
import traceback
#from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
#load_dotenv()

# Configurar variables de entorno
DYNAMO_CHAT_HISTORY_TABLE = os.environ.get("DYNAMO_CHAT_HISTORY_TABLE")
HISTORY_CANT_ELEMENTS = int(os.environ.get("HISTORY_CANT_ELEMENTS", 5))

#AWS_PROFILE = os.environ.get("AWS_PROFILE")
#AWS_REGION = os.environ.get("AWS_REGION")

OWNER = os.environ.get("OWNER")
PROJECT_NAME = os.environ.get("PROJECT_NAME")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

logger.info("Iniciando logging")
#boto3.setup_default_session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
  
# Inicializar DynamoDBHelper
dynamo_chat_history = DynamoDBHelper(
    table_name=DYNAMO_CHAT_HISTORY_TABLE,
    pk_name="ALUMNO_ID",
    sk_name="DATE_TIME"
)

def lambda_handler(event, context):
    """Función Lambda para obtener historial de conversación."""
    try:
        logger.info("Iniciando función get_history")
        
        # Parsear el body del evento
        if 'body' in event:
            if isinstance(event['body'], dict):
                body = event['body']
            else:
                body = json.loads(event['body'])
        else:
            body = event
        
        # Validar campos requeridos
        required_fields = ["user_id", "syllabus_event_id"]
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            logger.error(f"Campos requeridos faltantes: {missing_fields}")
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "success": False,
                    "message": f"Campos requeridos faltantes: {missing_fields}"
                })
            }
        
        user_id = body["user_id"]
        syllabus_event_id = body["syllabus_event_id"]
        
        logger.info(f"Obteniendo historial para usuario: {user_id}, syllabus: {syllabus_event_id}")
        
        # En lugar de usar filter_expression como string, usamos el nuevo enfoque
        from boto3.dynamodb.conditions import Attr
        
        # Construir la expresión de filtro correctamente
        filter_expression = Attr('SILABUS_ID').eq(syllabus_event_id) & Attr('IS_DELETED').eq(False)
        
        # Usa query si estás buscando por ALUMNO_ID específico:
        history_items = dynamo_chat_history.query_table(
            key_condition=f"{dynamo_chat_history.pk_name} = :user_id",
            filter_expression=f"SILABUS_ID = :syllabus_id AND IS_DELETED = :is_deleted",
            expression_attribute_values={
                ":user_id": user_id,
                ":syllabus_id": syllabus_event_id,
                ":is_deleted": False
            },
            limit=HISTORY_CANT_ELEMENTS * 5,
            scan_forward=False  # Para obtener los más recientes primero
        )
        
        # Filtrar por ALUMNO_ID en memoria
        filtered_history = [
            item for item in history_items 
            if item.get("ALUMNO_ID") == user_id
        ]
        
        # Ordenar por fecha descendente
        filtered_history.sort(key=lambda x: x.get("DATE_TIME", ""), reverse=True)
        
        # Limitar a la cantidad deseada
        filtered_history = filtered_history[:HISTORY_CANT_ELEMENTS]
        
        logger.info(f"Historial obtenido exitosamente: {len(filtered_history)} mensajes")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "success": True,
                "message": "Historial obtenido exitosamente",
                "data": {"history": filtered_history}
            }, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Error en get_history: {str(e)}")
        logger.debug(traceback.format_exc())
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "message": "Error al obtener historial",
                "error": str(e)
            })
        }