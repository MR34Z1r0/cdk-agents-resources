import json
import os
import boto3
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.logger import custom_logger
from aje_libs.common.utils import DecimalEncoder
import traceback
from boto3.dynamodb.conditions import Attr

# Configurar variables de entorno
DYNAMO_CHAT_HISTORY_TABLE = os.environ.get("DYNAMO_CHAT_HISTORY_TABLE")
HISTORY_CANT_ELEMENTS = int(os.environ.get("HISTORY_CANT_ELEMENTS", 5))

OWNER = os.environ.get("OWNER")
PROJECT_NAME = os.environ.get("PROJECT_NAME")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)
  
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
        
        # Validar campos requeridos usando formato estandarizado
        required_fields = ["userId", "syllabusEventId"]
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            logger.error(f"Campos requeridos faltantes: {missing_fields}")
            return {
                "success": False,
                "message": f"Campos requeridos faltantes: {missing_fields}",
                "statusCode": 400,
                "error": {
                    "code": "MISSING_FIELDS",
                    "details": f"Campos requeridos faltantes: {missing_fields}"
                }
            }
        
        user_id = body["userId"]
        syllabus_event_id = body["syllabusEventId"]
        
        logger.info(f"Obteniendo historial para usuario: {user_id}, syllabus: {syllabus_event_id}")
        
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
        
        # Ordenar por fecha descendente
        history_items.sort(key=lambda x: x.get("DATE_TIME", ""), reverse=True)
        
        # Limitar a la cantidad deseada
        history_items = history_items[:HISTORY_CANT_ELEMENTS]
        
        # Transformar a formato estandarizado
        formatted_history = []
        for item in history_items:
            formatted_history.append({
                "userId": item.get("ALUMNO_ID", ""),
                "dateTime": item.get("DATE_TIME", ""),
                "syllabusEventId": item.get("SILABUS_ID", ""),
                "userMessage": item.get("USER_MESSAGE", ""),
                "aiMessage": item.get("AI_MESSAGE", "")
            })
        
        logger.info(f"Historial obtenido exitosamente: {len(formatted_history)} mensajes")
        
        return {
            "success": True,
            "message": "Historial obtenido exitosamente",
            "statusCode": 200,
            "data": {
                "history": formatted_history
            }
        }
        
    except Exception as e:
        logger.error(f"Error en get_history: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            "success": False,
            "message": "Error al obtener historial",
            "statusCode": 500,
            "error": {
                "code": "INTERNAL_ERROR",
                "details": str(e)
            }
        }