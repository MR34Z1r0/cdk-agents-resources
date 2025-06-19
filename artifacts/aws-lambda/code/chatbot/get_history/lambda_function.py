import json
import os
import boto3
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.logger import custom_logger
from aje_libs.common.utils import DecimalEncoder
from boto3.dynamodb.conditions import Attr
from aje_libs.common.helpers.ssm_helper import SSMParameterHelper
# Configuración
ENVIRONMENT = os.environ["ENVIRONMENT"]
PROJECT_NAME = os.environ["PROJECT_NAME"]
OWNER = os.environ["OWNER"]
DYNAMO_CHAT_HISTORY_TABLE = os.environ["DYNAMO_CHAT_HISTORY_TABLE"]

# Parameter Store
ssm_chatbot = SSMParameterHelper(f"/{ENVIRONMENT}/{PROJECT_NAME}/chatbot")
PARAMETER_VALUE = json.loads(ssm_chatbot.get_parameter_value())

HISTORY_CANT_ELEMENTS = int(os.environ.get("HISTORY_CANT_ELEMENTS", 5))

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
                "ALUMNO_ID": item.get("ALUMNO_ID", ""),
                "DATE_TIME": item.get("DATE_TIME", ""),
                "SILABUS_ID": item.get("SILABUS_ID", ""),
                "USER_MESSAGE": item.get("USER_MESSAGE", ""),
                "AI_MESSAGE": item.get("AI_MESSAGE", "")
            })
        
        logger.info(f"Historial obtenido exitosamente: {len(formatted_history)} mensajes")
        
        return {            
                "statusCode": 200,
                "body": json.dumps({
                    "success": True,
                    "message": "Historial obtenido exitosamente",
                    "data": {
                    "history": formatted_history
                    }
                })
            }
        
    except Exception as e:
        logger.error(f"Error en get_history: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "message": str(e)
            })
        }