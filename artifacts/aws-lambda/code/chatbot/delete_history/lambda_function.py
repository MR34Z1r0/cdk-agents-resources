import json
import os
import boto3
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.logger import custom_logger
import traceback
from boto3.dynamodb.conditions import Attr
from aje_libs.common.helpers.ssm_helper import SSMParameterHelper
# Configuración
ENVIRONMENT = os.environ["ENVIRONMENT"]
PROJECT_NAME = os.environ["PROJECT_NAME"]
OWNER = os.environ["OWNER"]
DYNAMO_CHAT_HISTORY_TABLE = os.environ["DYNAMO_CHAT_HISTORY_TABLE"]

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

# Inicializar DynamoDBHelper
dynamo_chat_history = DynamoDBHelper(
    table_name=DYNAMO_CHAT_HISTORY_TABLE,
    pk_name="ALUMNO_ID",
    sk_name="DATE_TIME"
)

def lambda_handler(event, context):
    """Función Lambda para eliminar historial de conversación."""
    try:
        logger.info("Iniciando función delete_history")
        
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
        
        logger.info(f"Eliminando historial para usuario: {user_id}, syllabus: {syllabus_event_id}")
        
        # Construir la expresión de filtro para encontrar los items a eliminar
        filter_expression = Attr('SILABUS_ID').eq(syllabus_event_id) & Attr('IS_DELETED').eq(False)
        
        # Obtener los items que coinciden con el filtro
        items_to_delete = dynamo_chat_history.scan_table(
            filter_expression=filter_expression
        )
        
        logger.info(f"Items encontrados para eliminar: {len(items_to_delete)}")
        
        # Filtrar por ALUMNO_ID en memoria
        filtered_items = [
            item for item in items_to_delete 
            if item.get("ALUMNO_ID") == user_id
        ]
        
        logger.info(f"Se encontraron {len(filtered_items)} items para marcar como eliminados")
        
        # Actualizar cada item marcándolo como eliminado
        deleted_count = 0
        for item in filtered_items:
            try:
                # Extraer valores de los campos 
                alumno_id = item["ALUMNO_ID"]  # Ya debería ser string
                date_time = item["DATE_TIME"]  # Ya debería ser string 
                # Usar update_item con el formato correcto
                dynamo_chat_history.update_item(
                    partition_key=alumno_id,
                    sort_key=date_time,
                    update_expression="SET IS_DELETED = :is_deleted",
                    expression_attribute_values={":is_deleted": True}
                )
                deleted_count += 1
                logger.info(f"Item actualizado exitosamente: {alumno_id}, {date_time}")
            except Exception as e:
                logger.error(f"Error al actualizar item {item}: {str(e)}")
        
        logger.info(f"Se marcaron {deleted_count} items como eliminados exitosamente")
        
        return {
            "success": True,
            "message": f"Historial eliminado exitosamente",
            "statusCode": 200,
            "data": {
                "deletedCount": deleted_count
            }
        }
        
    except Exception as e:
        logger.error(f"Error en delete_history: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            "success": False,
            "message": "Error al eliminar historial",
            "statusCode": 500,
            "error": {
                "code": "INTERNAL_ERROR",
                "details": str(e)
            }
        }