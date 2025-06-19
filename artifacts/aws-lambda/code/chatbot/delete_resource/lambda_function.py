import json
import os
from typing import Dict, Any

# Importar helpers de aje-libs
from aje_libs.common.helpers.s3_helper import S3Helper
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.bd.helpers.pinecone_helper import PineconeHelper
from aje_libs.common.logger import custom_logger
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.common.helpers.ssm_helper import SSMParameterHelper
# Configuración
ENVIRONMENT = os.environ["ENVIRONMENT"]
PROJECT_NAME = os.environ["PROJECT_NAME"]
OWNER = os.environ["OWNER"]
DYNAMO_RESOURCES_TABLE = os.environ["DYNAMO_RESOURCES_TABLE"]
DYNAMO_LIBRARY_TABLE = os.environ["DYNAMO_LIBRARY_TABLE"]
DYNAMO_RESOURCES_HASH_TABLE = os.environ["DYNAMO_RESOURCES_HASH_TABLE"]
S3_RESOURCES_BUCKET = os.environ["S3_RESOURCES_BUCKET"]

# Parameter Store
ssm_chatbot = SSMParameterHelper(f"/{ENVIRONMENT}/{PROJECT_NAME}/chatbot")
PARAMETER_VALUE = json.loads(ssm_chatbot.get_parameter_value())

EMBEDDINGS_MODEL_ID = PARAMETER_VALUE["EMBEDDINGS_MODEL_ID"]
EMBEDDINGS_REGION = PARAMETER_VALUE["EMBEDDINGS_REGION"]
# Secrets
secret_pinecone = SecretsHelper(f"{ENVIRONMENT}/{PROJECT_NAME}/pinecone-api-key")

PINECONE_INDEX_NAME = secret_pinecone.get_secret_value("PINECONE_INDEX_NAME")
PINECONE_API_KEY = secret_pinecone.get_secret_value("PINECONE_API_KEY")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

# Crear helper instances
s3_helper = S3Helper(bucket_name=S3_RESOURCES_BUCKET)
files_table_helper = DynamoDBHelper(
    table_name=DYNAMO_RESOURCES_TABLE,
    pk_name="resource_id"
)
hash_table_helper = DynamoDBHelper(
    table_name=DYNAMO_RESOURCES_HASH_TABLE,
    pk_name="file_hash"
)
pinecone_helper = PineconeHelper(
    index_name=PINECONE_INDEX_NAME,
    api_key=PINECONE_API_KEY,
    embeddings_model_id=EMBEDDINGS_MODEL_ID,
    embeddings_region=EMBEDDINGS_REGION
)
dynamo_library = DynamoDBHelper(
    table_name=DYNAMO_LIBRARY_TABLE,
    pk_name="silabus_id"
)
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal de Lambda para eliminar un recurso educativo.
    
    :param event: Evento de Lambda (debe contener body con resourceId)
    :param context: Contexto de Lambda
    :return: Respuesta estandarizada
    """
    try:
        # Parsear el body del evento
        if 'body' in event:
            if isinstance(event['body'], dict):
                body = event['body']
            else:
                body = json.loads(event['body'])
        else:
            body = event
        
        # Validar que los campos necesarios estén presentes usando el formato estandarizado
        
        required_fields = ["RecursoDidacticoId", "SilaboEventoId"]
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
        
        resource_id = body["RecursoDidacticoId"]
        silabus_id = body['SilaboEventoId']
        # Procesar la eliminación del recurso
        result = process_resource_deletion(resource_id, silabus_id)
        
        if result["success"]:
            return {            
                "statusCode": 200,
                "body": json.dumps({
                    "success": True,
                    "data": {
                    "resourceId": resource_id,
                    "details": result.get("details", {})
                    }
                })
            }
        else:
            status_code = 404 if "no existe" in result.get("message", "").lower() else 500
            return {
            "statusCode": status_code,
            "body": json.dumps({
                "success": False,
                "message": result["message"]
                })
            }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "message": str(e)
            })
        }

def process_resource_deletion(resource_id: str, silabus_id: str) -> Dict[str, Any]:
    """
    Elimina un recurso y sus vectores asociados de DynamoDB, S3 y Pinecone.
    
    :param resource_id: ID del recurso a eliminar
    :return: Resultado de la operación
    """
    try:
        logger.info(f"Iniciando eliminación del recurso: {resource_id}")
        
        # Obtener el elemento de la tabla de archivos
        item = files_table_helper.get_item(resource_id)
        
        if not item:
            message = f"El recurso con resource_id '{resource_id}' no existe."
            logger.info(message)
            return {"success": False, "message": message}
        
        file_hash = item.get('file_hash')
        s3_path = item.get('s3_path')
        pinecone_ids = item.get('pinecone_ids', [])
        
        # 1. Eliminar vectores de Pinecone si hay IDs
        if pinecone_ids:
            logger.info(f"Eliminando {len(pinecone_ids)} vectores de Pinecone")
            try:
                pinecone_helper.delete_vectors(pinecone_ids)
                logger.info(f"Vectores eliminados exitosamente de Pinecone")
            except Exception as e:
                logger.error(f"Error eliminando vectores de Pinecone: {str(e)}", exc_info=True)
                # Continuamos con el proceso aunque falle Pinecone
        
        # 2. Eliminar objeto de S3 si existe la ruta
        if s3_path:
            try:
                # Extraer la clave del objeto de la ruta S3
                object_key = s3_path.replace(f"s3://{S3_RESOURCES_BUCKET}/", "")
                if object_key:
                    s3_helper.delete_object(object_key)
                    logger.info(f"Objeto S3 eliminado exitosamente: {object_key}")
            except Exception as e:
                logger.error(f"Error eliminando objeto de S3: {str(e)}", exc_info=True)
                # Continuamos con el proceso aunque falle S3
        
        # 3. Eliminar registros de DynamoDB
        deleted_tables = []
        
        try:
            if file_hash:
                hash_table_helper.delete_item(file_hash)
                deleted_tables.append(DYNAMO_RESOURCES_HASH_TABLE)
                logger.info(f"Registro eliminado de la tabla hash: {file_hash}")
            
            library_item = dynamo_library.get_item(silabus_id)
            if library_item and "resources" in library_item and resource_id in library_item["resources"]:
                # Eliminar un resource_id de library_item["resources"]
                update_expression = "SET resources = :resources"
                new_resources = [r for r in library_item["resources"] if r != resource_id]
                expression_attribute_values = {
                    ":resources": new_resources
                }
                
                dynamo_library.update_item(
                    partition_key=silabus_id, 
                    update_expression=update_expression,
                    expression_attribute_values=expression_attribute_values
                )
                
                logger.info(f"Recurso {resource_id} eliminado del listado de recursos de silabus {silabus_id}")
            else:
                logger.info(f"No se encontró el recurso {resource_id} en la lista de recursos de silabus {silabus_id}")
                
            files_table_helper.delete_item(resource_id)
            deleted_tables.append(DYNAMO_RESOURCES_TABLE)
            logger.info(f"Registro eliminado de la tabla de recursos: {resource_id}")
            
            return {
                "success": True,
                "message": f"Recurso {resource_id} eliminado exitosamente",
                "details": {
                    "deleted_from_tables": deleted_tables,
                    "deleted_from_pinecone": len(pinecone_ids) > 0,
                    "deleted_from_s3": s3_path is not None
                }
            }
        except Exception as e:
            logger.error(f"Error eliminando registros de DynamoDB: {str(e)}", exc_info=True)
            raise
        
    except Exception as e:
        logger.error(f"Error en process_resource_deletion: {str(e)}", exc_info=True)
        return {
            "success": False,
            "message": f"Error al eliminar el recurso: {str(e)}"
        }