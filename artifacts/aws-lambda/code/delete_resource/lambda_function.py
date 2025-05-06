# delete_resource.py
import json
import os
from typing import Dict, Any

# Importar helpers de aje-libs
from aje_libs.common.helpers.s3_helper import S3Helper
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.bd.helpers.pinecone_helper import PineconeHelper
from aje_libs.common.logger import custom_logger
#from dotenv import load_dotenv
import boto3
# Cargar variables de entorno desde archivo .env
#load_dotenv()

# Configuración
FILES_TABLE_NAME = os.environ.get("FILES_TABLE_NAME", "db_learning_resources")
HASH_TABLE_NAME = os.environ.get("HASH_TABLE_NAME", "db_learning_resources_hash")
S3_BUCKET = os.environ.get("S3_BUCKET", "datalake-cls-509399624591-landing-s3-bucket")
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", "")
EMBEDDINGS_MODEL_ID = os.environ.get("EMBEDDINGS_MODEL_ID", "amazon.titan-embed-text-v2:0")
EMBEDDINGS_REGION = os.environ.get("EMBEDDINGS_REGION", "us-west-2")

# Configurar logger
#AWS_PROFILE = os.environ.get("AWS_PROFILE")
#AWS_REGION = os.environ.get("AWS_REGION")

OWNER = os.environ.get("OWNER")
PROJECT_NAME = os.environ.get("PROJECT_NAME")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

logger.info("Iniciando logging")
#boto3.setup_default_session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

# Crear helper instances
s3_helper = S3Helper(bucket_name=S3_BUCKET)
files_table_helper = DynamoDBHelper(
    table_name=FILES_TABLE_NAME,
    pk_name="resource_id"
)
hash_table_helper = DynamoDBHelper(
    table_name=HASH_TABLE_NAME,
    pk_name="file_hash"
)
pinecone_helper = PineconeHelper(
    index_name=PINECONE_INDEX_NAME,
    api_key=PINECONE_API_KEY,
    embeddings_model_id=EMBEDDINGS_MODEL_ID,
    embeddings_region=EMBEDDINGS_REGION
)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal de Lambda para eliminar un recurso educativo.
    
    :param event: Evento de Lambda (debe contener body con RecursoDidacticoId)
    :param context: Contexto de Lambda
    :return: Respuesta en formato API Gateway
    """
    try:
        # Parsear el body del evento
        body = json.loads(event.get('body', '{}'))
        
        # Validar campos requeridos
        required_fields = ['RecursoDidacticoId']
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'message': f"Missing required fields: {', '.join(missing_fields)}"
                })
            }
        
        resource_id = body['RecursoDidacticoId']
        
        # Procesar la eliminación del recurso
        result = process_resource_deletion(resource_id)
        
        return {
            'statusCode': 200 if result['success'] else 404 if 'not found' in result.get('message', '').lower() else 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result)
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': False,
                'message': 'Internal server error',
                'error': str(e)
            })
        }

def process_resource_deletion(resource_id: str) -> Dict[str, Any]:
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
                object_key = s3_path.replace(f"s3://{S3_BUCKET}/", "")
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
                deleted_tables.append(HASH_TABLE_NAME)
                logger.info(f"Registro eliminado de la tabla hash: {file_hash}")
            
            files_table_helper.delete_item(resource_id)
            deleted_tables.append(FILES_TABLE_NAME)
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