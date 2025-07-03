import json
import os
import hashlib
import requests
import unicodedata
import re
import boto3
from pathlib import Path
from typing import Dict, Any, List
from uuid import uuid4
from datetime import datetime

# Importar helpers de aje-libs
from aje_libs.common.helpers.s3_helper import S3Helper
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.bd.helpers.pinecone_helper import PineconeHelper
from aje_libs.documents.helpers.document_processor import DocumentProcessor
from aje_libs.common.logger import custom_logger
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.common.helpers.ssm_helper import SSMParameterHelper

# Configuración
ENVIRONMENT = os.environ["ENVIRONMENT"]
PROJECT_NAME = os.environ["PROJECT_NAME"]
OWNER = os.environ["OWNER"]
DYNAMO_RESOURCES_TABLE = os.environ["DYNAMO_RESOURCES_TABLE"]
DYNAMO_RESOURCES_HASH_TABLE = os.environ["DYNAMO_RESOURCES_HASH_TABLE"]
DYNAMO_LIBRARY_TABLE = os.environ["DYNAMO_LIBRARY_TABLE"]
S3_RESOURCES_BUCKET = os.environ["S3_RESOURCES_BUCKET"]

# Parameter Store
ssm_chatbot = SSMParameterHelper(f"/{ENVIRONMENT}/{PROJECT_NAME}/chatbot")
PARAMETER_VALUE = json.loads(ssm_chatbot.get_parameter_value())

EMBEDDINGS_MODEL_ID = PARAMETER_VALUE["EMBEDDINGS_MODEL_ID"]
EMBEDDINGS_REGION = PARAMETER_VALUE["EMBEDDINGS_REGION"]
# Secrets
secret_pinecone = SecretsHelper(f"{ENVIRONMENT}/{PROJECT_NAME}/pinecone-api-key2")

PINECONE_INDEX_NAME = secret_pinecone.get_secret_value("PINECONE_INDEX_NAME")
PINECONE_API_KEY = secret_pinecone.get_secret_value("PINECONE_API_KEY")

DOWNLOAD_FOLDER = "/tmp/downloads"
S3_PATH = "SOFIA_FILE/PLANIFICACION/AV_Recursos"
 
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
library_table_helper = DynamoDBHelper(
    table_name=DYNAMO_LIBRARY_TABLE,
    pk_name="silabus_id"
)
pinecone_helper = PineconeHelper(
    index_name=PINECONE_INDEX_NAME,
    api_key=PINECONE_API_KEY,
    embeddings_model_id=EMBEDDINGS_MODEL_ID,
    embeddings_region=EMBEDDINGS_REGION
)
document_processor = DocumentProcessor()

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal de Lambda para agregar un recurso educativo.
    
    :param event: Evento de Lambda (debe contener body con resourceId, content.title, content.driveId)
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
        required_fields = ["RecursoDidacticoId", "DriveId", "TituloRecurso", "SilaboEventoId"]
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
        title = body["TituloRecurso"]
        drive_id = body["DriveId"]
        silabus_id = body["SilaboEventoId"]
        
        # Procesar el recurso
        result = process_resource_addition(resource_id, title, drive_id, silabus_id)
        
        if result['success']:
            return {            
                "statusCode": 200,
                "body": json.dumps({
                    "success": True,
                    "data": {
                    "resourceId": resource_id
                    }
                })
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "success": False,
                    "message": result['message']
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

def process_resource_addition(resource_id: str, title: str, drive_id: str, silabus_id: str) -> Dict[str, Any]:
    """
    Procesa la adición de un recurso educativo.
    
    :param resource_id: ID del recurso
    :param title: Título del recurso
    :param drive_id: ID de Google Drive
    :param silabus_id: ID del silabo
    :return: Resultado de la operación
    """
    try:
        # Descargar archivo desde Google Drive
        file_path = download_file_from_gdrive(title, drive_id)
        
        # Generar hash del archivo
        file_hash = generate_file_hash(file_path)
        
        # Verificar si el hash ya existe en DynamoDB
        existing_hash = hash_table_helper.get_item(file_hash)
        if existing_hash:
            logger.info(f"Hash {file_hash} already exists in DynamoDB")
            os.remove(file_path)  # Limpiar archivo temporal
            return {'success': True, 'message': 'Resource already exists'}
        
        # Subir archivo a S3
        object_key = f"{S3_PATH}/{sanitize_filename(title)}"
        s3_path = s3_helper.upload_file(file_path, object_key)
        
        # Registrar en DynamoDB
        resource_data = {
            'resource_id': resource_id,
            'resource_title': title,
            'drive_id': drive_id,
            'file_hash': file_hash,
            's3_path': s3_path,
            'pinecone_ids': []
        }
        
        # Procesar el documento y obtener los IDs de Pinecone
        pinecone_ids = process_document_to_pinecone(file_path, resource_data)
        
        # Actualizar los IDs de Pinecone en el recurso
        resource_data['pinecone_ids'] = pinecone_ids
        
        # Guardar en DynamoDB
        files_table_helper.put_item(resource_data)
        hash_table_helper.put_item({
            'file_hash': file_hash,
            's3_path': s3_path
        })

        try:
            library_item = library_table_helper.get_item(silabus_id)
            if library_item and "resources" in library_item:
                resources = library_item["resources"]
                
                if any(r.get('resource_id') == resource_id for r in resources):
                    return {'success': True, 'message': 'Resource already associated with the selected syllabus'}
                
                resources.append({'resource_id': resource_id})
            else:
                resources = [{'resource_id': resource_id}]

            item = {
                "silabus_id": silabus_id,
                "resources": resources,
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            library_table_helper.put_item(item)
            logger.info(f"Sílabo '{silabus_id}' actualizado o creado con {len(resources)} recursos")
        except Exception as e:
            logger.error(f"Error eliminando registros de DynamoDB: {str(e)}", exc_info=True)
            raise
        
        # Limpiar archivo temporal
        os.remove(file_path)
        
        logger.info(f"Successfully added resource {resource_id}")
        return {'success': True, 'message': 'Resource added successfully'}
        
    except Exception as e:
        logger.error(f"Error processing resource addition: {str(e)}", exc_info=True)
        return {'success': False, 'message': str(e)}

def download_file_from_gdrive(file_name: str, gdrive_id: str) -> str:
    """
    Descarga un archivo desde Google Drive y lo guarda localmente.
    
    :param file_name: Nombre del archivo
    :param gdrive_id: ID de Google Drive
    :return: Ruta del archivo descargado
    """
    url = f"https://drive.google.com/uc?export=download&id={gdrive_id}"
    file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
    
    # Crear directorio si no existe
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    logger.info(f"Downloading {file_name} from Google Drive")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    return file_path

def generate_file_hash(file_path: str) -> str:
    """
    Genera un hash SHA256 para el archivo dado.
    
    :param file_path: Ruta al archivo
    :return: Hash SHA256
    """
    logger.info("Generating file hash")
    sha256_hash = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    
    return sha256_hash.hexdigest()

def sanitize_filename(filename: str) -> str:
    """
    Limpia caracteres especiales y espacios en el nombre del archivo.
    
    :param filename: Nombre original del archivo
    :return: Nombre sanitizado
    """
    normalized_name = unicodedata.normalize('NFKD', filename.lower()).encode('ASCII', 'ignore').decode('ASCII')
    return re.sub(r"[., ]", "_", normalized_name)

def chunk_text(text: str, chunk_size: int = 500, overlap: int = 20) -> List[str]:
    """
    Divide el texto en chunks con solapamiento.
    
    :param text: Texto a dividir
    :param chunk_size: Tamaño de cada chunk
    :param overlap: Solapamiento entre chunks
    :return: Lista de chunks
    """
    chunks = []
    words = text.split()
    
    if not words:
        return []
    
    i = 0
    while i < len(words):
        # Calcular el final del chunk actual
        end = min(i + chunk_size, len(words))
        
        # Crear el chunk
        chunk = " ".join(words[i:end])
        chunks.append(chunk)
        
        # Avanzar, teniendo en cuenta el solapamiento
        i += (chunk_size - overlap)
    
    return chunks

def process_document_to_pinecone(file_path: str, metadata: Dict[str, Any]) -> List[str]:
    """
    Procesa un documento y lo indexa en Pinecone.
    
    :param file_path: Ruta al archivo
    :param metadata: Metadatos del documento
    :return: Lista de IDs de Pinecone
    """
    file_extension = Path(file_path).suffix.lower().replace('.', '')
    
    try:
        # Extraer texto del documento usando DocumentProcessor
        text_content = document_processor.process_document(file_path)
        
        if not text_content:
            logger.warning(f"No text content extracted from {file_path}")
            return []
        
        # Dividir texto en chunks (sin usar langchain)
        chunks = chunk_text(text_content)
        
        # Generar UUIDs para los vectores
        uuids = [str(uuid4()) for _ in range(len(chunks))]
        
        # Convertir chunks a vectores y subir a Pinecone
        vectors_to_upsert = []
        for chunk, doc_id in zip(chunks, uuids):
            # Obtener embeddings
            embedding = pinecone_helper.get_embeddings(chunk)
            # Crear vector con metadata
            vectors_to_upsert.append({
                'id': doc_id,
                'values': embedding,
                'metadata': {
                    **metadata,
                    'text': chunk  # Agregar el texto como parte de metadata
                }
            })
        
        if not vectors_to_upsert:
            logger.warning("No vectors to upsert")
            return []
        
        # Subir vectores a Pinecone
        logger.info(f"Vectors to upsert: {len(vectors_to_upsert)}")
        
        response = pinecone_helper.upsert_vectors(vectors_to_upsert)
        logger.info(f"Upsert successful. Response: {response}")
        
        # Devolver IDs de los vectores
        return uuids
        
    except Exception as e:
        logger.error(f"Error processing document to Pinecone: {str(e)}", exc_info=True)
        return []