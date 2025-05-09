import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.bd.helpers.pinecone_helper import PineconeHelper
from aje_libs.common.logger import custom_logger
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.common.helpers.ssm_helper import SSMParameterHelper

# Configuración
ENVIRONMENT = os.environ["ENVIRONMENT"]
PROJECT_NAME = os.environ["PROJECT_NAME"]
OWNER = os.environ["OWNER"]
DYNAMO_CHAT_HISTORY_TABLE = os.environ["DYNAMO_CHAT_HISTORY_TABLE"]
DYNAMO_LIBRARY_TABLE = os.environ["DYNAMO_LIBRARY_TABLE"]

# Parameter Store
ssm_chatbot = SSMParameterHelper(f"/{ENVIRONMENT}/{PROJECT_NAME}/chatbot")
PARAMETER_VALUE = json.loads(ssm_chatbot.get_parameter_value())
CHATBOT_MODEL_ID = PARAMETER_VALUE["CHATBOT_MODEL_ID"]
CHATBOT_REGION = PARAMETER_VALUE["CHATBOT_REGION"]
CHATBOT_LLM_MAX_TOKENS = int(PARAMETER_VALUE["CHATBOT_LLM_MAX_TOKENS"])
CHATBOT_HISTORY_ELEMENTS = int(PARAMETER_VALUE["CHATBOT_HISTORY_ELEMENTS"])
PINECONE_MAX_RETRIEVE_DOCUMENTS = int(PARAMETER_VALUE["PINECONE_MAX_RETRIEVE_DOCUMENTS"])
PINECONE_MIN_THRESHOLD = float(PARAMETER_VALUE["PINECONE_MIN_THRESHOLD"])
EMBEDDINGS_MODEL_ID = PARAMETER_VALUE["EMBEDDINGS_MODEL_ID"]
EMBEDDINGS_REGION = PARAMETER_VALUE["EMBEDDINGS_REGION"]
# Secrets
secret_pinecone = SecretsHelper(f"{ENVIRONMENT}/{PROJECT_NAME}/pinecone-api-key")

PINECONE_INDEX_NAME = secret_pinecone.get_secret_value("PINECONE_INDEX_NAME")
PINECONE_API_KEY = secret_pinecone.get_secret_value("PINECONE_API_KEY")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

# Inicialización de recursos
dynamo_chat_history = DynamoDBHelper(
    table_name=DYNAMO_CHAT_HISTORY_TABLE,
    pk_name="ALUMNO_ID",
    sk_name="DATE_TIME"
)

dynamo_library = DynamoDBHelper(
    table_name=DYNAMO_LIBRARY_TABLE,
    pk_name="silabus_id"
)

pinecone_helper = PineconeHelper(
    index_name=PINECONE_INDEX_NAME,
    api_key=PINECONE_API_KEY,
    embeddings_model_id=EMBEDDINGS_MODEL_ID,
    embeddings_region=CHATBOT_REGION,
    max_retrieve_documents=PINECONE_MAX_RETRIEVE_DOCUMENTS,
    min_threshold=PINECONE_MIN_THRESHOLD
)

bedrock_client = boto3.client(
    'bedrock-runtime',
    region_name=CHATBOT_REGION
)

DATA_PROMPT = """  
    ### Configuración del Chatbot "{asistente_nombre}"

    **Parámetros del Contexto de la Conversación:**
    - Rol del usuario: {usuario_rol}
    - Nombre del usuario: {usuario_nombre}
    - Curso en el que se encuentran: {curso}
    - Institución en la que se encuentran: {institucion}
    - Nombre del chatbot: {asistente_nombre}

    **Historial de la Conversación:**
    {chat_history}
                                            
    **Mensaje Original del Usuario:**
    {question}
                                            
    **Base de Conocimientos:**
    {bd_context}
                                                                                                                    
    ### Instrucciones para {asistente_nombre}:
    Eres un chatbot llamado {asistente_nombre}. 
    Debes responder y conversar de manera natural como si estuvieras respondiendo directamente la respuesta del usuario sin instrucciones de código como "Respuesta de {asistente_nombre}", 
    debes ajustarte al tono formal y amigable, adapta tus respuestas según el rol del usuario.
    Si existe una base de conocimientos (contexto), utilízala para responder y haz la tarea indicada por el usuario, y si no hay una base de conocimiento (contexto) responde de tu propio conocimiento.                    
    Si existe historial de conversación debes responder directamente al usuario sin saludar, caso contrario debes saludar primero como el "Ejemplo de respuesta".
    Responde a la siguiente pregunta considerando todos los parámetros y la información proporcionada:

    **Pregunta del Usuario:**
    {question}
                                            
    ### Ejemplo de Respuesta:
    ###Hola {usuario_nombre}, soy {asistente_nombre}, tu guía en {curso}. ¿En qué puedo ayudarte hoy en relación con {curso}?
    """

def bedrock_converse(client, prompt: str, max_tokens: int, temperature: float = 1) -> dict:
    return client.converse(
        modelId=CHATBOT_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={
            'maxTokens': max_tokens,
            'temperature': temperature,
            'topP': 0.2
        }
    )

def get_message_history(alumno_id, silabo_id, cant_items=CHATBOT_HISTORY_ELEMENTS):
    """
    Obtiene el historial de mensajes utilizando DynamoDBHelper.
    """
    try:
        # KeyConditionExpression solo puede usar claves de partición y sort
        key_condition = Key("ALUMNO_ID").eq(alumno_id)
        
        # Filtro adicional que se aplica después de la condición de clave
        filter_expression = (
            Attr("SILABUS_ID").eq(silabo_id) &
            Attr("IS_DELETED").eq(False)
        )
        
        messages = dynamo_chat_history.query_table(
            key_condition=key_condition,
            filter_expression=filter_expression,
            limit=cant_items
        )
        
        # Ordenar por fecha descendente y limitar a cant_items
        messages.sort(key=lambda x: x.get("DATE_TIME", ""), reverse=True)
        messages = messages[:cant_items]
        
        logger.info(f"Mensajes obtenidos para ALUMNO_ID: {alumno_id}, SILABUS_ID: {silabo_id}: {messages}")
        return messages
    except Exception as e:
        logger.error(f"Error al obtener los mensajes: {e}")
        return []

def upload_message(alumno_id, silabo_id, user_msg, ai_msg, prompt=""):
    """
    Sube un mensaje utilizando DynamoDBHelper.
    """
    try:
        from datetime import datetime, timedelta
        
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ttl_seconds = 604800  # 7 días
        ttl_timestamp = int((datetime.now() + timedelta(seconds=ttl_seconds)).timestamp())
        
        item = {
            "ALUMNO_ID": alumno_id,
            "DATE_TIME": current_datetime,
            "SILABUS_ID": silabo_id,
            "USER_MESSAGE": user_msg,
            "AI_MESSAGE": ai_msg,
            "PROMPT": prompt,
            "IS_DELETED": False,
            "TTL": ttl_timestamp
        }
        
        dynamo_chat_history.put_item(data=item)
        logger.info(f"Elemento subido con éxito: {item}")
    except Exception as e:
        logger.error(f"Error al subir el elemento: {e}")

def search_in_dynamodb(silabus_id):
    """
    Busca en DynamoDB por silabus_id usando DynamoDBHelper.
    """
    try:
        item = dynamo_library.get_item(partition_key=silabus_id)
        return item if item else None
    except Exception as e:
        logger.error(f"Error al buscar en DynamoDB: {e}")
        return None

def get_documents_context(question, syllabus_event_id=None, data=None):
    """
    Obtiene contexto relevante para una pregunta usando PineconeHelper.
    """
    try:
        logger.info(f"Pregunta: {question}")
        
        filter_conditions = {}
        
        # Si syllabus_event_id tiene valor, agregarlo al filtro
        if syllabus_event_id is not None:
            filter_conditions["syllabus_event_id"] = float(syllabus_event_id)
            
        # Si data tiene valor, extraer los resource_id y agregarlos al filtro
        if data and 'resources' in data:
            resource_ids = [str(item["resource_id"]) for item in data["resources"]]
            filter_conditions["resource_id"] = {"$in": resource_ids}
            
        logger.info(f"Condiciones de filtro: {filter_conditions}")
        
        # Usar search_by_text en lugar de query_pinecone
        relevant_data = pinecone_helper.search_by_text(
            query_text=question,
            filter_conditions=filter_conditions if filter_conditions else None,
            return_format="text",
            text_field="text"
        )
        
        logger.info(f"Datos relevantes: {relevant_data}\n" + '-'*100)
        return relevant_data
    except Exception as e:
        logger.error(f"Error al obtener el contexto de documentos: {e}")
        return ""

def lambda_handler(event, context):
    try:
        logger.info("Iniciando procesamiento de lambda_handler")
        
        # Maneja tanto body tipo dict como string
        if 'body' in event:
            if isinstance(event['body'], dict):
                body = event['body']
            else:
                body = json.loads(event['body'])
        else:
            body = event
        
        # Validar que los campos necesarios estén presentes usando el formato estandarizado
        required_fields = ["userId", "syllabusEventId", "message"]
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
        
        # Extraer datos del payload estandarizado
        user_id = body["userId"]
        syllabus_event_id = body["syllabusEventId"]
        message_text = body["message"]
        
        # Extraer metadatos (opcionales)
        metadata = body.get("metadata", {})
        asistente_nombre = metadata.get("assistantName", "Asistente")
        usuario_nombre = metadata.get("userName", "Estudiante")
        usuario_rol = metadata.get("userRole", "Estudiante")
        institucion = metadata.get("institution", "Universidad")
        curso = metadata.get("course", "Curso")
        
        # Obtener historial de conversación
        chat_history = get_message_history(
            user_id,
            syllabus_event_id,
            cant_items=CHATBOT_HISTORY_ELEMENTS
        )
        
        if chat_history:
            formatted_history = "Historial de conversación:\n" + "\n".join(
                [f"{i + 1}. user: {h['USER_MESSAGE']}\n   assistant: {h['AI_MESSAGE']}" 
                for i, h in enumerate(chat_history)]
            )
        else:
            formatted_history = "No hay historial previo"
 
        # Obtener contexto
        # Busca los resources de ese syllabus_event_id
        logger.info(f"Buscando recursos para syllabus_event_id: {syllabus_event_id}")
        data = search_in_dynamodb(syllabus_event_id)
        
        # Se envía los resources encontrados para buscar en Pinecone
        pinecone_context = get_documents_context(message_text, None, data)
        
        # Construir prompt
        prompt = DATA_PROMPT.format(
            asistente_nombre=asistente_nombre,
            usuario_nombre=usuario_nombre,
            usuario_rol=usuario_rol,
            institucion=institucion,
            curso=curso,
            chat_history=formatted_history,
            bd_context=pinecone_context,
            question=message_text
        )

        # Generar respuesta
        logger.info("Generando respuesta con Bedrock")
        response = bedrock_converse(
            bedrock_client,
            prompt,
            CHATBOT_LLM_MAX_TOKENS
        )
        answer = response['output']['message']['content'][0]['text']
        
        # Guardar en historial
        upload_message(
            user_id,
            syllabus_event_id,
            message_text,
            answer,
            body.get("context", "") # Por ahora está vacío
        )
        
        # Retornar respuesta en formato estandarizado
        return {            
            "statusCode": 200,
            "body": json.dumps({
                "success": True,
                "message": "Respuesta generada correctamente",
                "answer": answer,
                "inputTokens": response['usage']['inputTokens'],
                "outputTokens": response['usage']['outputTokens']
            })
        }
        
    except Exception as e:
        logger.error(f"Error en la función Lambda: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "message": str(e)
            })
        }