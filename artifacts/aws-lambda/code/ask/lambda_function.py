import json
import os
import boto3
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.bd.helpers.pinecone_helper import PineconeHelper
from aje_libs.common.logger import custom_logger
#from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
#load_dotenv()
  
BEDROCK_CHATBOT_MODEL_ID = os.environ.get("BEDROCK_CHATBOT_MODEL_ID", "us.meta.llama3-2-3b-instruct-v1:0")
BEDROCK_CHATBOT_REGION = os.environ.get("BEDROCK_CHATBOT_REGION", "us-west-2")
BEDROCK_CHATBOT_LLM_MAX_TOKENS = int(os.environ.get("BEDROCK_CHATBOT_LLM_MAX_TOKENS", 1024))
CHATBOT_HISTORY_ELEMENTS = int(os.environ.get("CHATBOT_HISTORY_ELEMENTS", 6))

# Environment variables for DynamoDB and Pinecone
DYNAMO_CHAT_HISTORY_TABLE = os.environ.get("DYNAMO_CHAT_HISTORY_TABLE")
DYNAMO_LIBRARY_TABLE = os.environ.get("DYNAMO_LIBRARY_TABLE")
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
EMBEDDINGS_MODEL_ID = os.environ.get("EMBEDDINGS_MODEL_ID")
PINECONE_MAX_RETRIEVE_DOCUMENTS = int(os.environ.get("PINECONE_MAX_RETRIEVE_DOCUMENTS", 6))
PINECONE_MIN_THRESHOLD = float(os.environ.get("PINECONE_MIN_THRESHOLD", 0.5))

#AWS_PROFILE = os.environ.get("AWS_PROFILE")
#AWS_REGION = os.environ.get("AWS_REGION")

OWNER = os.environ.get("OWNER")
PROJECT_NAME = os.environ.get("PROJECT_NAME")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

logger.info("Iniciando logging")
#boto3.setup_default_session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

# Inicialización de recursos (fuera del handler para reutilización en ejecuciones posteriores)
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
    embeddings_region=BEDROCK_CHATBOT_REGION,
    max_retrieve_documents=PINECONE_MAX_RETRIEVE_DOCUMENTS,
    min_threshold=PINECONE_MIN_THRESHOLD
)

bedrock_client = boto3.client(
    'bedrock-runtime',
    region_name=BEDROCK_CHATBOT_REGION
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
        modelId=BEDROCK_CHATBOT_MODEL_ID,
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
        key_condition = f"ALUMNO_ID = '{alumno_id}'"
        filter_expression = f"SILABUS_ID = '{silabo_id}' AND IS_DELETED = false"
        
        # Usamos scan_table ya que necesitamos aplicar un filtro complejo
        messages = dynamo_chat_history.scan_table(
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
        
        message = body
        logger.info(f"Mensaje recibido: {message.keys()}")
        
        # Verificar que los campos necesarios estén presentes
        required_fields = ["user_id", "syllabus_event_id", "message"]
        missing_fields = [field for field in required_fields if field not in message]
        
        if missing_fields:
            logger.error(f"Campos requeridos faltantes: {missing_fields}")
            return {
                "success": False,
                "answer": "Faltan campos requeridos en la solicitud",
                "message": f"Campos requeridos faltantes: {missing_fields}"
            }
        
        # Obtener historial de conversación
        chat_history = get_message_history(
            message["user_id"],
            message["syllabus_event_id"],
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
        logger.info(f"Buscando recursos para syllabus_event_id: {message['syllabus_event_id']}")
        data = search_in_dynamodb(message["syllabus_event_id"])
        
        # Se envía los resources encontrados para buscar en Pinecone
        pinecone_context = get_documents_context(message["message"], None, data)
        
        # Construir prompt
        prompt = DATA_PROMPT.format(
            asistente_nombre=message.get("asistente_nombre", "Asistente"),
            usuario_nombre=message.get("usuario_nombre", "Estudiante"),
            usuario_rol=message.get("usuario_rol", "Estudiante"),
            institucion=message.get("institucion", "Universidad"),
            curso=message.get("curso", "Curso"),
            chat_history=formatted_history,
            bd_context=pinecone_context,
            question=message.get("message", "")
        )

        # Generar respuesta
        logger.info("Generando respuesta con Bedrock")
        response = bedrock_converse(
            bedrock_client,
            prompt,
            BEDROCK_CHATBOT_LLM_MAX_TOKENS
        )
        answer = response['output']['message']['content'][0]['text']
        
        # Guardar en historial
        upload_message(
            message.get("user_id", ""),
            message.get("syllabus_event_id", ""),
            message.get("message", ""),
            answer,
            message.get("context", "") # Por ahora está vacío
        )
        
        # Retornar respuesta
        logger.info("Procesamiento completado con éxito")
        return {
                "answer": answer,
                "input_tokens": response['usage']['inputTokens'],
                "output_tokens": response['usage']['outputTokens'],
                "success": True
            }
        
    except Exception as e:
        logger.error(f"Error en la función Lambda: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
                "success": False,
                "answer": "Ocurrió un error procesando tu solicitud",
                "message": str(e)
            }
