import json
import os
import re
from boto3.dynamodb.conditions import Key, Attr
from aje_libs.bd.helpers.pinecone_helper import PineconeHelper
from aje_libs.common.helpers.bedrock_helper import BedrockHelper
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.helpers.s3_helper import S3Helper
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.common.helpers.ssm_helper import SSMParameterHelper
from aje_libs.common.logger import custom_logger

# Configuración
ENVIRONMENT = os.environ["ENVIRONMENT"]
PROJECT_NAME = os.environ["PROJECT_NAME"]
OWNER = os.environ["OWNER"]
DYNAMO_CHAT_HISTORY_TABLE = os.environ["DYNAMO_CHAT_HISTORY_TABLE"]
DYNAMO_RESOURCES_TABLE = os.environ["DYNAMO_RESOURCES_TABLE"]
DYNAMO_RESOURCES_HASH_TABLE = os.environ["DYNAMO_RESOURCES_HASH_TABLE"]
DYNAMO_LIBRARY_TABLE = os.environ["DYNAMO_LIBRARY_TABLE"]
S3_RESOURCES_BUCKET = os.environ["S3_RESOURCES_BUCKET"]

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
secret_pinecone = SecretsHelper(f"{ENVIRONMENT}/{PROJECT_NAME}/pinecone-api")
PINECONE_INDEX_NAME = secret_pinecone.get_secret_value("PINECONE_INDEX_NAME")
PINECONE_API_KEY = secret_pinecone.get_secret_value("PINECONE_API_KEY")

logger = custom_logger(__name__, owner=OWNER, service=PROJECT_NAME)

# Inicialización de recursos
history_table_helper = DynamoDBHelper(
    table_name=DYNAMO_CHAT_HISTORY_TABLE,
    pk_name="ALUMNO_ID",
    sk_name="DATE_TIME"
)
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
    embeddings_region=CHATBOT_REGION,
    max_retrieve_documents=PINECONE_MAX_RETRIEVE_DOCUMENTS,
    min_threshold=PINECONE_MIN_THRESHOLD
)

s3_helper = S3Helper(bucket_name=S3_RESOURCES_BUCKET)
bedrock_helper = BedrockHelper(region_name=CHATBOT_REGION)

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

SYSTEM_PROMPT = """
### Configuración del Chatbot "{asistente_nombre}"

## Parámetros del Contexto Conversacional:
- Rol del usuario: {usuario_rol}
- Nombre del usuario: {usuario_nombre}
- Curso: {curso}
- Institución: {institucion}
- Nombre del Chatbot: {asistente_nombre}

## Base de Conocimientos:
{text_context}

## Instrucciones para el Chatbot ({asistente_nombre}):
- DEBE adoptar un tono formal y accesible, adaptado al rol del usuario.
- DEBE utilizar SIEMPRE la base de conocimiento proporcionada para responder la pregunta del usuario.
- SI la base de conocimientos NO contiene información específica sobre lo que el usuario consulta, DEBE responder claramente: **“No dispongo de información sobre eso.”**
- NO completar o asumir contenido que NO se encuentre en la base de conocimiento.
- PUEDE utilizar conocimiento general del dominio (ej. explicaciones técnicas o teóricas sobre {curso}) para ayudar al usuario con preguntas conceptuales o académicas.
- DEBE mantener la relevancia con el contexto del curso y la institución en todo momento.
- NO genere contenido fuera del ámbito educativo a menos que se le solicite explícitamente.
"""

SYSTEM_PROMPT2 = """
Eres un asistente llamado {asistente_nombre} que puede ayudar al usuario con sus preguntas usando **únicamente información confiable**.

Contexto del usuario:
- Rol del usuario: {usuario_rol}
- Nombre del usuario: {usuario_nombre}
- Curso: {curso}
- Institución: {institucion}

Instrucciones del modelo:
- Debe proporcionar una respuesta concisa a preguntas sencillas cuando la respuesta se encuentre directamente en los resultados
  de búsqueda. Sin embargo, en el caso de preguntas de sí/no, proporcione algunos detalles.
- Si la pregunta requiere un razonamiento complejo, debe buscar información relevante en los resultados de búsqueda y resumir la
  respuesta basándose en dicha información mediante un razonamiento lógico.
- Si los resultados de búsqueda no contienen información que pueda responder a la pregunta, indique que no pudo encontrar una
  respuesta exacta. Si los resultados de búsqueda son completamente irrelevantes, indique que no pudo encontrar una respuesta exacta y resuma los resultados.
- **NO uses información externa que no esté en los resultados de búsqueda**, excepto para dar explicaciones conceptuales generales del curso **{curso}**.
- **NO inventes información** ni generes contenido fuera del ámbito educativo salvo que el usuario lo solicite explícitamente.
- Mantén **siempre un tono formal, claro y enfocado al ámbito académico**.
"""

def get_converse_response(messages: list, system_prompt: str, max_tokens: int, temperature: float = 1.0) -> dict:
    """
    Conversa con el modelo de Bedrock usando un prompt de sistema separado y mensajes estructurados.
    
    Parámetros:
    - messages: lista de mensajes estructurados entre user y assistant
    - system_prompt: texto con las instrucciones iniciales del sistema
    - max_tokens: número máximo de tokens de respuesta
    - temperature: control de aleatoriedad
    """

    logger.info(json.dumps(messages, indent=2))

    tool_config = {
        "tools": [
            {
                "toolSpec": {
                    "name": "get_resources",
                    "description": "Obtiene los títulos de los recursos.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {},
                            "required": []
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "retrieve_context",
                    "description": "Consulta la base vectorial de Pinecone y devuelve los fragmentos relevantes según la consulta del usuario.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Consulta o pregunta del usuario a buscar en la base vectorial"
                                },
                            },
                            "required": ["query"]
                        }
                    }
                }
            }
        ],
        "toolChoice": {
            "auto": {}
        }
    }

    parameters = {
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": 0.2
    }

    response = bedrock_helper.converse(
        model=CHATBOT_MODEL_ID,
        messages=messages,
        system_prompt=system_prompt,
        parameters=parameters,
        tool_config=tool_config
    )

    return response

def get_message_history(alumno_id, silabo_id, cant_items=CHATBOT_HISTORY_ELEMENTS):
    """
    Obtiene el historial de mensajes utilizando DynamoDBHelper.
    """
    try:
        # Usa query si estás buscando por ALUMNO_ID específico:
        messages = history_table_helper.query_table(
            key_condition=f"{history_table_helper.pk_name} = :user_id",
            filter_expression=f"SILABUS_ID = :syllabus_id AND IS_DELETED = :is_deleted",
            expression_attribute_values={
                ":user_id": alumno_id,
                ":syllabus_id": silabo_id,
                ":is_deleted": False
            },
            limit=cant_items * 5,
            scan_forward=False  # Para obtener los más recientes primero
        )
        
        # Ordenar por fecha descendente y limitar a cant_items
        messages.sort(key=lambda x: x.get("DATE_TIME", ""), reverse=True)
        messages = messages[:cant_items]
        messages.reverse()
        
        formatted_messages = []
        for msg in messages:
            if msg.get("USER_MESSAGE"):
                formatted_messages.append({
                    "role": "user",
                    "content": [{"text": msg["USER_MESSAGE"]}]
                })
            if msg.get("AI_MESSAGE"):
                formatted_messages.append({
                    "role": "assistant",
                    "content": [{"text": msg["AI_MESSAGE"]}]
                })

        logger.info(f"Mensajes obtenidos para ALUMNO_ID: {alumno_id}, SILABUS_ID: {silabo_id}: {messages}")
        return formatted_messages
    except Exception as e:
        logger.error(f"Error al obtener los mensajes: {e}")
        return []

def upload_message(alumno_id, silabo_id, user_msg, ai_msg, prompt=""):
    """
    Sube un mensaje a DynamoDB. Permite marcar mensajes como irrelevantes para el contexto futuro.

    :param alumno_id: ID del alumno
    :param silabo_id: ID del sílabo
    :param user_msg: Mensaje del usuario
    :param ai_msg: Respuesta del modelo
    :param prompt: Prompt del sistema usado
    """
    try:
        from datetime import datetime, timedelta
        
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ttl_seconds = 604800  # 7 días
        ttl_timestamp = int((datetime.now() + timedelta(seconds=ttl_seconds)).timestamp())
        
        item = {
            "ALUMNO_ID": alumno_id, # En realidad es el ID del Usuario
            "DATE_TIME": current_datetime,
            "SILABUS_ID": silabo_id,
            "USER_MESSAGE": user_msg,
            "AI_MESSAGE": ai_msg,
            "PROMPT": prompt,
            "IS_DELETED": False,
            "TTL": ttl_timestamp
        }

        history_table_helper.put_item(data=item)
        logger.info(f"Elemento subido con éxito: {item}")
    except Exception as e:
        logger.error(f"Error al subir el elemento: {e}")

def get_resource_ids_by_syllabus(silabus_id):
    """
    Busca en DynamoDB por silabus_id usando DynamoDBHelper los id de los recursos.
    """
    try:
        item = library_table_helper.get_item(partition_key=silabus_id)
        return item if item else None
    except Exception as e:
        logger.error(f"Error al buscar en DynamoDB: {e}")
        return None

def get_documents_context_json(question, data=None):
    """
    Obtiene contexto relevante para una pregunta usando PineconeHelper.
    """
    try:
        logger.info(f"Pregunta: {question}")
            
        # Si data tiene valor, extraer los resource_id y agregarlos al filtro
        filter_conditions = {}
        if data and 'resources' in data:
            resource_ids = [str(item["resource_id"]) for item in data["resources"]]
            filter_conditions["resource_id"] = {"$in": resource_ids}
            
        logger.info(f"Condiciones de filtro: {filter_conditions}")
        
        # Obtener resultados crudos de Pinecone
        raw_results = pinecone_helper.search_by_text(
            query_text=question,
            filter_conditions=filter_conditions if filter_conditions else None,
            return_format="raw"
        )
        
        # Convertir a JSON estructurado para Nova
        json_chunks = {}
        for i, match in enumerate(raw_results):
            chunk_text = (
                match.get("metadata", {})
                .get("text", "")
                .replace("\n", " ")
                .strip()
            )
            resource_id = match.get("metadata", {}).get("resource_id", "unknown")
            json_chunks[f"chunk_{i+1}"] = {
                "text": chunk_text,
                "resource_id": resource_id,
                "score": match.get("score")
            }

        logger.info(f"Chunks JSON: {json_chunks}")
        return json_chunks
    
    except Exception as e:
        logger.error(f"Error al obtener el contexto JSON de documentos: {e}")
        return {}

# Extrar el contenido válido
def extract_relevant_text_from_response(text: str, tags: list[str] = None) -> str:
    """
    Extrae el texto útil eliminando contenido entre etiquetas de razonamiento interno, como <thinking>...</thinking>.

    :param text: Respuesta completa generada por el modelo.
    :param tags: Lista de dos strings que representan la etiqueta de apertura y cierre. Ejemplo: ["<thinking>", "</thinking>"]
    :return: Texto limpio, sin contenido de razonamiento interno si se encuentran las etiquetas.
    """
    if not tags or len(tags) != 2:
        return text.strip()
    
    pattern = f"(?s).*{re.escape(tags[0])}(.*?){re.escape(tags[1])}"
    match = re.search(pattern, text)
    
    if match:
        # Elimina también el bloque entre las etiquetas
        cleaned = re.sub(pattern, "", text).strip()
        return cleaned
    return text.strip()

# Format Success Response
def format_success_response(answer_text: str, usage_info: dict, message: str = "Respuesta generada correctamente") -> dict:
    """
    Formatea una respuesta HTTP estándar para Lambda con estructura unificada.

    :param answer_text: Texto final generado o devuelto al usuario.
    :param usage_info: Diccionario con tokens utilizados (input/output).
    :param message: Mensaje contextual que se desea mostrar (por ejemplo, si vino de herramienta).
    :return: Diccionario con statusCode y body estandarizado.
    """
    return {
        "statusCode": 200,
        "body": json.dumps({
            "success": True,
            "message": message,
            "answer": answer_text,
            "input_tokens": usage_info.get('inputTokens', 0),
            "output_tokens": usage_info.get('outputTokens', 0)
        })
    }

# Tools
def get_resources(silabus_id) -> list[str]:
    """
    Obtiene los títulos de los recursos asociados a un silabo específico.

    :param silabus_id: ID del silabo
    :return: Lista de títulos de recursos
    """
    try:
        library_item = library_table_helper.get_item(silabus_id)
        if library_item and "resources" in library_item:
            resources = library_item["resources"]
        else:
            logger.warning(f"No se encontraron recursos para el silabo {silabus_id}")
            return []
        
        resource_ids = [resource["resource_id"] for resource in resources]
        logger.info(f"Se encontraron {len(resource_ids)} resource_id(s) en el silabo {silabus_id}")

        titles = []
        for resource_id in resource_ids:
            title = get_title_from_resource_id(resource_id)
            if title:
                titles.append(title)

        return titles
    except Exception as e:
        logger.error(f"Error obteniendo títulos de recursos para el silabo {silabus_id}: {e}")
        return []
    
def get_title_from_resource_id(resource_id) -> str:
    """
    Obtiene el título de un recurso dado su ID.

    :param resource_id: ID del recurso
    :return: Título del recurso o None si no se encuentra
    """
    try:
        item = files_table_helper.get_item(resource_id)
        if item and "resource_title" in item:
            return item["resource_title"]
        else:
            logger.warning(f"No se encontró el recurso con ID {resource_id}")
            return None
    except Exception as e:
        logger.error(f"Error obteniendo título del recurso {resource_id}: {e}")
        return None

def retrieve_context(syllabus_event_id, message_text, resources):
    # Obtener recursos
    if resources:
        if isinstance(resources, str):
            resources = resources.split(",")
        data = {
            "resources": [{"resource_id": rid} for rid in resources]
        }             
    else:
        logger.info(f"Buscando resource_ids para syllabus_event_id: {syllabus_event_id}")
        data = get_resource_ids_by_syllabus(syllabus_event_id)

    # Consultar Pinecone
    text_context = get_documents_context_json(message_text, data)
    return text_context

# Others
def invoke_with_prompt(
        user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
        messages: list, system_prompt, max_tokens, temperature
    ):
    content = [
        {
            'text': message_text
        }
    ]
    return invoke(
        user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
        content, messages, system_prompt, max_tokens, temperature
    )

def invoke(
        user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
        content, messages: list, system_prompt, max_tokens, temperature
    ):

    # print(f"User: {json.dumps(content, indent=2)}")

    messages.append(
        {
            "role": "user", 
            "content": content
        }
    )
    response = get_converse_response(messages, system_prompt, max_tokens, temperature)
    logger.info(f"Agent: {response}")

    return handle_response(
        user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
        messages, system_prompt, response
    )

def handle_response(
        user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
        messages: list, system_prompt, response
    ):
    messages.append(response['output']['message'])

    # Determinar por qué se detuvo el modelo (respuesta directa o llamada a herramienta)
    content_blocks = response.get('output', {}).get('message', {}).get('content', [])
    stop_reason = response['stopReason']
    usage_info = response['usage']

    # Caso 1: Respuesta directa
    if stop_reason in ['end_turn', 'stop_sequence']:
        answer_text = next((block.get('text', '') for block in content_blocks if 'text' in block), '')
        relevant_text = extract_relevant_text_from_response(answer_text, ["<thinking>", "</thinking>"])
        upload_message(alumno_id=user_id, silabo_id=syllabus_event_id, user_msg=message_text, ai_msg=relevant_text, prompt=system_prompt)
        return format_success_response(relevant_text, usage_info)
    # Caso 2: Tool Calling
    elif stop_reason == 'tool_use':
        tool_block = next((block for block in content_blocks if 'toolUse' in block), None)
        if not tool_block:
            raise ValueError("No se encontró bloque 'toolUse' en la respuesta.")

        # Extraer razonamiento del modelo
        thought_process = next(
            (block["text"] for block in content_blocks if "text" in block),
            "[Sin razonamiento textual del modelo]"
        )
        logger.info(f"Pensamiento previo a la herramienta: {thought_process}")
                
        tool_name = tool_block['toolUse']['name']
        tool_input = tool_block['toolUse']['input']
        tool_use_id = tool_block['toolUse']['toolUseId']
        logger.info(f"Herramienta solicitada: {tool_name} con input: {tool_input}")

        # Ejecutar herramienta correspondiente 
        if tool_name == "get_resources":
            resource_titles = get_resources(syllabus_event_id)
            if not resource_titles:
                tool_result_text = (
                    f"{usuario_nombre}, no se encontraron recursos disponibles "
                    f"para el curso *{curso}* en este momento."
                )
            else:
                recursos_listados = "\n- " + "\n- ".join(resource_titles)
                tool_result_text = (
                    f"{usuario_nombre}, estos son los recursos disponibles "
                    f"para el curso *{curso}*:\n"
                    f"{recursos_listados}"
                )

        elif tool_name == "retrieve_context":
            pinecone_chunks = retrieve_context(syllabus_event_id, message_text, resources)
            
            # Retornar resultado a Nova
            #tool_response = []
            tool_result = [{
                "toolResult": {
                    "toolUseId": tool_use_id,
                    "content": [{"json": pinecone_chunks}],
                    "status": "success"
                }
            }]
            #tool_response.append({'toolResult': tool_result})
        
            return invoke(
                user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
                tool_result, messages, system_prompt, CHATBOT_LLM_MAX_TOKENS, 0
            )

        else:
            raise ValueError(f"Herramienta no reconocida: {tool_name}")
        
        upload_message(alumno_id=user_id, silabo_id=syllabus_event_id, user_msg=message_text, ai_msg=tool_result_text, prompt=system_prompt)
        return format_success_response(tool_result_text, usage_info, message="Herramienta ejecutada correctamente")   
    # Caso 3: Hit token limit (this is one way to handle it.)
    elif stop_reason == 'max_tokens':
        return invoke_with_prompt(
            user_id, syllabus_event_id, "Por favor continue.", usuario_nombre, curso, resources,
            messages, system_prompt, CHATBOT_LLM_MAX_TOKENS, 0.7
        )
    # Caso 4: Otro motivo de detención no manejado
    else:
        logger.warning(f"Razón de detención no reconocida: {stop_reason}")
        raise ValueError(f"Unknown stop reason: {stop_reason}")

def lambda_handler(event, context):
    try:
        body = event.get('body', event)
        if isinstance(body, str):
            body = json.loads(body)

        required_fields = ["user_id", "syllabus_event_id", "message"]
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            return {
                "success": False,
                "message": f"Campos requeridos faltantes: {missing_fields}",
                "statusCode": 400,
                "error": {
                    "code": "MISSING_FIELDS",
                    "details": f"Campos requeridos faltantes: {missing_fields}"
                }
            }
        
        user_id = body["user_id"]
        syllabus_event_id = body["syllabus_event_id"]
        message_text = body["message"]
        asistente_nombre = body["asistente_nombre"]
        usuario_nombre = body["usuario_nombre"]
        usuario_rol = body["usuario_rol"]
        institucion = body["institucion"]
        curso = body["curso"]
        resources = body.get("resources", None)
        
        # Obtener historial de conversación
        messages = get_message_history(user_id, syllabus_event_id)
        logger.info(f"Messages: {messages}")
        # Armar el prompt
        system_prompt = SYSTEM_PROMPT2.format(
            asistente_nombre=asistente_nombre,
            usuario_rol=usuario_rol,
            usuario_nombre=usuario_nombre,
            curso=curso,
            institucion=institucion
        )
        logger.info(f"System prompt: {system_prompt}")

        return invoke_with_prompt(
            user_id, syllabus_event_id, message_text, usuario_nombre, curso, resources,
            messages, system_prompt, CHATBOT_LLM_MAX_TOKENS, 0.7
        )

    except Exception as e:
        logger.error(f"Error en la función Lambda: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "message": str(e)
            })
        }

'''
def lambda_handler(event, context):
    try:
        body = event.get('body', event)
        if isinstance(body, str):
            body = json.loads(body)

        required_fields = ["user_id", "syllabus_event_id", "message"]
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            return {
                "success": False,
                "message": f"Campos requeridos faltantes: {missing_fields}",
                "statusCode": 400,
                "error": {
                    "code": "MISSING_FIELDS",
                    "details": f"Campos requeridos faltantes: {missing_fields}"
                }
            }
        
        user_id = body["user_id"]
        syllabus_event_id = body["syllabus_event_id"]
        message_text = body["message"]
        asistente_nombre = body["asistente_nombre"]
        usuario_nombre = body["usuario_nombre"]
        usuario_rol = body["usuario_rol"]
        institucion = body["institucion"]
        curso = body["curso"]
        resources = body.get("resources", None)
        
        # Obtener historial de conversación
        formatted_history = get_message_history(user_id, syllabus_event_id)
        formatted_history.append({"role": "user", "content": [{"text": message_text}]})
        
        system_prompt = SYSTEM_PROMPT2.format(
            asistente_nombre=asistente_nombre,
            usuario_rol=usuario_rol,
            usuario_nombre=usuario_nombre,
            curso=curso,
            institucion=institucion
        )
        logger.info(f"System prompt: {system_prompt}")

        initial_response = get_converse_response(
            messages=formatted_history,
            system_prompt=system_prompt,
            max_tokens=CHATBOT_LLM_MAX_TOKENS,
            temperature=0.7
        )
        formatted_history.append(initial_response['output']['message'])
        logger.info(f"Respuesta inicial: {initial_response}")
        
        # Determinar por qué se detuvo el modelo (respuesta directa o llamada a herramienta)
        content_blocks = initial_response.get('output', {}).get('message', {}).get('content', [])
        stop_reason = initial_response['stopReason']
        usage_info = initial_response['usage']

        # Caso 1: Respuesta directa
        if stop_reason in ['end_turn', 'stop_sequence']:
            answer_text = next((block.get('text', '') for block in content_blocks if 'text' in block), '')
            relevant_text = extract_relevant_text_from_response(answer_text, ["<thinking>", "</thinking>"])
            
            upload_message(alumno_id=user_id, silabo_id=syllabus_event_id, user_msg=message_text, ai_msg=relevant_text, prompt=system_prompt)
                
            return format_success_response(relevant_text, usage_info)
        # Caso 2: Tool Calling
        elif stop_reason == 'tool_use':
            
            tool_block = next((block for block in content_blocks if 'toolUse' in block), None)
            if not tool_block:
                raise ValueError("No se encontró bloque 'toolUse' en la respuesta.")

            # Extraer razonamiento del modelo
            thought_process = next(
                (block["text"] for block in content_blocks if "text" in block),
                "[Sin razonamiento textual del modelo]"
            )
            logger.info(f"Pensamiento previo a la herramienta: {thought_process}")
                
            tool_name = tool_block['toolUse']['name']
            tool_input = tool_block['toolUse']['input']
            tool_use_id = tool_block['toolUse']['toolUseId']
            logger.info(f"Herramienta solicitada: {tool_name} con input: {tool_input}")

            # Ejecutar herramienta correspondiente 
            if tool_name == "top_song":
                sign = tool_input["sign"]
                song, artist = get_top_song(sign)
                tool_result_text = f"La canción más popular en la estación {sign} es '{song}' de {artist}."
                
            elif tool_name == "get_resources":
                resource_titles = get_resources(syllabus_event_id)

                if not resource_titles:
                    tool_result_text = (
                        f"{usuario_nombre}, no se encontraron recursos disponibles "
                        f"para el curso *{curso}* en este momento."
                    )
                else:
                    recursos_listados = "\n- " + "\n- ".join(resource_titles)
                    tool_result_text = (
                        f"{usuario_nombre}, estos son los recursos disponibles "
                        f"para el curso *{curso}*:\n"
                        f"{recursos_listados}"
                    )

            elif tool_name == "retrieve_context":

                pinecone_chunks = retrieve_context(syllabus_event_id, message_text, resources)

                # Retornar resultado a Nova
                tool_result = {
                    "role": "user",
                    "content": [
                        {
                            "toolResult": {
                                "toolUseId": tool_use_id,
                                "content": [{"json": pinecone_chunks}],
                                "status": "success"
                            }
                        }
                    ]
                }

                formatted_history.append(tool_result)

                # Segunda llamada a Nova para generar respuesta final
                enriched_response = get_converse_response(
                    messages=formatted_history,
                    system_prompt=system_prompt,
                    max_tokens=CHATBOT_LLM_MAX_TOKENS,
                    temperature=0
                )
                logger.info(f"Respuesta completa del modelo de herramienta: {enriched_response}")
                message_obj = enriched_response.get('output', {}).get('message', {})
                content_blocks = message_obj.get('content', [])
                tool_result_text = next((block.get('text', '') for block in content_blocks if 'text' in block), '')
                usage_info = enriched_response.get('usage')

            else:
                raise ValueError(f"Herramienta no reconocida: {tool_name}")

            # Guardar en historial
            upload_message(
                alumno_id=user_id,
                silabo_id=syllabus_event_id,
                user_msg=message_text,
                ai_msg=tool_result_text,
                prompt=system_prompt
            )

            # Retornar respuesta generada por herramienta
            return format_success_response(tool_result_text, usage_info, message="Herramienta ejecutada correctamente")   
        elif stop_reason == 'max_tokens':
            # Hit token limit (this is one way to handle it.)
            #await self.invoke_with_prompt('Please continue.')
            formatted_history.append({"role": "user", "content": [{"text": "Por favor continue."}]})
            initial_response = get_converse_response(
                messages=formatted_history,
                system_prompt=system_prompt,
                max_tokens=CHATBOT_LLM_MAX_TOKENS,
                temperature=1.0
            )

        # Caso 3: Otro motivo de detención no manejado
        else:
            logger.warning(f"Razón de detención no reconocida: {stop_reason}")
            raise ValueError(f"Unknown stop reason: {stop_reason}")
        
    except Exception as e:
        logger.error(f"Error en la función Lambda: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "message": str(e)
            })
        }
'''