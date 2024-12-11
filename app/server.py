# server.py
import asyncio  # Importa el módulo asyncio para manejar operaciones asíncronas
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, APIRouter  # Importa clases y funciones de FastAPI
from fastapi.middleware.cors import CORSMiddleware  # Importa el middleware CORS para manejar solicitudes de diferentes orígenes
import os  # Importa el módulo os para interactuar con el sistema operativo
from dotenv import load_dotenv  # Importa la función load_dotenv para cargar variables de entorno desde un archivo .env
from app.process_file import process_file_parallel  # Importa la función process_file_parallel del módulo process_file
import pymongo  # Importa el módulo pymongo para interactuar con MongoDB
import logging  # Importa el módulo logging para registrar mensajes de log
from bson import ObjectId  # Importa ObjectId de bson para manejar identificadores de objetos de MongoDB
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  # Importa ejecutores de hilos y procesos
import tempfile  # Importa el módulo tempfile para crear archivos temporales
from datetime import datetime  # Importa datetime para manejar fechas y horas
import time  # Importa el módulo time para manejar operaciones relacionadas con el tiempo
import threading  # Importa el módulo threading para manejar hilos

router = APIRouter(prefix="", tags=["server"])  # Crea un enrutador de FastAPI con prefijo vacío y etiqueta "server"

# Configurar logging
logging.basicConfig(
   level=logging.INFO,  # Establece el nivel de log en INFO
   format='%(asctime)s - %(levelname)s - %(message)s',  # Define el formato de los mensajes de log
   handlers=[
       logging.StreamHandler(),  # Agrega un manejador de log para la salida estándar
       logging.FileHandler('server.log')  # Agrega un manejador de log para un archivo
   ]
)

def json_compatible(obj):  # Define una función para convertir objetos a un formato compatible con JSON
   if isinstance(obj, ObjectId):  # Si el objeto es un ObjectId, lo convierte a cadena
       return str(obj)
   elif isinstance(obj, dict):  # Si el objeto es un diccionario, convierte sus valores recursivamente
       return {key: json_compatible(value) for key, value in obj.items()}
   elif isinstance(obj, list):  # Si el objeto es una lista, convierte sus elementos recursivamente
       return [json_compatible(item) for item in obj]
   else:  # Si el objeto no es ninguno de los anteriores, lo devuelve tal cual
       return obj

load_dotenv()  # Carga las variables de entorno desde un archivo .env

MONGO_URI = os.getenv("MONGO_URI")  # Obtiene la URI de MongoDB desde las variables de entorno
DATABASE_NAME = os.getenv("DATABASE_NAME")  # Obtiene el nombre de la base de datos desde las variables de entorno
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", os.cpu_count()))  # Obtiene el número de procesos desde las variables de entorno o usa el número de CPUs
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))  # Obtiene el tamaño de los chunks desde las variables de entorno o usa 10000
MAX_WORKERS = int(os.getenv("MAX_WORKERS", os.cpu_count()))  # Obtiene el número máximo de trabajadores desde las variables de entorno o usa el número de CPUs

client = pymongo.MongoClient(
   MONGO_URI,  # Conecta a MongoDB usando la URI
   maxPoolSize=MAX_WORKERS,  # Establece el tamaño máximo del pool de conexiones
   connectTimeoutMS=30000,  # Establece el tiempo de espera para conectar en milisegundos
   socketTimeoutMS=None,  # No establece un tiempo de espera para los sockets
   connect=False,  # No conecta automáticamente
   compressors='snappy'  # Usa el compresor 'snappy'
)

db = client[DATABASE_NAME]  # Obtiene la base de datos
collection = db['genomas']  # Obtiene la colección 'genomas'

if collection.name in db.list_collection_names():  # Si la colección existe en la base de datos
   primer_documento = collection.find_one()  # Obtiene el primer documento de la colección
   ultimo_documento = collection.find_one(sort=[('_id', -1)])  # Obtiene el último documento de la colección
   sample_document = {**primer_documento, **ultimo_documento} if primer_documento and ultimo_documento else {}  # Combina los documentos si ambos existen
   valid_fields = sample_document.keys()  # Obtiene las claves del documento combinado como campos válidos

logging.info(f"Conectado a MongoDB: processes={NUM_PROCESSES}, chunk={CHUNK_SIZE}, workers={MAX_WORKERS}")  # Registra un mensaje de conexión exitosa

app = FastAPI()  # Crea una instancia de FastAPI

app.add_middleware(
   CORSMiddleware,  # Agrega el middleware CORS
   allow_origins=["http://localhost:4200"],  # Permite solicitudes desde este origen
   allow_credentials=True,  # Permite el uso de credenciales
   allow_methods=["GET", "POST", "PUT", "DELETE"],  # Permite estos métodos HTTP
   allow_headers=["*"],  # Permite todos los encabezados
   expose_headers=["*"]  # Expone todos los encabezados
)

def process_chunk(process_id, skip, limit, query, sort_by, sort_order, max_workers, hint=None):  # Define una función para procesar un chunk de datos
   with ThreadPoolExecutor(max_workers=max_workers) as thread_executor:  # Crea un ejecutor de hilos con el número máximo de trabajadores
       def thread_query(thread_id):  # Define una función para ejecutar una consulta en un hilo
           try:
               base_query = query if query else {}  # Usa la consulta proporcionada o una consulta vacía
               cursor = collection.find(base_query)  # Ejecuta la consulta en la colección
               
               if sort_by:  # Si se proporciona un campo de ordenación
                   cursor = cursor.sort(sort_by, sort_order)  # Ordena los resultados
               
               cursor = cursor.skip(skip).limit(limit)  # Salta y limita los resultados
               
               if hint:  # Si se proporciona una pista de índice
                   logging.info(f"P{process_id}-T{thread_id}: Using hint {hint}")  # Registra el uso de la pista
                   cursor = cursor.hint(hint)  # Usa la pista de índice
               
               results = [
                   {**{k: str(v) if isinstance(v, ObjectId) else v for k, v in doc.items()}}
                   for doc in cursor
               ]  # Convierte los resultados a un formato compatible con JSON
               
               logging.info(f"P{process_id}-T{thread_id}: Found {len(results)} docs")  # Registra el número de documentos encontrados
               return results  # Devuelve los resultados
           except Exception as e:  # Si ocurre una excepción
               logging.error(f"P{process_id}-T{thread_id}: Error - {str(e)}")  # Registra el error
               return []

       futures = []  # Crea una lista para almacenar futuros
       threads_per_process = max_workers  # Establece el número de hilos por proceso
       for t in range(threads_per_process):  # Para cada hilo
           futures.append(thread_executor.submit(thread_query, t))  # Envía la consulta del hilo al ejecutor
       
       results = []  # Crea una lista para almacenar los resultados
       for f in futures:  # Para cada futuro
           try:
               result = f.result()  # Obtiene el resultado del futuro
               if result:  # Si hay resultados
                   results.extend(result)  # Agrega los resultados a la lista
           except Exception as e:  # Si ocurre una excepción
               logging.error(f"Thread error: {e}")  # Registra el error
       
       return results  # Devuelve los resultados

def parallel_process_file(file_path):  # Define una función para procesar un archivo en paralelo
   process_file_parallel(file_path)  # Llama a la función process_file_parallel con la ruta del archivo

@router.post("/process_file")  # Define una ruta POST para procesar archivos
async def process_file(
    background_tasks: BackgroundTasks,  # Recibe tareas en segundo plano
    file: UploadFile = File(...)  # Recibe un archivo subido
):
    """Procesa un archivo VCF en segundo plano"""
    start_time = time.time()  # Obtiene el tiempo de inicio
    try:
        with tempfile.NamedTemporaryFile(
            delete=False, 
            prefix=f"vcf_{datetime.now().strftime('%Y%m%d_%H%M%S')}_", 
            suffix=f"_{file.filename}"
        ) as temp_file:  # Crea un archivo temporal
            content = await file.read()  # Lee el contenido del archivo subido
            temp_file.write(content)  # Escribe el contenido en el archivo temporal
            temp_file_path = temp_file.name  # Obtiene la ruta del archivo temporal
            
            if not content:  # Si el contenido está vacío
                logging.warning("Archivo vacío recibido")  # Registra una advertencia
                return {
                    "error": "El archivo está vacío",
                    "process_time": round(time.time() - start_time, 3)
                }
            
            if not file.filename.lower().endswith('.vcf'):  # Si el archivo no tiene la extensión .vcf
                logging.warning(f"Archivo inválido: {file.filename}")  # Registra una advertencia
                return {
                    "error": "Por favor, suba un archivo VCF",
                    "process_time": round(time.time() - start_time, 3)
                }
        
        def cleanup_temp_file():  # Define una función para limpiar el archivo temporal
            try:
                os.unlink(temp_file_path)  # Elimina el archivo temporal
                logging.info(f"Archivo temporal {temp_file_path} eliminado")  # Registra la eliminación
            except Exception as e:  # Si ocurre una excepción
                logging.error(f"Error eliminando archivo temporal: {e}")  # Registra el error
        
        background_tasks.add_task(parallel_process_file, temp_file_path)  # Agrega la tarea de procesamiento en paralelo
        background_tasks.add_task(cleanup_temp_file)  # Agrega la tarea de limpieza del archivo temporal

        return {
            "message": f"Archivo '{file.filename}' cargado. Procesamiento iniciado.",
            "temp_file": temp_file_path,
            "process_time": round(time.time() - start_time, 3)
        }
    except Exception as e:  # Si ocurre una excepción
        logging.error(f"Error procesando archivo: {str(e)}")  # Registra el error
        return {
            "error": f"Error procesando archivo: {str(e)}",
            "process_time": round(time.time() - start_time, 3)
        }

async def get_variants_parallel(query=None, sort_by="_id", sort_order=1, page=1, page_size=10, hint=None):  # Define una función para obtener variantes en paralelo
    docs_per_process = max(1, page_size // NUM_PROCESSES)  # Calcula el número de documentos por proceso
    remaining = page_size  # Inicializa el número de documentos restantes
    current_skip = (page - 1) * page_size  # Calcula el número de documentos a saltar
    
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as process_executor:  # Crea un ejecutor de procesos con el número máximo de procesos
        futures = []  # Crea una lista para almacenar futuros
        while remaining > 0:  # Mientras queden documentos por procesar
            futures.append(process_executor.submit(
                process_chunk,  # Envía la tarea de procesamiento de chunk al ejecutor
                len(futures),  # Identificador del proceso
                current_skip,  # Número de documentos a saltar
                docs_per_process,  # Número de documentos por proceso
                query,  # Consulta
                sort_by,  # Campo de ordenación
                sort_order,  # Orden de ordenación
                MAX_WORKERS,  # Número máximo de trabajadores
                hint  # Pista de índice
            ))
            remaining -= docs_per_process  # Resta el número de documentos procesados
            current_skip += docs_per_process  # Incrementa el número de documentos a saltar

        all_results = []  # Crea una lista para almacenar todos los resultados
        for future in futures:  # Para cada futuro
            try:
                result = future.result()  # Obtiene el resultado del futuro
                if result:  # Si hay resultados
                    all_results.extend(result)  # Agrega los resultados a la lista
            except Exception as e:  # Si ocurre una excepción
                logging.error(f"Process error: {e}")  # Registra el error

        return all_results[:page_size] if all_results else []  # Devuelve los resultados limitados al tamaño de página