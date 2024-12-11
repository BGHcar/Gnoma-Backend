import time  # Importa el módulo time para medir el tiempo de ejecución
import os  # Importa el módulo os para interactuar con el sistema operativo
from fastapi import FastAPI, HTTPException, APIRouter  # Importa FastAPI y componentes relacionados
from fastapi.middleware.cors import CORSMiddleware  # Importa middleware CORS para FastAPI
import logging  # Importa el módulo logging para registrar eventos
from dotenv import load_dotenv  # Importa load_dotenv para cargar variables de entorno desde un archivo .env
from typing import Any, Dict, List  # Importa tipos de datos para anotaciones de tipo
from concurrent.futures import ThreadPoolExecutor  # Importa ThreadPoolExecutor para ejecutar tareas en paralelo
from motor.motor_asyncio import AsyncIOMotorClient  # Importa AsyncIOMotorClient para interactuar con MongoDB de manera asíncrona

router = APIRouter(prefix="/genome", tags=["Genome Parellel Processing"])  # Crea un enrutador de FastAPI con un prefijo y etiquetas

# Configuración básica
load_dotenv()  # Carga las variables de entorno desde un archivo .env

logging.basicConfig(  # Configura el registro de eventos
    level=logging.INFO,  # Nivel de registro: INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato del mensaje de registro
    handlers=[  # Manejadores de registro
        logging.FileHandler('genome_processing.log'),  # Guarda los registros en un archivo
        logging.StreamHandler()  # Muestra los registros en la consola
    ]
)

# Variables de entorno y configuración
MONGO_URI = os.getenv("MONGO_URI")  # Obtiene la URI de MongoDB desde las variables de entorno
DATABASE_NAME = os.getenv("DATABASE_NAME")  # Obtiene el nombre de la base de datos desde las variables de entorno
NUM_WORKERS = 8  # Número de hilos para el pool de threads
BATCH_SIZE = 5  # Tamaño de los lotes para el procesamiento paralelo

# Cliente MongoDB optimizado
client = AsyncIOMotorClient(  # Crea un cliente de MongoDB asíncrono
    MONGO_URI,  # URI de MongoDB
    maxPoolSize=50,  # Tamaño máximo del pool de conexiones
    minPoolSize=20,  # Tamaño mínimo del pool de conexiones
    maxIdleTimeMS=30000,  # Tiempo máximo de inactividad en milisegundos
    connectTimeoutMS=20000,  # Tiempo máximo de espera para conectar en milisegundos
    retryWrites=True,  # Habilita la reintento de escrituras
    compressors=['zlib']  # Usa compresión zlib
)

class GenomeDocument:  # Clase para representar un documento del genoma
    def __init__(self, doc: Dict[str, Any]):  # Constructor que recibe un diccionario
        self.CHROM = doc.get('CHROM')  # Asigna el valor de 'CHROM'
        self.POS = doc.get('POS')  # Asigna el valor de 'POS'
        self.ID = doc.get('ID')  # Asigna el valor de 'ID'
        self.REF = doc.get('REF')  # Asigna el valor de 'REF'
        self.ALT = doc.get('ALT')  # Asigna el valor de 'ALT'
        self.QUAL = doc.get('QUAL')  # Asigna el valor de 'QUAL'
        self.FILTER = doc.get('FILTER')  # Asigna el valor de 'FILTER'
        self.INFO = doc.get('INFO')  # Asigna el valor de 'INFO'
        self.FORMAT = doc.get('FORMAT')  # Asigna el valor de 'FORMAT'
        self.outputs = {k: v for k, v in doc.items() if k.startswith('output_')}  # Asigna los valores que empiezan con 'output_'

# Cliente MongoDB
client = AsyncIOMotorClient(MONGO_URI)  # Crea un cliente de MongoDB asíncrono
db = client[DATABASE_NAME]  # Obtiene la base de datos
collection = db[DATABASE_NAME]  # Obtiene la colección

# Pool de threads optimizado para 16 hilos
thread_pool = ThreadPoolExecutor(max_workers=NUM_WORKERS)  # Crea un pool de threads con un número máximo de hilos

def process_batch(documents: List[Dict]) -> List[GenomeDocument]:  # Función para procesar un lote de documentos
    """Procesa un batch pequeño de documentos"""
    return [GenomeDocument(doc) for doc in documents]  # Convierte cada documento en una instancia de GenomeDocument

async def get_genome_data(  # Función asíncrona para obtener datos del genoma
    filter: str = "CHROM",  # Filtro por defecto: "CHROM"
    search: str = "",  # Búsqueda por defecto: cadena vacía
    page: int = 1,  # Página por defecto: 1
    page_size: int = 10  # Tamaño de página por defecto: 10
) -> Dict:
    """Función principal con procesamiento altamente paralelo"""
    try:
        start_time = time.time()  # Registra el tiempo de inicio
        
        # Validar página y tamaño de página
        if page_size < 5 or page_size > 100:  # Verifica que el tamaño de página esté entre 5 y 100
            raise HTTPException(  # Lanza una excepción HTTP si el tamaño de página no es válido
                status_code=400,
                detail="page_size must be between 5 and 100"
            )
        
        # Construir query
        query = {filter: {"$regex": search, "$options": "i"}}  # Construye la consulta con una expresión regular
        skip = (page - 1) * page_size  # Calcula el número de documentos a omitir
        
        # Obtener documentos
        cursor = collection.find(query).hint([(filter, 1)]).sort(filter, 1).skip(skip).limit(page_size)  # Ejecuta la consulta en la colección
        documents = []  # Lista para almacenar los documentos
        async for doc in cursor:  # Itera sobre los documentos de manera asíncrona
            documents.append(doc)  # Añade cada documento a la lista

        if not documents:  # Si no hay documentos
            return {  # Retorna un diccionario con los resultados
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "process_time": time.time() - start_time
            }

        # Dividir en batches muy pequeños
        num_docs = len(documents)  # Número de documentos obtenidos
        batch_size = max(1, min(BATCH_SIZE, num_docs // NUM_WORKERS))  # Calcula el tamaño del lote
        batches = [documents[i:i + batch_size] for i in range(0, num_docs, batch_size)]  # Divide los documentos en lotes

        # Procesar todos los batches en paralelo
        futures = []  # Lista para almacenar los futuros
        for batch in batches:  # Itera sobre los lotes
            future = thread_pool.submit(process_batch, batch)  # Envía el lote al pool de threads
            futures.append(future)  # Añade el futuro a la lista

        # Recolectar resultados manteniendo el orden
        processed_docs = []  # Lista para almacenar los documentos procesados
        for future in futures:  # Itera sobre los futuros
            processed_docs.extend(future.result())  # Añade los resultados a la lista

        total_time = time.time() - start_time  # Calcula el tiempo total de procesamiento
        logging.info(f"Query took {total_time:.2f} seconds")  # Registra el tiempo de procesamiento
        
        return {  # Retorna un diccionario con los resultados
            "data": processed_docs,
            "total": len(processed_docs),
            "page": page,
            "page_size": page_size,
            "total_pages": page,  # Simplificado para evitar count innecesario
            "process_time": total_time
        }
        
    except Exception as e:  # Captura cualquier excepción
        logging.error(f"Error in get_genome_data: {str(e)}")  # Registra el error
        raise HTTPException(  # Lanza una excepción HTTP
            status_code=500,
            detail=f"Error processing request: {str(e)}"
        )

@router.on_event("startup")  # Evento de inicio de la aplicación
async def startup_event():
    try:
        await db.command("ping")  # Verifica la conexión a la base de datos
        logging.info(f"Conexión exitosa a la base de datos {DATABASE_NAME}")  # Registra la conexión exitosa
    except Exception as e:  # Captura cualquier excepción
        logging.error(f"Error durante la inicialización: {str(e)}")  # Registra el error
        raise

@router.get("/all")  # Ruta para obtener todos los datos
async def root(
    filter: str = "CHROM",  # Filtro por defecto: "CHROM"
    search: str = "",  # Búsqueda por defecto: cadena vacía
    page: int = 1,  # Página por defecto: 1
    page_size: int = 10  # Tamaño de página por defecto: 10
):
    return await get_genome_data(filter, search, page, page_size)  # Llama a la función get_genome_data y retorna los resultados

@router.get("/search")  # Ruta para buscar variantes
async def search_variants(
    filter: str = "CHROM",  # Filtro por defecto: "CHROM"
    search: str = "",  # Búsqueda por defecto: cadena vacía
    page: int = 1,  # Página por defecto: 1
    page_size: int = 10  # Tamaño de página por defecto: 10
):
    return await get_genome_data(filter, search, page, page_size)  # Llama a la función get_genome_data y retorna los resultados

@router.on_event("shutdown")  # Evento de apagado de la aplicación
async def shutdown_event():
    thread_pool.shutdown(wait=True)  # Apaga el pool de threads
    client.close()  # Cierra el cliente de MongoDB