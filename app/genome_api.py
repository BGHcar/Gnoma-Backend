import time
import os
from fastapi import FastAPI, HTTPException, APIRouter
from fastapi.middleware.cors import CORSMiddleware
import logging
from dotenv import load_dotenv
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
from aiocache import Cache
from aiocache.serializers import PickleSerializer

router = APIRouter(prefix="/genome", tags=["Genome Parellel Processing"])

# Configuración básica
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('genome_processing.log'),
        logging.StreamHandler()
    ]
)

# Variables de entorno y configuración
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
NUM_WORKERS = 8
BATCH_SIZE = 5
CACHE_EXPIRATION = 30 * 60  # 30 minutos en segundos

# Configuración de caché
cache = Cache(
    Cache.MEMORY,
    serializer=PickleSerializer(),
    ttl=CACHE_EXPIRATION,
    namespace="genome_cache"
)

# Cliente MongoDB optimizado
client = AsyncIOMotorClient(
    MONGO_URI,
    maxPoolSize=50,
    minPoolSize=20,
    maxIdleTimeMS=30000,
    connectTimeoutMS=20000,
    retryWrites=True,
    compressors=['zlib']
)

class GenomeDocument:
    def __init__(self, doc: Dict[str, Any]):
        self.CHROM = doc.get('CHROM')
        self.POS = doc.get('POS')
        self.ID = doc.get('ID')
        self.REF = doc.get('REF')
        self.ALT = doc.get('ALT')
        self.QUAL = doc.get('QUAL')
        self.FILTER = doc.get('FILTER')
        self.INFO = doc.get('INFO')
        self.FORMAT = doc.get('FORMAT')
        self.outputs = {k: v for k, v in doc.items() if k.startswith('output_')}

# Cliente MongoDB
client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[DATABASE_NAME]

# Pool de threads optimizado
thread_pool = ThreadPoolExecutor(max_workers=NUM_WORKERS)

def process_batch(documents: List[Dict]) -> List[GenomeDocument]:
    """Procesa un batch pequeño de documentos"""
    return [GenomeDocument(doc) for doc in documents]

def generate_cache_key(filter: str, search: str, page: int, page_size: int) -> str:
    """Genera una clave única para el caché basada en los parámetros de la consulta"""
    return f"genome_data:{filter}:{search}:{page}:{page_size}"

async def get_genome_data(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
) -> Dict:
    """Función principal con procesamiento altamente paralelo y caché"""
    try:
        start_time = time.time()
        
        # Validar página y tamaño de página
        if page_size < 5 or page_size > 100:
            raise HTTPException(
                status_code=400,
                detail="page_size must be between 5 and 100"
            )
        
        # Generar clave de caché
        cache_key = generate_cache_key(filter, search, page, page_size)
        
        # Intentar obtener datos desde el caché
        cached_data = await cache.get(cache_key)
        if cached_data is not None:
            # Actualizar el tiempo de proceso para reflejar el uso de caché
            cache_process_time = time.time() - start_time
            cached_data["process_time"] = round(cache_process_time, 6)  # Mostrar 6 decimales para mayor precisión
            cached_data["cache_hit"] = True
            logging.info(f"Cache hit for key: {cache_key}, process_time: {cache_process_time:.6f} seconds")
            return cached_data
            
        # Si no hay datos en caché, proceder con la consulta a MongoDB
        query = {filter: {"$regex": search, "$options": "i"}}
        skip = (page - 1) * page_size
        
        cursor = collection.find(query).hint([(filter, 1)]).sort(filter, 1).skip(skip).limit(page_size)
        documents = []
        async for doc in cursor:
            documents.append(doc)

        if not documents:
            result = {
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "process_time": time.time() - start_time,
                "cache_hit": False
            }
            # Guardar el resultado vacío en caché
            await cache.set(cache_key, result)
            return result

        # Procesar documentos en paralelo
        num_docs = len(documents)
        batch_size = max(1, min(BATCH_SIZE, num_docs // NUM_WORKERS))
        batches = [documents[i:i + batch_size] for i in range(0, num_docs, batch_size)]

        futures = []
        for batch in batches:
            future = thread_pool.submit(process_batch, batch)
            futures.append(future)

        processed_docs = []
        for future in futures:
            processed_docs.extend(future.result())

        total_time = round(time.time() - start_time, 6)  # Redondear a 6 decimales
        logging.info(f"Query took {total_time:.6f} seconds")
        
        result = {
            "data": processed_docs,
            "total": len(processed_docs),
            "page": page,
            "page_size": page_size,
            "total_pages": page,
            "process_time": total_time,
            "cache_hit": False
        }
        
        # Guardar resultado en caché
        await cache.set(cache_key, result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_genome_data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing request: {str(e)}"
        )

@router.on_event("startup")
async def startup_event():
    try:
        await db.command("ping")
        logging.info(f"Conexión exitosa a la base de datos {DATABASE_NAME}")
    except Exception as e:
        logging.error(f"Error durante la inicialización: {str(e)}")
        raise

@router.get("/all")
async def root(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    return await get_genome_data(filter, search, page, page_size)

@router.get("/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    return await get_genome_data(filter, search, page, page_size)

@router.on_event("shutdown")
async def shutdown_event():
    thread_pool.shutdown(wait=True)
    client.close()
    await cache.close()