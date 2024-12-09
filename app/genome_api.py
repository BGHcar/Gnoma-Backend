import time
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
from dotenv import load_dotenv
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

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

app = FastAPI()

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables de entorno y configuración
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
NUM_WORKERS = int(os.getenv("MAX_WORKERS")) # Usar todos los hilos disponibles
BATCH_SIZE = 5  # Bloques más pequeños para mejor distribución

# Cliente MongoDB
client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[DATABASE_NAME]

# Pool de threads global
thread_pool = None

async def process_documents_chunk(chunk: List[Dict]) -> List[GenomeDocument]:
    """Procesa un chunk de documentos en un thread separado"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        thread_pool,
        lambda: [GenomeDocument(doc) for doc in chunk]
    )

async def get_genome_data(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
) -> Dict:
    try:
        start_time = time.time()
        query = {filter: {"$regex": search, "$options": "i"}} if search else {}
        
        # Optimizar la query usando hint y sort
        cursor = collection.find(query).hint([(filter, 1)]).sort(filter, 1)
        
        # Aplicar paginación
        cursor = cursor.skip((page - 1) * page_size).limit(page_size)
        documents = await cursor.to_list(length=page_size)
        
        if not documents:
            return {
                "data": [],
                "total": 0,
                "process_time": time.time() - start_time
            }

        # Dividir en batches muy pequeños
        num_docs = len(documents)
        batch_size = max(1, min(BATCH_SIZE, num_docs // NUM_WORKERS))
        batches = [documents[i:i + batch_size] for i in range(0, num_docs, batch_size)]

        # Procesar todos los batches en paralelo
        futures = []
        for batch in batches:
            future = thread_pool.submit(process_batch, batch)
            futures.append(future)

        # Recolectar resultados manteniendo el orden
        processed_docs = []
        for future in futures:
            processed_docs.extend(future.result())

        total_time = round(time.time() - start_time, 5)# Redondear a 5 decimales
        logging.info(f"Query took {total_time:.2f} seconds")
        
        return {
            "data": processed_docs,
            "total": total_docs,
            "process_time": process_time
        }
    except Exception as e:
        logging.error(f"Error in get_genome_data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing request: {str(e)}"
        )

@app.on_event("startup")
async def startup_event():
    try:
        global thread_pool
        thread_pool = ThreadPoolExecutor(max_workers=NUM_WORKERS)
        
        # Verificar conexión a MongoDB
        await db.command("ping")
        logging.info(f"Conexión exitosa a la base de datos {DATABASE_NAME}")
        
        # Asegurar que tenemos el índice necesario
        await db[DATABASE_NAME].create_index([("CHROM", 1)])
        logging.info("Índices verificados")
        
    except Exception as e:
        logging.error(f"Error durante la inicialización: {str(e)}")
        raise

@app.get("/genome/all")
async def root(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    return await get_genome_data(filter, search, page, page_size)

@app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    return await get_genome_data(filter, search, page, page_size)

@app.on_event("shutdown")
async def shutdown_event():
    if thread_pool:
        thread_pool.shutdown(wait=True)
    client.close()