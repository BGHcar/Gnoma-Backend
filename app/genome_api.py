import time
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
from dotenv import load_dotenv
from typing import Any, Dict, List
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient

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
NUM_WORKERS = 16  # Usar todos los hilos disponibles
BATCH_SIZE = 5  # Bloques más pequeños para mejor distribución

# Cliente MongoDB
client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[DATABASE_NAME]

# Pool de threads optimizado para 16 hilos
thread_pool = ThreadPoolExecutor(max_workers=NUM_WORKERS)

def process_batch(documents: List[Dict]) -> List[GenomeDocument]:
    """Procesa un batch pequeño de documentos"""
    return [GenomeDocument(doc) for doc in documents]

async def get_genome_data(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
) -> Dict:
    """Función principal con procesamiento altamente paralelo"""
    try:
        start_time = time.time()
        
        # Validar página y tamaño de página
        if page_size < 5 or page_size > 100:
            raise HTTPException(
                status_code=400,
                detail="page_size must be between 5 and 100"
            )
        
        # Construir query
        query = {filter: {"$regex": search, "$options": "i"}}
        skip = (page - 1) * page_size
        
        # Obtener documentos
        cursor = collection.find(query).hint([(filter, 1)]).sort(filter, 1).skip(skip).limit(page_size)
        documents = []
        async for doc in cursor:
            documents.append(doc)

        if not documents:
            return {
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
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

        total_time = time.time() - start_time
        logging.info(f"Query took {total_time:.2f} seconds")
        
        return {
            "data": processed_docs,
            "total": len(processed_docs),
            "page": page,
            "page_size": page_size,
            "total_pages": page,  # Simplificado para evitar count innecesario
            "process_time": total_time
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
        await db.command("ping")
        logging.info(f"Conexión exitosa a la base de datos {DATABASE_NAME}")
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
    thread_pool.shutdown(wait=True)
    client.close()