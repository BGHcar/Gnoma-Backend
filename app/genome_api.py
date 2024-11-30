import os
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import logging
import app.serverNew as database
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from bson import ObjectId
from typing import Dict, Any, Optional
from functools import partial

# Definir estructura del documento
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
        # Campos output dinámicos
        self.outputs = {k: v for k, v in doc.items() if k.startswith('output_')}

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

# Variables globales (inicialmente vacías)
app = FastAPI()
collection = None
total_documents = 0
indexes = {}
valid_fields = []
numWorkers = int(os.getenv("MAX_WORKERS", os.cpu_count()))
numProcesses = int(os.getenv("NUM_PROCESSES", os.cpu_count()))
chunkSize = int(os.getenv("CHUNK_SIZE", 10000))
numProcessByPool = 0
numDividedByProcesses = 0

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    global collection, total_documents, indexes, numProcessByPool, numDividedByProcesses
    
    try:
        # Inicializar variables de forma asíncrona
        collection = database.collection
        total_documents = await database.getTotalDocuments()
        indexes = await database.getIndexes()
        valid_fields = await database.getValidFields()
        
        # Calcular variables dependientes
        numProcessByPool = min(total_documents / numWorkers, chunkSize)
        numDividedByProcesses = int(numProcessByPool / numProcesses)
        
        logging.info(f"Inicialización completada: {numWorkers} workers, {numProcesses} processes")
        logging.info(f"Total documentos: {total_documents}")
    except Exception as e:
        logging.error(f"Error durante la inicialización: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    await database.close_mongo_connection()

async def conectionStatus():
    logging.info(f"Conexión exitosa, total de documentos en la colección: {total_documents}")

def convert_mongo_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convierte un documento de MongoDB a formato JSON serializable"""
    if doc is None:
        return None
    
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            doc[k] = str(v)
        elif isinstance(v, dict):
            convert_mongo_doc(v)
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    convert_mongo_doc(item)
    return doc

async def queryParallelProcessing(query, page, page_size, sort_by, order, filter_field=None):
    try:
        start_index = (page - 1) * page_size
        chunks = []
        
        for i in range(numProcesses):
            chunk_size = page_size // numProcesses
            skip = start_index + (i * chunk_size)
            limit = chunk_size if i < numProcesses - 1 else page_size - (chunk_size * (numProcesses - 1))
            chunks.append((skip, limit))
        
        tasks = [
            search_chunk(
                query=query,
                skip=skip,
                limit=limit,
                sort_by=sort_by,
                order=order,
                filter_field=filter_field
            )
            for skip, limit in chunks
        ]
        
        results = await asyncio.gather(*tasks)
        variants = []
        for chunk_result in results:
            variants.extend(chunk_result)
            
        if sort_by:
            variants.sort(
                key=lambda x: x.get(sort_by, ''), 
                reverse=(order != "ASC")
            )
            
        return variants
        
    except Exception as e:
        logging.error(f"Error en procesamiento paralelo: {str(e)}")
        return []

async def search_chunk(query, skip, limit, sort_by, order, filter_field=None):
    try:
        # Proyección completa
        projection = {
            "CHROM": 1,
            "POS": 1,
            "ID": 1,
            "REF": 1,
            "ALT": 1,
            "QUAL": 1,
            "FILTER": 1,
            "INFO": 1,
            "FORMAT": 1
        }
        
        # Agregar campos output dinámicamente
        for i in range(1, 21):
            projection[f"output_CH{i}"] = 1
            
        # Crear cursor con hint correcto
        cursor = database.collection.find(
            query,
            projection
        )
        
        # Aplicar hint solo si hay un campo de filtro
        if filter_field:
            cursor = cursor.hint([(filter_field, 1)])
        
        if sort_by:
            cursor = cursor.sort([(sort_by, 1 if order == "ASC" else -1)])
            
        cursor = cursor.skip(skip).limit(limit)
        chunk_results = await cursor.to_list(length=limit)
        return [convert_mongo_doc(doc) for doc in chunk_results]
        
    except Exception as e:
        logging.error(f"Error en chunk: {str(e)}")
        return []

@app.get("/genome/all")
async def start_page(page: int = 1, page_size: int = 10, sort_by: str = None, order: str = "ASC", search: str = "", filter: str = ""):
    start_time = time.time()
    
    # Mejorar la construcción del query
    if filter and search:
        query = {filter: {"$regex": search, "$options": "i"}}
    else:
        query = {}
        
    data = await queryParallelProcessing(
        query,
        page,
        page_size,
        sort_by,
        order
    )
    
    return {
        "data": data,
        "process_time": round(time.time() - start_time, 3),
        "total_items": len(data)
    }

@app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10,
    sort_by: str = None,
    order: str = "ASC"
):
    start_time = time.time()
    logging.info(f"FILTER: {filter}, SEARCH: {search}")
    
    try:
        # Construir query para búsqueda parcial
        query = {filter: {"$regex": f".*{search}.*", "$options": "i"}} if search else {}
        
        # Usar procesamiento paralelo con hint
        variants = await queryParallelProcessing(
            query=query,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            order=order,
            filter_field=filter
        )
        
        # Obtener total de coincidencias
        total = await database.collection.count_documents(query)
        
        return {
            "data": variants,
            "total": total,
            "page": page,
            "page_size": page_size,
            "process_time": round(time.time() - start_time, 3)
        }
        
    except Exception as e:
        logging.error(f"Error en búsqueda: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al buscar variantes: {str(e)}"
        )
