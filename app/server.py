# server.py
import asyncio
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
from process_file import process_file_parallel
import pymongo
import logging
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor
import tempfile
import asyncio
from datetime import datetime
import time

# Configure logging to use a separate logs directory
LOG_DIR = './logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'server.log')),
        logging.StreamHandler()
    ]
)

# Función para convertir ObjectId en string
def jsonable_encoder_with_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)  # Convertir ObjectId a string
    elif isinstance(obj, dict):
        return {key: jsonable_encoder_with_objectid(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [jsonable_encoder_with_objectid(item) for item in obj]
    else:
        return obj

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", os.cpu_count()))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))
MAX_WORKERS = os.cpu_count() * 5

client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db['genomas']

#Si la base de datos existe, obtener un documento de la colección
if collection.name in client.list_database_names():
    # Obtener el primer documento
    primer_documento = collection.find_one()

    # Obtener el último documento
    ultimo_documento = collection.find_one(sort=[('_id', -1)])

    # Combinar ambos documentos sin duplicar datos
    sample_document = {**primer_documento, **ultimo_documento}
    
    # Extraer las claves (nombres de los campos)
    valid_fields = sample_document.keys()

logging.info(f"Connected to MongoDB: with num_processes={NUM_PROCESSES}, chunk_size={CHUNK_SIZE}, workers={MAX_WORKERS}")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Parallel file processing using thread pool
def parallel_process_file(file_path):
    process_file_parallel(file_path)

@app.post("/process_file")
async def process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    start_time = time.time()
    try:
        # Crear archivo temporal con un nombre más descriptivo
        with tempfile.NamedTemporaryFile(
            delete=False, 
            prefix=f"vcf_{datetime.now().strftime('%Y%m%d_%H%M%S')}_", 
            suffix=f"_{file.filename}"
        ) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
            
            # Validación básica de archivo
            if not content:
                logging.warning("Archivo vacío recibido")
                return {
                    "error": "El archivo está vacío",
                    "process_time": round(time.time() - start_time, 3)
                }
            
            if not file.filename.lower().endswith('.vcf'):
                logging.warning(f"Archivo inválido: {file.filename}")
                return {
                    "error": "Por favor, suba un archivo VCF",
                    "process_time": round(time.time() - start_time, 3)
                }
        
        # Programar limpieza de archivos temporales
        def cleanup_temp_file():
            try:
                os.unlink(temp_file_path)
                logging.info(f"Archivo temporal {temp_file_path} eliminado")
            except Exception as e:
                logging.error(f"Error eliminando archivo temporal: {e}")
        
        background_tasks.add_task(parallel_process_file, temp_file_path)
        background_tasks.add_task(cleanup_temp_file)

        return {
            "message": f"Archivo '{file.filename}' cargado. Procesamiento iniciado.",
            "temp_file": temp_file_path,
            "process_time": round(time.time() - start_time, 3)
        }
    except Exception as e:
        logging.error(f"Error procesando archivo: {str(e)}")
        return {
            "error": f"Error procesando archivo: {str(e)}",
            "process_time": round(time.time() - start_time, 3)
        }
    
@app.get("/")
async def root():
    start_time = time.time()
    return {
        "message": "FastAPI server running correctly.",
        "process_time": round(time.time() - start_time, 3)
    }

async def get_variants_parallel(query=None, sort_by=None, sort_order=None, page=1, page_size=10):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        def fetch_variants():
            start_index = (page - 1) * page_size
            find_query = query or {}
            
            cursor = collection.find(find_query)
            hint_used = None
            
            # Aplicar hints basados en el tipo de consulta y ordenamiento
            if sort_by and sort_order is not None:
                if query:  # Si hay query junto con sort
                    search_field = next(iter(query))
                    hint_used = [(search_field, 1)]
                    cursor = cursor.hint(hint_used)
                    logging.info(f"Using hint for combined search and sort: {hint_used}")
                else:  # Solo sort sin query
                    if sort_by in ["CHROM", "POS", "ID", "REF", "ALT", "FILTER", "INFO", "FORMAT", "output"]:
                        hint_used = [(sort_by, 1)]
                        cursor = cursor.hint(hint_used)
                        logging.info(f"Using hint for sort: {hint_used}")
                
                cursor = cursor.sort(sort_by, sort_order)
            
            # Si solo hay búsqueda sin ordenamiento
            elif query:
                search_field = next(iter(query))
                if search_field in ["CHROM", "POS", "ID", "REF", "ALT", "FILTER", "INFO", "FORMAT", "output"]:
                    hint_used = [(search_field, 1)]
                    cursor = cursor.hint(hint_used)
                    logging.info(f"Using hint for search: {hint_used}")
            
            # Si no se usó ningún hint
            if hint_used is None:
                logging.info("No hint used for this query")
            
            # Ejecutar query
            variants = list(cursor.skip(start_index).limit(page_size))
            return [jsonable_encoder_with_objectid(variant) for variant in variants]
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, fetch_variants)

@app.get("/genome/all")
async def get_all_variants(page: int = 1, page_size: int = 10):
    start_time = time.time()
    variants = await get_variants_parallel(page=page, page_size=page_size)
    return {
        "data": variants,
        "process_time": round(time.time() - start_time, 3)
    }

@app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10,
    sort_by: str = None,
    order: str = None
):
    start_time = time.time()
    query = {filter: {"$regex": search, "$options": "i"}}
    
    # Si se especifica ordenamiento junto con la búsqueda
    sort_order = None
    if sort_by and order:
        sort_order = 1 if order.upper() == "ASC" else -1
        
    variants = await get_variants_parallel(
        query=query,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size
    )
    
    return {
        "data": variants,
        "process_time": round(time.time() - start_time, 3)
    }

@app.get("/genome/sort")
async def sort_variants(
    sort_by: str = "CHROM", 
    order: str = "ASC",
    page: int = 1,
    page_size: int = 10,
    filter: str = None,  # Campo de búsqueda
    search: str = None  # Término de búsqueda
):
    start_time = time.time()
    
    # Validate order parameter
    if order not in ["ASC", "DESC"]:
        return {
            "error": "Invalid order value. Use 'ASC' or 'DESC'.",
            "process_time": round(time.time() - start_time, 3)
        }
    
    # Validate sort_by parameter
    if sort_by not in valid_fields:
        return {
            "error": f"Invalid sort_by value. Valid fields are: {', '.join(valid_fields)}",
            "process_time": round(time.time() - start_time, 3)
        }

    # Construir query si hay parámetros de búsqueda
    query = None
    if filter and search:
        query = {filter: {"$regex": search, "$options": "i"}}
        logging.info(f"Sort combined with search query: {query}")

    # Determine sort order
    sort_order = 1 if order == "ASC" else -1

    # Obtener documentos filtrados
    results = await get_variants_parallel(
        query=query,
        sort_by=sort_by, 
        sort_order=sort_order, 
        page=page, 
        page_size=page_size
    )
    
    return {
        "data": results,
        "process_time": round(time.time() - start_time, 3)
    }