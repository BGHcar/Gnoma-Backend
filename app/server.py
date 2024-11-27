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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 16))

client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db['genomas']

# Obtener un documento de la colección
sample_document = collection.find_one()

# Extraer las claves (nombres de los campos)
valid_fields = sample_document.keys()

# Mostrar los nombres de los campos

logging.info(f"Connected to MongoDB: {client.server_info()} with num_processes={NUM_PROCESSES}, chunk_size={CHUNK_SIZE}, workers={MAX_WORKERS}")

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
                return {"error": "El archivo está vacío"}
            
            if not file.filename.lower().endswith('.vcf'):
                logging.warning(f"Archivo inválido: {file.filename}")
                return {"error": "Por favor, suba un archivo VCF"}
        
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
            "temp_file": temp_file_path
        }
    except Exception as e:
        logging.error(f"Error procesando archivo: {str(e)}")
        return {"error": f"Error procesando archivo: {str(e)}"}
    
@app.get("/")
async def root():
    return {"message": "FastAPI server running correctly."}

# Parallel variant retrieval
async def get_variants_parallel(query=None, sort_by=None, sort_order=None, page=1, page_size=10):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Parallel database queries
        def fetch_variants():
            start_index = (page - 1) * page_size
            
            # Construct query with optional filtering
            find_query = query or {}
            
            # Construct sorting with safe default
            cursor = collection.find(find_query)
            
            # Apply sorting only if sort_by is provided
            if sort_by and sort_order is not None:
                cursor = cursor.sort(sort_by, sort_order)
            
            # Execute query
            variants = list(cursor.skip(start_index).limit(page_size))
            return [jsonable_encoder_with_objectid(variant) for variant in variants]
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, fetch_variants)

@app.get("/genome/all")
async def get_all_variants(page: int = 1, page_size: int = 10):
    return await get_variants_parallel(page=page, page_size=page_size)

@app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    query = {filter: {"$regex": search, "$options": "i"}}
    return await get_variants_parallel(query=query, page=page, page_size=page_size)

@app.get("/genome/sort")
async def sort_variants(
    sort_by: str = "CHROM", 
    order: str = "ASC",
    page: int = 1,
    page_size: int = 10,
    include_outputs: bool = False  # Nuevo parámetro para incluir o excluir outputs
):
    # Validate order parameter
    if order not in ["ASC", "DESC"]:
        raise HTTPException(status_code=400, detail="Invalid order value. Use 'ASC' or 'DESC'.")
    
    # Validate sort_by parameter
    if sort_by not in valid_fields:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by value. Valid fields are: {', '.join(valid_fields)}")

    # Determine sort order
    sort_order = 1 if order == "ASC" else -1

    # Obtener documentos filtrados
    results = await get_variants_parallel(
        sort_by=sort_by, 
        sort_order=sort_order, 
        page=page, 
        page_size=page_size
    )
    
    # Filtrar outputs dinámicos si es necesario
    filtered_results = []
    for doc in results:
        if not include_outputs:
            # Excluir campos dinámicos (outputs)
            doc = {k: v for k, v in doc.items() if not k.startswith("output_")}
        filtered_results.append(doc)

    return filtered_results
