import asyncio
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, APIRouter
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
from app.process_file import process_file_parallel
import pymongo
import logging
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import tempfile
from datetime import datetime
import time

# Asegurar que existe el directorio de logs
os.makedirs('logs', exist_ok=True)

router = APIRouter(prefix="",tags=["server"])

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/server.log'),
        logging.StreamHandler()
    ]
)

def json_compatible(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: json_compatible(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [json_compatible(item) for item in obj]
    else:
        return obj

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", os.cpu_count()))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", os.cpu_count()))

client = pymongo.MongoClient(
    MONGO_URI,
    maxPoolSize=MAX_WORKERS,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
    w=1,
    journal=False,
    compressors='snappy'
)

db = client[DATABASE_NAME]
collection = db['genomas']

if collection.name in db.list_collection_names():
    primer_documento = collection.find_one()
    ultimo_documento = collection.find_one(sort=[('_id', -1)])
    sample_document = {**primer_documento, **ultimo_documento} if primer_documento and ultimo_documento else {}
    valid_fields = sample_document.keys()

logging.info(f"Connected to MongoDB: processes={NUM_PROCESSES}, chunk={CHUNK_SIZE}, workers={MAX_WORKERS}")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
    expose_headers=["*"]
)

def process_chunk(process_id, skip, limit, query, sort_by, sort_order, max_workers, hint=None):
    with ThreadPoolExecutor(max_workers=max_workers) as thread_executor:
        def thread_query(thread_id):
            try:
                base_query = query if query else {}
                cursor = collection.find(base_query)
                
                if sort_by:
                    cursor = cursor.sort(sort_by, sort_order)
                
                cursor = cursor.skip(skip).limit(limit)
                
                if hint:
                    logging.info(f"P{process_id}-T{thread_id}: Using hint {hint}")
                    cursor = cursor.hint(hint)
                
                results = [
                    {**{k: str(v) if isinstance(v, ObjectId) else v for k, v in doc.items()}}
                    for doc in cursor
                ]
                
                logging.info(f"P{process_id}-T{thread_id}: Found {len(results)} docs")
                return results
            except Exception as e:
                logging.error(f"P{process_id}-T{thread_id}: Error - {str(e)}")
                return []

        futures = []
        threads_per_process = max_workers
        for t in range(threads_per_process):
            futures.append(thread_executor.submit(thread_query, t))
        
        results = []
        for f in futures:
            try:
                result = f.result()
                if result:
                    results.extend(result)
            except Exception as e:
                logging.error(f"Thread error: {e}")
        
        return results

def parallel_process_file(file_path):
    process_file_parallel(file_path)

@router.post("/process_file")
async def process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """Procesa un archivo VCF en segundo plano"""
    start_time = time.time()
    try:
        with tempfile.NamedTemporaryFile(
            delete=False, 
            prefix=f"vcf_{datetime.now().strftime('%Y%m%d_%H%M%S')}_", 
            suffix=f"_{file.filename}"
        ) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
            
            if not content:
                logging.warning("Empty file received")
                return {
                    "error": "The file is empty",
                    "process_time": round(time.time() - start_time, 3)
                }
            
            if not file.filename.lower().endswith('.vcf'):
                logging.warning(f"Invalid file: {file.filename}")
                return {
                    "error": "Please upload a VCF file",
                    "process_time": round(time.time() - start_time, 3)
                }
        
        def cleanup_temp_file():
            try:
                os.unlink(temp_file_path)
                logging.info(f"Temporary file {temp_file_path} deleted")
            except Exception as e:
                logging.error(f"Error deleting temp file: {e}")
        
        background_tasks.add_task(parallel_process_file, temp_file_path)
        background_tasks.add_task(cleanup_temp_file)

        return {
            "message": f"File '{file.filename}' uploaded. Processing started.",
            "temp_file": temp_file_path,
            "process_time": round(time.time() - start_time, 3)
        }
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        return {
            "error": f"Error processing file: {str(e)}",
            "process_time": round(time.time() - start_time, 3)
        }


async def get_variants_parallel(query=None, sort_by="_id", sort_order=1, page=1, page_size=10, hint=None):
    docs_per_process = max(1, page_size // NUM_PROCESSES)
    remaining = page_size
    current_skip = (page - 1) * page_size
    
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as process_executor:
        futures = []
        while remaining > 0:
            futures.append(process_executor.submit(
                process_chunk,
                len(futures),
                current_skip,
                docs_per_process,
                query,
                sort_by,
                sort_order,
                MAX_WORKERS,
                hint
            ))
            remaining -= docs_per_process
            current_skip += docs_per_process

        all_results = []
        for future in futures:
            try:
                result = future.result()
                if result:
                    all_results.extend(result)
            except Exception as e:
                logging.error(f"Process error: {e}")

        return all_results[:page_size] if all_results else []

@app.get("/genome/all")
async def get_all_variants(page: int = 1, page_size: int = 10):
    filter = "CHROM"
    search = ""
    start_time = time.time()
    variants = await get_variants_parallel(
        query={filter: {"$regex": search, "$options": "i"}},
        sort_by=None,
        sort_order=1,
        page=page,
        page_size=page_size,
        hint=[("CHROM", 1)]
    )
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
    
    sort_order = 1 if order and order.upper() == "ASC" else -1
    hint = None
    
    if sort_by:
        if sort_by.startswith("output_"):
            hint = [("output", 1)]
        else:
            hint = [(sort_by, 1)]
    else:
        if filter in valid_fields:
            hint = [(filter, 1)]
        else:
            hint = [("CHROM", 1)]
    
    logging.info(f"Query: {query}, Sort by: {sort_by}, Sort order: {sort_order}, Hint: {hint}")
    
    results = await get_variants_parallel(
        query=query,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size,
        hint=hint
    )
    
    return {
        "data": results,
        "process_time": round(time.time() - start_time, 3)
    }

@app.get("/genome/sort")
async def sort_variants(
    sort_by: str = "CHROM", 
    order: str = "ASC",
    page: int = 1,
    page_size: int = 10,
    filter: str = None,
    search: str = None
):
    logging.info(f"Search filters: {filter} - {search}")
    start_time = time.time()
    
    if order not in ["ASC", "DESC"]:
        return {
            "error": "Invalid order value. Use 'ASC' or 'DESC'.",
            "process_time": round(time.time() - start_time, 3)
        }
    
    if sort_by not in valid_fields:
        return {
            "error": f"Invalid sort_by value. Valid fields are: {', '.join(valid_fields)}",
            "process_time": round(time.time() - start_time, 3)
        }
    sort_order = 1 if order == "ASC" else -1

    query = None
    if filter and search:
        query = {filter: {"$regex": search, "$options": "i"}}
        logging.info(f"Sort combined with search query: {query}")
        hint = [(filter, sort_order)]
    else:
        if sort_by.startswith("output_"):
            hint = [("output", sort_order)]
        else:
            hint = [(sort_by, sort_order)]

    results = await get_variants_parallel(
        query=query,
        sort_by=sort_by, 
        sort_order=sort_order,
        page=page,
        page_size=page_size,
        hint=hint
    )
    
    return {
        "data": results,
        "process_time": round(time.time() - start_time, 3)
    }