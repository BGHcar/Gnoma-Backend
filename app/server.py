from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
from dotenv import load_dotenv
from process_file import process_file_parallel
import asyncio
import pymongo
import logging
from bson import ObjectId

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
BASE_URL = os.getenv("BASE_URL")
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db['genomas']

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Incluir los endpoints del genome_api

@app.post("/process_file")
async def process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    try:
        TEMP_DIR = './data'
        os.makedirs(TEMP_DIR, exist_ok=True)

        file_path = os.path.join(TEMP_DIR, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Process file in background
        background_tasks.add_task(process_file_parallel, file_path)

        return {
            "message": f"File '{file.filename}' upload complete. Processing started in background."
        }
    except Exception as e:
        return {"error": f"Error processing file: {str(e)}"}

@app.get("/")
async def root():
    return {"message": "FastAPI server running correctly."}

# Endpoints para obtener variantes de todos los cromosoma y muestras de la base de datos con paginación usando paralelización
@app.get("/genome/all")
async def get_all_variants(page: int = 1, page_size: int = 10):
    start_index = (page - 1) * page_size
    variants = collection.find().skip(start_index).limit(page_size)
    
    # Convertir los documentos para que ObjectId sea serializable
    return [jsonable_encoder_with_objectid(variant) for variant in variants]

'''
Las columnas por las que se debe buscar son:
Chrom
Filter
Info
Format

no tienen que ser valores exactos, pueden ser parciales, por ejemplo, si se busca por Chrom=1, se deben retornar todas las variantes que tengan el valor 1 en la columna Chrom.

en el front se manda esto

{
    "filter": "columnda a filtrar (Chrom, Filter, Info, Format)", 
    "search": "palabra a buscar",
}

en 


'''

@app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    start_index = (page - 1) * page_size
    query = {filter: {"$regex": search, "$options": "i"}}
    variants = collection.find(query).skip(start_index).limit(page_size)
    
    logging.info(f"Searching for {search} in {filter}")
    logging.info(f"Query: {query}")
    
    
    # Convertir los documentos para que ObjectId sea serializable
    return [jsonable_encoder_with_objectid(variant) for variant in variants]



#En la capa de visualización debo poder ordenar los resultados por cada columna.

@app.get("/genome/sort")
async def sort_variants(
    sort_by: str = "CHROM",
    page: int = 1,
    page_size: int = 10
):
    start_index = (page - 1) * page_size
    variants = collection.find().sort(sort_by).skip(start_index).limit(page_size)
    
    # Convertir los documentos para que ObjectId sea serializable
    return [jsonable_encoder_with_objectid(variant) for variant in variants]

# total de endpoints: 5

# localhost:8000/genome/all, con paginación 
# localhost:8000/genome/search, con filtros por Chrom, Filter, Info y Format
# localhost:8000/genome/sort, con ordenamiento por columna
# localhost:8000/process_file
# localhost:8000/ para verificar que el servidor está corriendo correctamente