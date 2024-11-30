import time
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
from dotenv import load_dotenv
import pymongo
from typing import Any, Dict, Tuple


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

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[DATABASE_NAME]

# Si la base de datos existe, obtener un documento de la colección
@ app.on_event("startup")
async def startup_event():
    try:
        # Inicializar variables de forma asíncrona
        collection = db[DATABASE_NAME]
        logging.info(f"Conexión exitosa a la base de datos {DATABASE_NAME}")
    except Exception as e:
        logging.error(f"Error durante la inicialización: {str(e)}")
        raise
    
@ app.get("/genome/all")
async def root(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    start_time = time.time()
    query = {filter: {"$regex": search, "$options": "i"}}
    
    sort_order = None
    
    cursor = collection.find(query)
    logging.info(f"Estos son los indices: {collection.index_information()}")
    logging.info(f"Filter in hint: {filter}")
    #cursor = cursor.hint([(filter, 1)]).sort(filter, 1).skip((page - 1) * page_size).limit(page_size)
    # usando una busqueda sin indices
    cursor = cursor.sort(filter, 1).skip((page - 1) *page_size).limit(page_size)   
    return_data = []
    for doc in cursor:
        return_data.append(GenomeDocument(doc))
        
    total_time = time.time() - start_time
    logging.info(f"Query took {total_time:.2f} seconds")
    
    return {
        "data": return_data,
        "total": len(return_data),
        "process_time": total_time
    }
    
@ app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    return await get_genome_data(filter, search, page, page_size)

async def get_genome_data(
    filter: str = "CHROM",
    search: str = "",
    page: int = 1,
    page_size: int = 10
):
    try:
        start_time = time.time()
        query = {filter: {"$regex": search, "$options": "i"}}
        
        cursor = collection.find(query)
        
        # Especificamos el índice usando el formato correcto de MongoDB
        cursor = cursor.hint([(filter, 1)]).sort(filter, 1).skip((page - 1) * page_size).limit(page_size)
        
        return_data = []
        for doc in cursor:
            return_data.append(GenomeDocument(doc))
            
        total_time = time.time() - start_time
        logging.info(f"Query took {total_time:.2f} seconds")
        
        return {
            "data": return_data,
            "total": len(return_data),
            "process_time": total_time
        }
        
    except Exception as e:
        logging.error(f"Error in get_genome_data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing request: {str(e)}"
        )