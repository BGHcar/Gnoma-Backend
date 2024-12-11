import time  # Importa el módulo time para medir el tiempo de ejecución
import os  # Importa el módulo os para interactuar con el sistema operativo
from fastapi import FastAPI, HTTPException  # Importa FastAPI y HTTPException de FastAPI
from fastapi.middleware.cors import CORSMiddleware  # Importa CORSMiddleware para configurar CORS
import logging  # Importa el módulo logging para registrar eventos
from dotenv import load_dotenv  # Importa load_dotenv para cargar variables de entorno desde un archivo .env
import pymongo  # Importa pymongo para interactuar con MongoDB
from typing import Any, Dict, Tuple  # Importa tipos de datos de typing

# Definir estructura del documento
class GenomeDocument:
    def __init__(self, doc: Dict[str, Any]):
        self.CHROM = doc.get('CHROM')  # Asigna el valor de 'CHROM' del documento
        self.POS = doc.get('POS')  # Asigna el valor de 'POS' del documento
        self.ID = doc.get('ID')  # Asigna el valor de 'ID' del documento
        self.REF = doc.get('REF')  # Asigna el valor de 'REF' del documento
        self.ALT = doc.get('ALT')  # Asigna el valor de 'ALT' del documento
        self.QUAL = doc.get('QUAL')  # Asigna el valor de 'QUAL' del documento
        self.FILTER = doc.get('FILTER')  # Asigna el valor de 'FILTER' del documento
        self.INFO = doc.get('INFO')  # Asigna el valor de 'INFO' del documento
        self.FORMAT = doc.get('FORMAT')  # Asigna el valor de 'FORMAT' del documento
        # Campos output dinámicos
        self.outputs = {k: v for k, v in doc.items() if k.startswith('output_')}  # Asigna campos dinámicos que empiezan con 'output_'

# Configuración básica

load_dotenv()  # Carga las variables de entorno desde un archivo .env

logging.basicConfig(  # Configura el registro de eventos
    level=logging.INFO,  # Nivel de registro: INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato del mensaje de registro
    handlers=[
        logging.FileHandler('genome_processing.log'),  # Guarda los registros en un archivo
        logging.StreamHandler()  # Muestra los registros en la consola
    ]
)

# Variables globales (inicialmente vacías)
app = FastAPI()  # Crea una instancia de FastAPI

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas las orígenes
    allow_credentials=True,  # Permite credenciales
    allow_methods=["*"],  # Permite todos los métodos
    allow_headers=["*"],  # Permite todos los encabezados
)

MONGO_URI = os.getenv("MONGO_URI")  # Obtiene la URI de MongoDB desde las variables de entorno
DATABASE_NAME = os.getenv("DATABASE_NAME")  # Obtiene el nombre de la base de datos desde las variables de entorno

client = pymongo.MongoClient(MONGO_URI)  # Crea un cliente de MongoDB
db = client[DATABASE_NAME]  # Selecciona la base de datos
collection = db[DATABASE_NAME]  # Selecciona la colección

# Si la base de datos existe, obtener un documento de la colección
@ app.on_event("startup")
async def startup_event():
    try:
        # Inicializar variables de forma asíncrona
        collection = db[DATABASE_NAME]  # Selecciona la colección
        logging.info(f"Conexión exitosa a la base de datos {DATABASE_NAME}")  # Registra un mensaje de éxito
    except Exception as e:
        logging.error(f"Error durante la inicialización: {str(e)}")  # Registra un mensaje de error
        raise
    
@ app.get("/genome/all")
async def root(
    filter: str = "CHROM",  # Filtro por defecto: 'CHROM'
    search: str = "",  # Cadena de búsqueda por defecto: vacía
    page: int = 1,  # Página por defecto: 1
    page_size: int = 10  # Tamaño de página por defecto: 10
):
    start_time = time.time()  # Registra el tiempo de inicio
    query = {filter: {"$regex": search, "$options": "i"}}  # Crea una consulta con expresión regular
    
    sort_order = None  # Orden de clasificación (no utilizado)
    
    cursor = collection.find(query)  # Ejecuta la consulta en la colección
    logging.info(f"Estos son los indices: {collection.index_information()}")  # Registra la información de los índices
    logging.info(f"Filter in hint: {filter}")  # Registra el filtro utilizado
    #cursor = cursor.hint([(filter, 1)]).sort(filter, 1).skip((page - 1) * page_size).limit(page_size)
    # usando una busqueda sin indices
    cursor = cursor.sort(filter, 1).skip((page - 1) *page_size).limit(page_size)  # Ordena, salta y limita los resultados
    return_data = []
    for doc in cursor:
        return_data.append(GenomeDocument(doc))  # Añade cada documento al resultado
        
    total_time = time.time() - start_time  # Calcula el tiempo total de ejecución
    logging.info(f"Query took {total_time:.2f} seconds")  # Registra el tiempo de ejecución
    
    return {
        "data": return_data,  # Datos devueltos
        "total": len(return_data),  # Total de documentos devueltos
        "process_time": total_time  # Tiempo de procesamiento
    }
    
@ app.get("/genome/search")
async def search_variants(
    filter: str = "CHROM",  # Filtro por defecto: 'CHROM'
    search: str = "",  # Cadena de búsqueda por defecto: vacía
    page: int = 1,  # Página por defecto: 1
    page_size: int = 10  # Tamaño de página por defecto: 10
):
    return await get_genome_data(filter, search, page, page_size)  # Llama a la función get_genome_data

async def get_genome_data(
    filter: str = "CHROM",  # Filtro por defecto: 'CHROM'
    search: str = "",  # Cadena de búsqueda por defecto: vacía
    page: int = 1,  # Página por defecto: 1
    page_size: int = 10  # Tamaño de página por defecto: 10
):
    try:
        start_time = time.time()  # Registra el tiempo de inicio
        query = {filter: {"$regex": search, "$options": "i"}}  # Crea una consulta con expresión regular
        
        cursor = collection.find(query)  # Ejecuta la consulta en la colección
        
        # Especificamos el índice usando el formato correcto de MongoDB
        cursor = cursor.hint([(filter, 1)]).sort(filter, 1).skip((page - 1) * page_size).limit(page_size)  # Usa un índice, ordena, salta y limita los resultados
        
        return_data = []
        for doc in cursor:
            return_data.append(GenomeDocument(doc))  # Añade cada documento al resultado
            
        total_time = time.time() - start_time  # Calcula el tiempo total de ejecución
        logging.info(f"Query took {total_time:.2f} seconds")  # Registra el tiempo de ejecución
        
        return {
            "data": return_data,  # Datos devueltos
            "total": len(return_data),  # Total de documentos devueltos
            "process_time": total_time  # Tiempo de procesamiento
        }
        
    except Exception as e:
        logging.error(f"Error in get_genome_data: {str(e)}")  # Registra un mensaje de error
        raise HTTPException(
            status_code=500,  # Código de estado HTTP: 500
            detail=f"Error processing request: {str(e)}"  # Detalle del error
        )