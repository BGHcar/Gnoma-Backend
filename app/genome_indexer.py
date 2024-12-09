from concurrent.futures import ThreadPoolExecutor
import pymongo
from pymongo import InsertOne
from dotenv import load_dotenv
import os
import time
import logging
from typing import List, Dict, Tuple

load_dotenv()

# Asegurar que existe el directorio de logs
os.makedirs('logs', exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/indexer.log'),
        logging.StreamHandler()
    ]
)

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
BATCH_SIZE = 1000

client = pymongo.MongoClient(
    MONGO_URI,
    maxPoolSize=None,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
    w=1,  # Write concern reducido
    journal=False,
    compressors='snappy'
)
db = client[DATABASE_NAME]
collection = db['genomas']

def get_header_info(file_path: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Extrae la información del encabezado del archivo VCF.
    Retorna los nombres de las columnas y sus posiciones.
    """
    with open(file_path, 'r') as f:
        for line in f:
            if line.startswith('#CHROM'):
                headers = line.strip().split('\t')
                sample_columns = headers[9:]
                column_positions = {
                    name: idx + 9 
                    for idx, name in enumerate(sample_columns)
                }
                return sample_columns, column_positions
    raise ValueError("No se encontró la línea de encabezado en el archivo VCF")

def process_line(line: str, column_positions: Dict[str, int]) -> Dict:
    """Procesa una línea del archivo VCF y retorna un documento."""
    if line.startswith('#'):
        return None
        
    fields = line.strip().split('\t')
    if len(fields) < 8:
        return None
    
    try:
        document = {
            "CHROM": fields[0],
            "POS": fields[1],
            "ID": fields[2] if fields[2] != '.' else None,
            "REF": fields[3],
            "ALT": fields[4],
            "QUAL": fields[5],
            "FILTER": fields[6],
            "INFO": fields[7],
            "FORMAT": fields[8]
        }
        
        for sample_name, position in column_positions.items():
            if position < len(fields):
                document[sample_name] = fields[position]
        
        return document
        
    except Exception as e:
        logging.error(f"Error procesando línea: {e}")
        return None

def process_file_chunk(file_path: str, start_pos: int, chunk_size: int, column_positions: Dict[str, int]) -> List[Dict]:
    """Procesa un chunk del archivo."""
    documents = []
    
    with open(file_path, 'r') as f:
        f.seek(start_pos)
        lines_processed = 0
        
        while lines_processed < chunk_size:
            line = f.readline()
            if not line:
                break
                
            document = process_line(line, column_positions)
            if document:
                documents.append(document)
            
            lines_processed += 1
    
    # Insertar documentos en batch
    if documents:
        try:
            result = collection.insert_many(documents, ordered=False)
            logging.info(f"Insertados {len(result.inserted_ids)} documentos")
        except Exception as e:
            logging.error(f"Error durante la inserción: {e}")
    
    return documents

def create_indices():
    """Crea índices para mejor rendimiento en consultas."""
    try:
        collection.create_index([("CHROM", pymongo.ASCENDING)])
        collection.create_index([("POS", pymongo.ASCENDING)])
        collection.create_index([("ID", pymongo.ASCENDING)])
        collection.create_index([("REF", pymongo.ASCENDING)])
        collection.create_index([("ALT", pymongo.ASCENDING)])
        collection.create_index([("QUAL", pymongo.ASCENDING)])
        collection.create_index([("FILTER", pymongo.ASCENDING)])
        collection.create_index([("INFO", pymongo.ASCENDING)])
        collection.create_index([("FORMAT", pymongo.ASCENDING)])
        
        logging.info("Índices creados con éxito")
    except Exception as e:
        logging.error(f"Error creando índices: {e}")