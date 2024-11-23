# genome_indexer.py
from concurrent.futures import ThreadPoolExecutor
import pymongo
from pymongo import UpdateOne, InsertOne
from dotenv import load_dotenv
import os
import time
from typing import List, Dict, Tuple
import mmap

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
BATCH_SIZE = 1000

client = pymongo.MongoClient(
    MONGO_URI,
    maxPoolSize=None,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
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
                # Obtener todas las columnas después de FORMAT
                sample_columns = headers[9:]
                # Crear un mapeo de nombre de columna a posición, asegurando el formato output_CH
                column_positions = {}
                for idx, name in enumerate(sample_columns):
                    # Reemplazar CS por CH si es necesario
                    corrected_name = name.replace('output_CS', 'output_CH')
                    column_positions[corrected_name] = idx + 9
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
        # Documento base con campos comunes
        document = {
            "CHROM": fields[0],
            "POS": fields[1],
            "ID": fields[2] if fields[2] != '.' else None,
            "REF": fields[3],
            "ALT": fields[4],
            "QUAL": fields[5],
            "FILTER": fields[6],
            "INFO": fields[7]
        }
        
        # Procesar campos de muestra si existen
        if len(fields) > 8:
            for sample_name, position in column_positions.items():
                if position < len(fields):
                    # Asegurar que usamos el nombre corregido (CH en lugar de CS)
                    document[sample_name] = fields[position]
        
        return document
        
    except Exception as e:
        print(f"Error procesando línea: {e}")
        print(f"Contenido de la línea: {line}")
        return None

def bulk_insert_mongo(data: List[Dict]):
    """Inserta múltiples documentos en MongoDB."""
    if not data:
        return
        
    operations = [
        InsertOne(doc) for doc in data
        if doc is not None
    ]
    
    if operations:
        try:
            result = collection.bulk_write(operations, ordered=False)
            print(f"Insertados {result.inserted_count} documentos")
        except pymongo.errors.BulkWriteError as bwe:
            print(f"Error de escritura masiva: {bwe.details['writeErrors']}")
        except Exception as e:
            print(f"Error durante la inserción masiva: {e}")

def process_file_chunk(file_path: str, start_pos: int, chunk_size: int, column_positions: Dict[str, int]):
    """Procesa un fragmento del archivo."""
    batch = []
    
    with open(file_path, 'r') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm.seek(start_pos)
        
        try:
            for _ in range(chunk_size):
                line = mm.readline().decode('utf-8')
                if not line:
                    break
                    
                document = process_line(line, column_positions)
                if document:
                    batch.append(document)
                    
                if len(batch) >= BATCH_SIZE:
                    bulk_insert_mongo(batch)
                    batch = []
            
            # Insertar registros restantes
            if batch:
                bulk_insert_mongo(batch)
                
        except Exception as e:
            print(f"Error procesando fragmento: {e}")
        finally:
            mm.close()

def create_indices():
    """Crea índices para mejor rendimiento en consultas."""
    try:
        collection.create_index([("CHROM", pymongo.ASCENDING), ("POS", pymongo.ASCENDING)])
        collection.create_index("POS")
        collection.create_index("CHROM")
        print("Índices creados correctamente")
    except Exception as e:
        print(f"Error creando índices: {e}")