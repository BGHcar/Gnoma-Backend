# genome_indexer.py

# Importa ThreadPoolExecutor para ejecutar tareas en paralelo
from concurrent.futures import ThreadPoolExecutor
# Importa pymongo para interactuar con MongoDB
import pymongo
# Importa InsertOne para realizar operaciones de inserción en MongoDB
from pymongo import InsertOne
# Importa load_dotenv para cargar variables de entorno desde un archivo .env
from dotenv import load_dotenv
# Importa os para interactuar con el sistema operativo
import os
# Importa time para medir el tiempo
import time
# Importa logging para registrar mensajes de log
import logging
# Importa mmap para manejar archivos grandes en memoria
import mmap
# Importa List, Dict y Tuple para anotaciones de tipo
from typing import List, Dict, Tuple

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# Obtiene la URI de MongoDB desde las variables de entorno
MONGO_URI = os.getenv("MONGO_URI")
# Obtiene el nombre de la base de datos desde las variables de entorno
DATABASE_NAME = os.getenv("DATABASE_NAME")
# Define el tamaño del lote para inserciones masivas
BATCH_SIZE = 1000

# Crea un cliente de MongoDB con la URI especificada y configuraciones de tiempo de espera
client = pymongo.MongoClient(
    MONGO_URI,
    maxPoolSize=None,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
)
# Selecciona la base de datos
db = client[DATABASE_NAME]
# Selecciona la colección 'genomas'
collection = db['genomas']

def get_header_info(file_path: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Extrae la información del encabezado del archivo VCF.
    Retorna los nombres de las columnas y sus posiciones.
    """
    # Abre el archivo en modo lectura
    with open(file_path, 'r') as f:
        # Itera sobre cada línea del archivo
        for line in f:
            # Si la línea comienza con '#CHROM', es la línea de encabezado
            if line.startswith('#CHROM'):
                # Divide la línea en columnas
                headers = line.strip().split('\t')
                # Obtiene los nombres de las muestras a partir de la columna 9
                sample_columns = headers[9:]
                # Crea un diccionario con los nombres de las muestras y sus posiciones
                column_positions = {
                    name: idx + 9 
                    for idx, name in enumerate(sample_columns)
                }
                # Retorna los nombres de las muestras y sus posiciones
                return sample_columns, column_positions
    # Si no se encuentra la línea de encabezado, lanza un error
    raise ValueError("No se encontró la línea de encabezado en el archivo VCF")

def process_line(line: str, column_positions: Dict[str, int]) -> Dict:
    """Procesa una línea del archivo VCF y retorna un documento."""
    # Si la línea comienza con '#', es un comentario y se ignora
    if line.startswith('#'):
        return None
        
    # Divide la línea en campos
    fields = line.strip().split('\t')
    # Si la línea tiene menos de 8 campos, se ignora
    if len(fields) < 8:
        return None
    
    try:
        # Crea un diccionario con los campos principales del VCF
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
        
        # Añade los datos de las muestras al diccionario
        for sample_name, position in column_positions.items():
            if position < len(fields):
                document[sample_name] = fields[position]
        
        # Retorna el documento
        return document
        
    except Exception as e:
        # Si ocurre un error, lo registra en el log
        logging.error(f"Error procesando línea: {e}")
        logging.error(f"Contenido de la línea: {line}")
        return None

def bulk_insert_mongo(data: List[Dict], total_processed: List[int]):
    """Inserta múltiples documentos en MongoDB."""
    # Si no hay datos, retorna
    if not data:
        return
        
    # Crea una lista de operaciones de inserción
    operations = [
        InsertOne(doc) for doc in data
        if doc is not None
    ]
    
    # Si hay operaciones, intenta realizar la inserción masiva
    if operations:
        try:
            # Realiza la inserción masiva y actualiza el contador de documentos insertados
            result = collection.bulk_write(operations, ordered=False)
            total_processed[0] += result.inserted_count
            logging.info(f"Insertados {result.inserted_count} documentos. Total: {total_processed[0]}")
        except pymongo.errors.BulkWriteError as bwe:
            # Si ocurre un error de escritura masiva, lo registra en el log
            logging.error(f"Error de escritura masiva: {bwe.details.get('writeErrors')}")
        except Exception as e:
            # Si ocurre cualquier otro error, lo registra en el log
            logging.error(f"Error durante la inserción masiva: {e}")

def process_file_chunk(file_path: str, start_pos: int, chunk_size: int, column_positions: Dict[str, int], total_processed: List[int]):
    """Procesa un fragmento del archivo."""
    # Crea una lista para almacenar el lote de documentos
    batch = []
    
    # Abre el archivo en modo lectura
    with open(file_path, 'r') as f:
        # Mapea el archivo en memoria
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        # Posiciona el puntero en la posición de inicio
        mm.seek(start_pos)
        
        try:
            # Itera sobre el número de líneas especificado por chunk_size
            for _ in range(chunk_size):
                # Lee una línea del archivo
                line = mm.readline().decode('utf-8')
                # Si no hay más líneas, rompe el bucle
                if not line:
                    break
                    
                # Procesa la línea y obtiene un documento
                document = process_line(line, column_positions)
                # Si el documento es válido, lo añade al lote
                if document:
                    batch.append(document)
                    
                # Si el lote alcanza el tamaño especificado, realiza una inserción masiva
                if len(batch) >= BATCH_SIZE:
                    bulk_insert_mongo(batch, total_processed)
                    batch = []
            
            # Inserta los documentos restantes en el lote
            if batch:
                bulk_insert_mongo(batch, total_processed)
                
        except Exception as e:
            # Si ocurre un error, lo registra en el log
            logging.error(f"Error procesando fragmento: {e}")
        finally:
            # Cierra el mapeo de memoria
            mm.close()

def create_indices():
    """Crea índices para mejor rendimiento en consultas."""
    try:
        # Crea índices en los campos especificados para mejorar el rendimiento de las consultas
        collection.create_index([("CHROM", pymongo.ASCENDING)])
        collection.create_index([("POS", pymongo.ASCENDING)])
        collection.create_index([("ID", pymongo.ASCENDING)])
        collection.create_index([("REF", pymongo.ASCENDING)])
        collection.create_index([("ALT", pymongo.ASCENDING)])
        collection.create_index([("QUAL", pymongo.ASCENDING)])
        collection.create_index([("FILTER", pymongo.ASCENDING)])
        collection.create_index([("INFO", pymongo.ASCENDING)])
        collection.create_index([("FORMAT", pymongo.ASCENDING)])
        collection.create_index([("output", pymongo.ASCENDING)])
        
        # Registra un mensaje de éxito en el log
        logging.info("Índices creados con éxito")
    except Exception as e:
        # Si ocurre un error, lo registra en el log
        logging.error(f"Error creando índices: {e}")

def main(file_path: str, chunk_size: int):
    """Función principal para procesar el archivo VCF."""
    # Obtiene la información del encabezado
    sample_columns, column_positions = get_header_info(file_path)
    
    # Inicializa el contador de documentos procesados
    total_processed = [0]
    
    # Mide el tiempo de procesamiento
    start_time = time.time()
    
    # Crea un ThreadPoolExecutor para ejecutar tareas en paralelo
    with ThreadPoolExecutor() as executor:
        # Abre el archivo en modo lectura
        with open(file_path, 'r') as f:
            # Mapea el archivo en memoria
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            # Inicializa la posición de inicio
            start_pos = 0
            
            # Itera sobre el archivo en fragmentos del tamaño especificado
            while start_pos < mm.size():
                # Envía una tarea al executor para procesar el fragmento del archivo
                executor.submit(process_file_chunk, file_path, start_pos, chunk_size, column_positions, total_processed)
                # Actualiza la posición de inicio para el siguiente fragmento
                start_pos += chunk_size * 100
                
            # Cierra el mapeo de memoria
            mm.close()
    
    # Mide el tiempo de procesamiento
    end_time = time.time()
    # Calcula el tiempo total de procesamiento
    total_time = end_time - start_time
    
    # Registra el tiempo total de procesamiento en el log
    logging.info(f"Tiempo total de procesamiento: {total_time} segundos")
    # Registra el total de documentos procesados en el log
    logging.info(f"Total de documentos procesados: {total_processed[0]}")
    
    # Crea los índices en la colección
    create_indices()

if __name__ == "__main__":
    # Configura el logging
    logging.basicConfig(level=logging.INFO)
    # Define el archivo VCF a procesar y el tamaño del fragmento
    file_path = "ruta/al/archivo.vcf"
    chunk_size = 1000
    # Llama a la función principal
    main(file_path, chunk_size)