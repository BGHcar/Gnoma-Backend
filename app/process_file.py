# process_file.py
from concurrent.futures import ProcessPoolExecutor
import os
from genome_indexer import process_file_chunk, get_header_info, create_indices
from dotenv import load_dotenv
import signal
import sys
import time
from datetime import datetime
import logging

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/genome_processing.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()

NUM_PROCESSES = 8 # int(os.getenv("NUM_PROCESSES", os.cpu_count()))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 50000))
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

# MongoDB setup
# MongoDB setup
client = pymongo.MongoClient(
    MONGO_URI,
    maxPoolSize=None,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
    w=1,
    journal=False,
    maxIdleTimeMS=None,
    compressors='zlib'
)
db = client[DATABASE_NAME]
collection = db.get_collection('genomas', write_concern=pymongo.WriteConcern(w=1, j=False))

def handle_interrupt(signal, frame):
    logging.info("\nProcess cancelled by user.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

def get_total_lines(file_path: str) -> int:
    """Cuenta el número total de líneas en el archivo."""
    with open(file_path, 'rb') as f:
        return sum(1 for _ in f)

def get_chunk_boundaries(file_path: str, chunk_size: int) -> List[Tuple[int, int]]:
    """
    Determina los límites exactos de cada chunk asegurando que no se pierdan líneas.
    Retorna una lista de tuplas (posición_inicio, posición_fin).
    """
    chunk_boundaries = []
    with open(file_path, 'rb') as f:
        chunk_start = 0
        count = 0
        
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                if pos > chunk_start:
                    chunk_boundaries.append((chunk_start, pos))
                break
                
            count += 1
            if count >= chunk_size:
                chunk_boundaries.append((chunk_start, pos))
                chunk_start = pos
                count = 0
    
    return chunk_boundaries

def bulk_insert_mongo(documents: List[Dict], retry_count: int = 3) -> int:
    """
    Inserta documentos en MongoDB con reintentos y mejor manejo de errores.
    """
    if not documents:
        return 0
        
    operations = [InsertOne(doc) for doc in documents]
    
    try:
        # Quitar write_concern del bulk_write
        result = collection.bulk_write(
            operations, 
            ordered=False,
            bypass_document_validation=True
        )
        return result.inserted_count
    except pymongo.errors.BulkWriteError as bwe:
        inserted = bwe.details.get('nInserted', 0)
        logging.warning(f"Bulk write parcialmente exitoso: {inserted} documentos insertados")
        return inserted
    except Exception as e:
        logging.error(f"Error en bulk write: {e}")
        return 0

def process_file_chunk(file_path: str, start_pos: int, end_pos: int, column_positions: Dict[str, int]) -> List[Dict]:
    """
    Procesa un chunk del archivo usando límites exactos.
    """
    batch_size = 5000  # Aumentar el tamaño del batch
    total_inserted = 0
    
    with open(file_path, 'rb') as f:
        f.seek(start_pos)
        current_batch = []
        
        while f.tell() < end_pos:
            try:
                line = f.readline().decode('utf-8')
                if not line:
                    break
                    
                document = process_line(line, column_positions)
                if document:
                    current_batch.append(document)
                    
                    if len(current_batch) >= batch_size:
                        inserted = bulk_insert_mongo(current_batch)
                        total_inserted += inserted
                        current_batch = []
                        
            except Exception as e:
                logging.error(f"Error procesando línea: {e}")
        
        if current_batch:
            inserted = bulk_insert_mongo(current_batch)
            total_inserted += inserted
    
    return total_inserted

def process_file_parallel(file_path: str):
    start_time = time.time()
    start_datetime = datetime.now()
    
    logging.info(f"Starting processing of {file_path}")
    logging.info(f"Start time: {start_datetime}")
    
    # Get file size
    file_size = os.path.getsize(file_path)
    logging.info(f"File size: {file_size / (1024*1024):.2f} MB")
    
    # Get header information
    sample_columns, column_positions = get_header_info(file_path)
    logging.info(f"Found {len(sample_columns)} sample columns")
    
    # Create indices first
    create_indices()
    
    chunk_positions = []
    total_lines = 0
    last_position = 0
    
    # Contador atómico para documentos procesados
    total_processed = Value('i', 0)
    # Incrementar el tamaño del buffer de lectura
    buffer_size = 8 * 1024 * 1024  # 8MB buffer

    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        futures = []
        for start_pos, chunk_len in chunk_positions:
            futures.append(
                executor.submit(
                    process_file_chunk,
                    file_path,
                    start_pos,
                    chunk_len,
                    column_positions
                )
            )
        
        completed = 0
        batch_size = 10
        for i in range(0, len(futures), batch_size):
            batch_futures = futures[i:i + batch_size]
            for future in concurrent.futures.as_completed(batch_futures):
                try:
                    inserted_count = future.result()
                    with total_processed.get_lock():
                        total_processed.value += inserted_count
                    
                    completed += 1
                    if completed % 5 == 0:  # Reducir la frecuencia de logging
                        progress = (completed / len(chunk_boundaries)) * 100
                        elapsed = time.time() - start_time
                        estimated_total = elapsed / (completed / len(chunk_boundaries))
                        remaining = estimated_total - elapsed
                        
                        with total_processed.get_lock():
                            current_processed = total_processed.value
                            processed_ratio = current_processed / total_lines * 100
                        
                        logging.info(
                            f"Progreso: {progress:.1f}% - "
                            f"Documentos procesados: {current_processed} de {total_lines} ({processed_ratio:.1f}%) - "
                            f"Tiempo transcurrido: {elapsed/60:.1f} minutos - "
                            f"Tiempo restante estimado: {remaining/60:.1f} minutos"
                        )
                except Exception as e:
                    logging.error(f"Error procesando chunk: {e}")
    end_time = time.time()
    end_datetime = datetime.now()
    total_time = end_time - start_time
    
    logging.info(f"Processing completed at: {end_datetime}")
    logging.info(f"Total processing time: {total_time/60:.2f} minutes")
    logging.info(f"Average processing speed: {total_lines/total_time:.0f} lines/second")
    logging.info(f"Total lines processed: {total_lines}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_file.py <vcf_file_path>")
        sys.exit(1)
    
    process_file_parallel(sys.argv[1])