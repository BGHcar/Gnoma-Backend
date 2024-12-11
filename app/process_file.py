# process_file.py
from concurrent.futures import ProcessPoolExecutor  # Importa el ejecutor de procesos en paralelo
import concurrent.futures  # Importa futuros concurrentes
from multiprocessing import Value  # Importa Value para variables compartidas entre procesos
import os  # Importa el módulo os para operaciones del sistema operativo
from genome_indexer import get_header_info, create_indices, process_line  # Importa funciones del indexador de genomas
from dotenv import load_dotenv  # Importa load_dotenv para cargar variables de entorno
import signal  # Importa signal para manejar señales del sistema
import sys  # Importa sys para interactuar con el intérprete de Python
import time  # Importa time para medir el tiempo
from datetime import datetime  # Importa datetime para manejar fechas y horas
import logging  # Importa logging para registrar mensajes
from typing import List, Dict, Tuple  # Importa tipos de datos para anotaciones de tipo
import pymongo  # Importa pymongo para interactuar con MongoDB
from pymongo import InsertOne  # Importa InsertOne para operaciones de inserción en MongoDB

# Configura el registro de mensajes
logging.basicConfig(
    level=logging.INFO,  # Nivel de registro
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato del mensaje
    handlers=[
        logging.StreamHandler(),  # Manejador para la consola
        logging.FileHandler('processing.log')  # Manejador para el archivo de registro
    ]
)

load_dotenv()  # Carga las variables de entorno desde un archivo .env

NUM_PROCESSES = 8  # Número de procesos a usar
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 50000))  # Tamaño de cada chunk
MONGO_URI = os.getenv("MONGO_URI")  # URI de MongoDB
DATABASE_NAME = os.getenv("DATABASE_NAME")  # Nombre de la base de datos

# Configuración de MongoDB
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
db = client[DATABASE_NAME]  # Selecciona la base de datos
collection = db.get_collection('genomas', write_concern=pymongo.WriteConcern(w=1, j=False))  # Selecciona la colección

def handle_interrupt(signal, frame):  # Maneja la interrupción del usuario
    logging.info("\nProceso cancelado por el usuario.")  # Registra el mensaje
    sys.exit(0)  # Sale del programa

signal.signal(signal.SIGINT, handle_interrupt)  # Asigna la función de manejo de interrupción

def get_total_lines(file_path: str) -> int:  # Cuenta el número total de líneas en el archivo
    with open(file_path, 'rb') as f:  # Abre el archivo en modo binario
        return sum(1 for _ in f)  # Cuenta las líneas

def get_chunk_boundaries(file_path: str, chunk_size: int) -> List[Tuple[int, int]]:  # Determina los límites de cada chunk
    chunk_boundaries = []  # Lista para almacenar los límites
    with open(file_path, 'rb') as f:  # Abre el archivo en modo binario
        chunk_start = 0  # Inicio del chunk
        count = 0  # Contador de líneas
        
        while True:
            pos = f.tell()  # Obtiene la posición actual
            line = f.readline()  # Lee una línea
            if not line:  # Si no hay más líneas
                if pos > chunk_start:  # Si la posición es mayor que el inicio del chunk
                    chunk_boundaries.append((chunk_start, pos))  # Añade los límites del chunk
                break
                
            count += 1  # Incrementa el contador
            if count >= chunk_size:  # Si el contador alcanza el tamaño del chunk
                chunk_boundaries.append((chunk_start, pos))  # Añade los límites del chunk
                chunk_start = pos  # Actualiza el inicio del chunk
                count = 0  # Reinicia el contador
    
    return chunk_boundaries  # Retorna los límites de los chunks

def bulk_insert_mongo(documents: List[Dict], retry_count: int = 3) -> int:  # Inserta documentos en MongoDB
    if not documents:  # Si no hay documentos
        return 0
        
    operations = [InsertOne(doc) for doc in documents]  # Crea operaciones de inserción
    
    try:
        result = collection.bulk_write(
            operations, 
            ordered=False,
            bypass_document_validation=True
        )
        return result.inserted_count  # Retorna el número de documentos insertados
    except pymongo.errors.BulkWriteError as bwe:  # Maneja errores de escritura en bloque
        inserted = bwe.details.get('nInserted', 0)  # Obtiene el número de documentos insertados
        logging.warning(f"Bulk write parcialmente exitoso: {inserted} documentos insertados")  # Registra una advertencia
        return inserted
    except Exception as e:  # Maneja otros errores
        logging.error(f"Error en bulk write: {e}")  # Registra un error
        return 0

def process_file_chunk(file_path: str, start_pos: int, end_pos: int, column_positions: Dict[str, int]) -> List[Dict]:  # Procesa un chunk del archivo
    batch_size = 5000  # Tamaño del batch
    total_inserted = 0  # Total de documentos insertados
    
    with open(file_path, 'rb') as f:  # Abre el archivo en modo binario
        f.seek(start_pos)  # Se posiciona en el inicio del chunk
        current_batch = []  # Lista para almacenar el batch actual
        
        while f.tell() < end_pos:  # Mientras no se alcance el final del chunk
            try:
                line = f.readline().decode('utf-8')  # Lee una línea y la decodifica
                if not line:  # Si no hay más líneas
                    break
                    
                document = process_line(line, column_positions)  # Procesa la línea
                if document:  # Si se obtiene un documento
                    current_batch.append(document)  # Añade el documento al batch
                    
                    if len(current_batch) >= batch_size:  # Si el batch alcanza el tamaño
                        inserted = bulk_insert_mongo(current_batch)  # Inserta el batch en MongoDB
                        total_inserted += inserted  # Incrementa el total de documentos insertados
                        current_batch = []  # Reinicia el batch
                        
            except Exception as e:  # Maneja errores
                logging.error(f"Error procesando línea: {e}")  # Registra un error
        
        if current_batch:  # Si hay documentos en el batch
            inserted = bulk_insert_mongo(current_batch)  # Inserta el batch en MongoDB
            total_inserted += inserted  # Incrementa el total de documentos insertados
    
    return total_inserted  # Retorna el total de documentos insertados

def process_file_parallel(file_path: str):  # Procesa un archivo en paralelo
    start_time = time.time()  # Obtiene el tiempo de inicio
    start_datetime = datetime.now()  # Obtiene la fecha y hora de inicio
    
    logging.info(f"Iniciando procesamiento de {file_path}")  # Registra el inicio del procesamiento
    logging.info(f"Hora de inicio: {start_datetime}")  # Registra la hora de inicio
    
    file_size = os.path.getsize(file_path)  # Obtiene el tamaño del archivo
    total_lines = get_total_lines(file_path)  # Obtiene el total de líneas
    logging.info(f"Tamaño del archivo: {file_size / (1024*1024):.2f} MB")  # Registra el tamaño del archivo
    logging.info(f"Total de líneas en el archivo: {total_lines}")  # Registra el total de líneas
    
    sample_columns, column_positions = get_header_info(file_path)  # Obtiene la información del encabezado
    logging.info(f"Encontradas {len(sample_columns)} columnas de muestras")  # Registra el número de columnas de muestras
    
    create_indices()  # Crea índices
    
    chunk_boundaries = get_chunk_boundaries(file_path, CHUNK_SIZE)  # Obtiene los límites de los chunks
    logging.info(f"Total de chunks a procesar: {len(chunk_boundaries)}")  # Registra el total de chunks
    
    total_processed = Value('i', 0)  # Contador atómico para documentos procesados
    buffer_size = 8 * 1024 * 1024  # Tamaño del buffer de lectura

    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:  # Crea un ejecutor de procesos
        futures = []  # Lista para almacenar los futuros
        for start_pos, end_pos in chunk_boundaries:  # Para cada chunk
            futures.append(
                executor.submit(
                    process_file_chunk,
                    file_path,
                    start_pos,
                    end_pos,
                    column_positions
                )
            )
        
        completed = 0  # Contador de chunks completados
        batch_size = 10  # Tamaño del batch de futuros
        for i in range(0, len(futures), batch_size):  # Para cada batch de futuros
            batch_futures = futures[i:i + batch_size]  # Obtiene el batch de futuros
            for future in concurrent.futures.as_completed(batch_futures):  # Para cada futuro completado
                try:
                    inserted_count = future.result()  # Obtiene el resultado del futuro
                    with total_processed.get_lock():  # Bloquea el contador atómico
                        total_processed.value += inserted_count  # Incrementa el contador atómico
                    
                    completed += 1  # Incrementa el contador de chunks completados
                    if completed % 5 == 0:  # Cada 5 chunks completados
                        progress = (completed / len(chunk_boundaries)) * 100  # Calcula el progreso
                        elapsed = time.time() - start_time  # Calcula el tiempo transcurrido
                        estimated_total = elapsed / (completed / len(chunk_boundaries))  # Estima el tiempo total
                        remaining = estimated_total - elapsed  # Calcula el tiempo restante
                        
                        with total_processed.get_lock():  # Bloquea el contador atómico
                            current_processed = total_processed.value  # Obtiene el valor del contador atómico
                            processed_ratio = current_processed / total_lines * 100  # Calcula el porcentaje procesado
                        
                        logging.info(
                            f"Progreso: {progress:.1f}% - "
                            f"Documentos procesados: {current_processed} de {total_lines} ({processed_ratio:.1f}%) - "
                            f"Tiempo transcurrido: {elapsed/60:.1f} minutos - "
                            f"Tiempo restante estimado: {remaining/60:.1f} minutos"
                        )
                except Exception as e:  # Maneja errores
                    logging.error(f"Error procesando chunk: {e}")  # Registra un error
    end_time = time.time()  # Obtiene el tiempo de finalización
    end_datetime = datetime.now()  # Obtiene la fecha y hora de finalización
    total_time = end_time - start_time  # Calcula el tiempo total
    
    with total_processed.get_lock():  # Bloquea el contador atómico
        final_processed = total_processed.value  # Obtiene el valor final del contador atómico
    
    logging.info(f"Procesamiento completado en: {end_datetime}")  # Registra la finalización del procesamiento
    logging.info(f"Tiempo total de procesamiento: {total_time/60:.2f} minutos")  # Registra el tiempo total
    logging.info(f"Velocidad promedio: {total_lines/total_time:.0f} líneas/segundo")  # Registra la velocidad promedio
    logging.info(f"Total de documentos procesados: {final_processed} de {total_lines}")  # Registra el total de documentos procesados
    logging.info(f"Porcentaje completado: {(final_processed/total_lines)*100:.2f}%")  # Registra el porcentaje completado
    logging.info(f"Número de procesos usados: {NUM_PROCESSES}")  # Registra el número de procesos usados
    logging.info(f"Tamaño de chunk: {CHUNK_SIZE}")  # Registra el tamaño del chunk

if __name__ == "__main__":  # Punto de entrada del script
    if len(sys.argv) != 2:  # Si no se proporciona la ruta del archivo
        print("Uso: python process_file.py <ruta_archivo_vcf>")  # Muestra el uso correcto
        sys.exit(1)  # Sale del programa
    
    process_file_parallel(sys.argv[1])  # Procesa el archivo en paralelo