#genome_indexer.py

import pymongo
from pymongo import UpdateOne
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv
import os
import time  # Importamos time para medir el tiempo de ejecución

# Cargar configuraciones desde el archivo .env
load_dotenv()

# Obtener parámetros desde el archivo .env
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
NUM_PROCESOS = int(os.getenv("NUM_PROCESOS", 4))  # Valor predeterminado 4 si no está configurado en .env

# Verifica si las variables se cargaron correctamente
if not MONGO_URI or not DATABASE_NAME:
    raise ValueError("MONGO_URI o DATABASE_NAME no están definidos en el archivo .env")

# Conexión a MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db['genomas']  # Usa el nombre de colección adecuado

# Función para procesar cada línea de datos (ajustada para VCF)
def procesar_linea(linea, nombres_columnas):
    if linea.startswith('#'):  # Ignorar líneas de encabezado
        return None
    datos = linea.strip().split('\t')  # Ajusta el delimitador según tu archivo

    genoma_data = {
        "CHROM": datos[0],  # Cromosoma
        "POS": datos[1],    # Posición
        "ID": datos[2],     # ID
        "REF": datos[3],    # Referencia
        "ALT": datos[4],    # Alternativa
        "QUAL": datos[5],   # Calidad
        "FILTER": datos[6], # Filtro
        "INFO": datos[7],   # Información
    }
    
    for i, columna in enumerate(nombres_columnas):
        if i + 8 < len(datos):
            genoma_data[columna] = datos[i + 8]
    
    return genoma_data

# Función para insertar en MongoDB usando operaciones en bloque
def insertar_en_mongo(datos):
    operaciones = []
    for registro in datos:
        if registro:
            operaciones.append(UpdateOne(
                {"POS": registro["POS"]}, 
                {"$set": registro},
                upsert=True  
            ))
    if operaciones:
        collection.bulk_write(operaciones)

# Función para leer y procesar el archivo en paralelo
def leer_archivo_en_paralelo(archivo):
    with open(archivo, 'r') as f:
        lineas = f.readlines()
    
    nombres_columnas = [col for col in lineas[0].strip().split('\t')[8:] if col.startswith('output_CH')]

    # Comienza a medir el tiempo de ejecución
    start_time = time.time()

    with ProcessPoolExecutor(max_workers=NUM_PROCESOS) as executor:
        registros = list(executor.map(lambda linea: procesar_linea(linea, nombres_columnas), lineas[1:]))
    
    # Calcula el tiempo transcurrido
    end_time = time.time()
    tiempo_ejecucion = end_time - start_time

    insertar_en_mongo(registros)

    print(f"Tiempo de procesamiento: {tiempo_ejecucion:.2f} segundos")

# Función que envuelve el procesamiento de cada línea
def indexar_genomas(linea):
    nombres_columnas = [f'output_CH{i}' for i in range(1, 20)]  # Genera los nombres de columna dinámicamente
    registros = procesar_linea(linea, nombres_columnas)
    insertar_en_mongo([registros])

