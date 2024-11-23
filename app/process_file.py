#process_file.py

import os
from genome_indexer import indexar_genomas
from multiprocessing import Pool
from tqdm import tqdm
from dotenv import load_dotenv

# Cargar configuraciones desde el archivo .env
load_dotenv()

# Obtener el número de procesos desde el archivo .env (con valor predeterminado de 8 si no está definido)
NUM_PROCESOS = int(os.getenv("NUM_PROCESOS", 8))

def procesar_archivos_con_multiprocessing(archivo_path: str):
    # Abrimos el archivo y leemos todas las líneas
    with open(archivo_path, 'r') as file:
        lineas = file.readlines()

    # Usamos un Pool de procesos para procesar las líneas en paralelo con barra de progreso
    with Pool(processes=NUM_PROCESOS) as pool:
        # Utilizamos tqdm para mostrar la barra de progreso
        with tqdm(total=len(lineas), desc="Procesando líneas", unit="línea") as pbar:
            for _ in pool.imap_unordered(indexar_genomas, lineas):
                pbar.update(1)
