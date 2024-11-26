# Buscador de cromosomas usando FAST API

'''
MONGO_URI=mongodb://localhost:27017
DATABASE_NAME=genomas
NUM_PROCESSES=8
CHUNK_SIZE=10000
MAX_WORKERS=16
BASE_URL=http://localhost:8000/genome
'''


'''
Estructura de la base de datos

{
  "_id": {
    "$oid": "674176882e554c12cc07d119"
  },
  "CHROM": "VITVvi_vCabSauv08_v1.1.hap1.chr01",
  "POS": "3789133",
  "ID": null,
  "REF": "T",
  "ALT": "A",
  "QUAL": "401",
  "FILTER": "PASS",
  "INFO": ".",
  "output_CH1": "0/0:34,0:34:99:.:.:0,99,1485:.",
  "output_CH12": "0/0:30,0:30:55:.:.:0,55,981:.",
  "output_CH14": "0/0:26,0:26:72:.:.:0,72,1080:.",
  "output_CH15": "0/0:26,0:26:54:.:.:0,54,810:.",
  "output_CH16": "0/0:22,0:22:60:.:.:0,60,900:.",
  "output_CH17": "0/0:28,0:28:72:.:.:0,72,1080:.",
  "output_CH18": "0/0:35,0:35:99:.:.:0,99,1260:.",
  "output_CH19": "0/0:32,0:32:90:.:.:0,90,1350:.",
  "output_CH20": "0/0:20,0:20:60:.:.:0,60,730:.",
  "output_CH21": "0/0:27,0:27:81:.:.:0,81,929:.",
  "output_CH22": "0/0:30,0:30:81:.:.:0,81,1215:.",
  "output_CH23": "0/0:35,0:35:99:.:.:0,99,1485:.",
  "output_CH24": "0|1:3,11:14:93:0|1:3789110_T_G:415,0,93:3789110",
  "output_CH25": "0/0:23,0:23:34:.:.:0,34,681:.",
  "output_CH3": "0/0:20,0:20:54:.:.:0,54,810:.",
  "output_CH4": "0/0:34,0:34:99:.:.:0,99,1485:.",
  "output_CH5": "0/0:30,0:30:81:.:.:0,81,1215:.",
  "output_CH6": "0/0:21,0:21:57:.:.:0,57,855:.",
  "output_CH7": "0/0:31,0:31:84:.:.:0,84,1260:.",
  "output_CH8": "0/0:30,0:30:81:.:.:0,81,1215:.",
  "output_CH9": "0/0:16,0:16:27:.:.:0,27,578:."
}
'''

import os
# no necesito ningun typing
import pymongo
from dotenv import load_dotenv

#FAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST APIFAST API
from fastapi import FastAPI

#
# Cargar variables de entorno
#
load_dotenv()

#
# Conexión a la base de datos
#

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db['genomas']

#
# Inicializar la aplicación
#

app = FastAPI()

#

@app.get("/variants/{chromosome}")
async def get_variants(chromosome: str, page: int = 1, page_size: int = 10):
    """
    Obtiene las variantes de un cromosoma.
    """
    # Calcular el índice de inicio
    start_index = (page - 1) * page_size
    # Obtener las variantes
    variants = collection.find({"CHROM": chromosome}).skip(start_index).limit(page_size)
    return list(variants)

@app.get("/samples")
async def get_samples():
    """
    Obtiene la lista de muestras.
    """
    # Obtener las muestras
    samples = collection.find_one()
    # Obtener las columnas de las muestras
    sample_columns = [col for col in samples if col.startswith("output_CH")]
    return sample_columns

@app.get("/variants/search")
async def search_variants(query_text: str, page: int = 1, page_size: int = 10):
    """
    Busca variantes por texto.
    """
    # Calcular el índice de inicio
    start_index = (page - 1) * page_size
    # Buscar variantes
    variants = collection.find({"$text": {"$search": query_text}}).skip(start_index).limit(page_size)
    return list(variants)

@app.get("/stats/chromosome/{chromosome}")
async def get_chromosome_stats(chromosome: str):
    """
    Obtiene las estadísticas de un cromosoma.
    """
    # Contar las variantes de un cromosoma
    total_variants = collection.count_documents({"CHROM": chromosome})
    return {"total_variants": total_variants}

@app.get("/variants/{chromosome}/filter")
async def filter_variants(chromosome: str, start_pos: int, end_pos: int, sample_id: str, page: int = 1, page_size: int = 10):
    """
    Filtra variantes por posición y muestra.
    """
    # Calcular el índice de inicio
    start_index = (page - 1) * page_size
    # Filtrar variantes
    variants = collection.find({
        "CHROM": chromosome,
        "POS": {"$gte": start_pos, "$lte": end_pos},
        sample_id: {"$regex": "1/1|0/1"}
    }).skip(start_index).limit(page_size)
    return list(variants)

# Get all CHROM

@app.get("/variants/all")
async def get_all_variants(page: int = 1, page_size: int = 10):
    """
    Obtiene todas las variantes.
    """
    # Calcular el índice de inicio
    start_index = (page - 1) * page_size
    # Obtener todas las variantes
    variants = collection.count_documents()
    return list(variants)







'''
Endpoint de prueba  

http://localhost:8000/genome/variants/1?page=1&page_size=10
http://localhost:8000/genome/samples
http://localhost:8000/genome/variants/search?query_text=rs&page=1&page_size=10
http://localhost:8000/genome/stats/chromosome/1
http://localhost:8000/genome/variants/1?start_pos=1000&end_pos=2000&sample_id=output_CH1&page=1&page_size=10
http://localhost:8000/genome/variants/all?page=1&page_size=10  - No muestra nada
'''
