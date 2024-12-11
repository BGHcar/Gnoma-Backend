import os  # Importa el módulo os para interactuar con el sistema operativo
import sys  # Importa el módulo sys para manipular el entorno de ejecución de Python
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))  # Añade el directorio 'app' al path de búsqueda de módulos

from fastapi import FastAPI  # Importa FastAPI para crear la aplicación web
from fastapi.middleware.cors import CORSMiddleware  # Importa CORSMiddleware para manejar CORS
from fastapi import status  # Importa status para manejar códigos de estado HTTP
from fastapi.responses import FileResponse  # Importa FileResponse para enviar archivos como respuesta
from app import server, genome_api  # Importa los routers server y genome_api desde el módulo app
from services import login  # Importa el router login desde el módulo services

app = FastAPI(  # Crea una instancia de la aplicación FastAPI
    title="GENOME INDEXER API",  # Título de la API
    debug=True  # Habilita el modo debug
)

# Configuración de CORS
origins = [
    "*",  # Permite solicitudes de cualquier origen
]

app.add_middleware(  # Añade el middleware de CORS a la aplicación
    CORSMiddleware,
    allow_origins=origins,  # Permite solicitudes de cualquier origen
    allow_credentials=True,  # Permite el uso de credenciales
    allow_methods=["*"],  # Permite todos los métodos HTTP
    allow_headers=["*"],  # Permite todos los encabezados
)

app.include_router(server.router)  # Incluye el router server en la aplicación
app.include_router(genome_api.router)  # Incluye el router genome_api en la aplicación
app.include_router(login.router)  # Incluye el router login en la aplicación

@app.get("/")  # Define una ruta GET para la raíz del sitio
def read_root():
    image_path = os.path.join("resources", "images", "genome-indexer.svg")  # Define la ruta de la imagen
    return FileResponse(image_path)  # Devuelve la imagen como respuesta

@app.get(  # Define una ruta GET para el health check
        "/healthCheck",
        status_code=status.HTTP_200_OK  # Define el código de estado HTTP 200 OK
        )
async def healthCheck():
    '''app is already working!'''
    return {"message": "All works!"}  # Devuelve un mensaje indicando que la aplicación funciona


