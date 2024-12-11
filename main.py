import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import status
from fastapi.responses import FileResponse
from app import server, genome_api
from services import login


app = FastAPI(
    title="GENOME INDEXER API",
    debug=True
)

# Configuración de CORS
origins = [
    "*",  # Permite solicitudes de cualquier origen
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(server.router)
app.include_router(genome_api.router)
app.include_router(login.router)

@app.get("/")
def read_root():
    image_path = os.path.join("resources", "images", "genome-indexer.svg")
    return FileResponse(image_path)

@app.get(
        "/healthCheck",
        status_code=status.HTTP_200_OK  
        )
async def healthCheck():
    '''app is already working!'''
    return {"message": "All works!"}


