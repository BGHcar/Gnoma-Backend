#server.py

from fastapi import FastAPI, UploadFile, File
import shutil
import os
from process_file import procesar_archivos_con_multiprocessing

app = FastAPI()

# Endpoint para procesar el archivo
@app.post("/procesar_archivo")
async def procesar_archivo(file: UploadFile = File(...)):
    try:
        # Directorio donde se guardarán los archivos cargados
        TEMP_DIR = './data'
        os.makedirs(TEMP_DIR, exist_ok=True)

        # Guardar archivo temporalmente
        archivo_path = os.path.join(TEMP_DIR, file.filename)
        with open(archivo_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Procesar el archivo y subirlo a MongoDB usando multiprocessing
        procesar_archivos_con_multiprocessing(archivo_path)

        return {"message": f"Archivo '{file.filename}' procesado y datos subidos a MongoDB."}
    except Exception as e:
        # Captura cualquier excepción y muestra el error
        return {"error": f"Hubo un error procesando el archivo: {str(e)}"}
    

# Endpoint de prueba
@app.get("/")
async def root():
    return {"message": "FastAPI servidor funcionando correctamente."}
