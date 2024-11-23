# server.py
from fastapi import FastAPI, UploadFile, File, BackgroundTasks
import shutil
import os
from process_file import process_file_parallel
import asyncio

app = FastAPI()

@app.post("/process_file")
async def process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    try:
        TEMP_DIR = './data'
        os.makedirs(TEMP_DIR, exist_ok=True)

        file_path = os.path.join(TEMP_DIR, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Process file in background
        background_tasks.add_task(process_file_parallel, file_path)

        return {
            "message": f"File '{file.filename}' upload complete. Processing started in background."
        }
    except Exception as e:
        return {"error": f"Error processing file: {str(e)}"}

@app.get("/")
async def root():
    return {"message": "FastAPI server running correctly."}