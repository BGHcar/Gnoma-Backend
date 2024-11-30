import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[DATABASE_NAME]

totalCores = os.cpu_count()

# Función asíncrona para obtener el total de documentos
async def getTotalDocuments():
    try:
        total_documents = await collection.count_documents({})
        return total_documents
    except Exception as e:
        print(f"Error getting total documents: {e}")
        return 0

# Función asíncrona para obtener los índices
async def getIndexes():
    try:
        indexes = await collection.index_information()
        return indexes
    except Exception as e:
        print(f"Error getting indexes: {e}")
        return {}

# Si necesitas ejecutar estas funciones directamente:
async def main():
    total = await getTotalDocuments()
    indexes = await getIndexes()
    print(f"Total documents: {total}")
    print(f"Indexes: {indexes}")


async def getValidFields():
    primer_documento = await collection.find_one()
    ultimo_documento = await collection.find_one(sort=[('_id', -1)])
    sample_document = {**primer_documento, **ultimo_documento}
    valid_fields = sample_document.keys()
    return valid_fields