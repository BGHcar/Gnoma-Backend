import os
import pymongo

print(f"Procesadores disponibles: {os.cpu_count()}")
print(f"Versión de PyMongo: {pymongo.version}")