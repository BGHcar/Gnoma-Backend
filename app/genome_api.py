# genome_api.py
from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import asyncio
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

# Configuración de MongoDB
client = MongoClient(
    MONGO_URI,
    maxPoolSize=None,
    connectTimeoutMS=30000,
    socketTimeoutMS=None,
    connect=False,
)
db = client[DATABASE_NAME]
collection = db['genomas']

# Configuración del executor para paralelización
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

app = FastAPI(title="Genome Query API")

async def parallel_query(query: Dict[str, Any], skip: int, limit: int) -> List[Dict]:
    """Ejecuta una consulta MongoDB en un thread separado"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        thread_pool,
        lambda: list(collection.find(query).skip(skip).limit(limit))
    )

async def parallel_count(query: Dict[str, Any]) -> int:
    """Cuenta documentos que coinciden con la consulta en un thread separado"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        thread_pool,
        lambda: collection.count_documents(query)
    )

@app.get("/variants/{chromosome}")
async def get_variants_by_chromosome(
    chromosome: str,
    start_pos: Optional[int] = Query(None, description="Posición inicial"),
    end_pos: Optional[int] = Query(None, description="Posición final"),
    sample_id: Optional[str] = Query(None, description="ID de la muestra (output_CH*)"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(100, ge=1, le=1000, description="Resultados por página")
):
    """
    Obtiene variantes genéticas filtradas por cromosoma y otros parámetros opcionales.
    Soporta paginación y filtrado por rango de posición y muestra.
    """
    try:
        # Construir query base
        query = {"CHROM": chromosome}
        
        # Añadir filtros de posición si se especifican
        if start_pos is not None or end_pos is not None:
            query["POS"] = {}
            if start_pos is not None:
                query["POS"]["$gte"] = str(start_pos)
            if end_pos is not None:
                query["POS"]["$lte"] = str(end_pos)

        # Añadir filtro de muestra si se especifica
        if sample_id:
            query[sample_id] = {"$exists": True}

        # Calcular skip para paginación
        skip = (page - 1) * page_size

        # Ejecutar consulta y conteo en paralelo
        variants_future = parallel_query(query, skip, page_size)
        total_count_future = parallel_count(query)

        # Esperar resultados
        variants, total_count = await asyncio.gather(variants_future, total_count_future)

        # Calcular metadata de paginación
        total_pages = (total_count + page_size - 1) // page_size

        return {
            "data": variants,
            "pagination": {
                "total": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/samples")
async def get_samples():
    """
    Obtiene la lista de todas las muestras disponibles (columnas output_CH*)
    """
    try:
        # Obtener todas las keys del primer documento
        sample_keys = await asyncio.to_thread(
            lambda: collection.find_one({}, {"_id": 0})
        )
        
        if not sample_keys:
            return []

        # Filtrar solo las keys que comienzan con output_CH
        samples = [key for key in sample_keys.keys() if key.startswith("output_CH")]
        return sorted(samples)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/variants/search")
async def search_variants(
    query_text: str = Query(..., description="Texto a buscar en ID, REF o ALT"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000)
):
    """
    Búsqueda de variantes por texto en campos ID, REF o ALT
    """
    try:
        # Construir query de búsqueda
        search_query = {
            "$or": [
                {"ID": {"$regex": query_text, "$options": "i"}},
                {"REF": {"$regex": query_text, "$options": "i"}},
                {"ALT": {"$regex": query_text, "$options": "i"}}
            ]
        }

        skip = (page - 1) * page_size

        # Ejecutar búsqueda y conteo en paralelo
        results_future = parallel_query(search_query, skip, page_size)
        count_future = parallel_count(search_query)

        results, total_count = await asyncio.gather(results_future, count_future)

        return {
            "data": results,
            "pagination": {
                "total": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_count + page_size - 1) // page_size
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/chromosome/{chromosome}")
async def get_chromosome_stats(chromosome: str):
    """
    Obtiene estadísticas básicas para un cromosoma específico
    """
    try:
        pipeline = [
            {"$match": {"CHROM": chromosome}},
            {"$group": {
                "_id": None,
                "total_variants": {"$sum": 1},
                "min_position": {"$min": {"$toInt": "$POS"}},
                "max_position": {"$max": {"$toInt": "$POS"}},
                "unique_refs": {"$addToSet": "$REF"},
                "unique_alts": {"$addToSet": "$ALT"}
            }}
        ]

        stats = await asyncio.to_thread(
            lambda: list(collection.aggregate(pipeline))
        )

        if not stats:
            raise HTTPException(status_code=404, detail=f"No data found for chromosome {chromosome}")

        stats = stats[0]
        return {
            "chromosome": chromosome,
            "total_variants": stats["total_variants"],
            "position_range": {
                "min": stats["min_position"],
                "max": stats["max_position"]
            },
            "unique_refs": len(stats["unique_refs"]),
            "unique_alts": len(stats["unique_alts"])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/all")
async def get_all_data(
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(100, ge=1, le=1000, description="Resultados por página"),
    chromosome: Optional[str] = Query(None, description="Filtrar por cromosoma"),
    sort_by: Optional[str] = Query(None, description="Campo por el cual ordenar", enum=["CHROM", "POS", "ID"]),
    sort_order: Optional[str] = Query("asc", enum=["asc", "desc"], description="Orden de clasificación")
):
    """
    Obtiene todos los datos de la base de datos con paginación y filtros opcionales.
    """
    try:
        # Construir query base
        query = {}
        
        # Añadir filtros si se especifican
        if chromosome:
            query["CHROM"] = chromosome

        # Calcular skip para paginación
        skip = (page - 1) * page_size

        # Configurar ordenamiento
        sort_config = None
        if sort_by:
            sort_config = [(sort_by, pymongo.ASCENDING if sort_order == "asc" else pymongo.DESCENDING)]

        # Ejecutar consulta y conteo en paralelo
        data_future = parallel_query(query, skip, page_size) if not sort_config else \
            asyncio.to_thread(lambda: list(collection.find(query).sort(sort_config).skip(skip).limit(page_size)))
        count_future = parallel_count(query)

        # Esperar resultados
        data, total_count = await asyncio.gather(data_future, count_future)

        # Calcular metadata de paginación
        total_pages = (total_count + page_size - 1) // page_size

        return {
            "data": data,
            "pagination": {
                "total_records": total_count,
                "current_page": page,
                "page_size": page_size,
                "total_pages": total_pages
            },
            "filters_applied": {
                "chromosome": chromosome if chromosome else "None",
                "sort_by": sort_by if sort_by else "None",
                "sort_order": sort_order if sort_by else "None"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/summary")
async def get_database_summary():
    """
    Obtiene un resumen general de la base de datos
    """
    try:
        # Pipeline para obtener estadísticas generales
        pipeline = [
            {
                "$group": {
                    "_id": "$CHROM",
                    "count": {"$sum": 1},
                    "unique_refs": {"$addToSet": "$REF"},
                    "unique_alts": {"$addToSet": "$ALT"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]

        # Ejecutar agregación
        stats = await asyncio.to_thread(
            lambda: list(collection.aggregate(pipeline))
        )

        # Obtener lista de campos disponibles
        sample_doc = await asyncio.to_thread(
            lambda: collection.find_one({}, {"_id": 0})
        )
        available_fields = list(sample_doc.keys()) if sample_doc else []

        # Construir resumen
        total_records = sum(stat["count"] for stat in stats)
        
        return {
            "total_records": total_records,
            "chromosomes": [
                {
                    "chromosome": stat["_id"],
                    "variant_count": stat["count"],
                    "unique_refs": len(stat["unique_refs"]),
                    "unique_alts": len(stat["unique_alts"])
                }
                for stat in stats
            ],
            "available_fields": available_fields,
            "last_updated": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))