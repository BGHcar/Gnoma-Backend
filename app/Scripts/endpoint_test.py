import os
import requests
import json

BASE_URL = os.getenv("BASE_URL")

def test_endpoints():
    # Test 1: Obtener variantes de un cromosoma
    print("\n1. Probando obtención de variantes...")
    response = requests.get(f"{BASE_URL}/variants/1", params={
        "page": 1,
        "page_size": 10
    })
    print_response(response)

    # Test 2: Obtener lista de muestras
    print("\n2. Probando lista de muestras...")
    response = requests.get(f"{BASE_URL}/samples")
    print_response(response)

    # Test 3: Búsqueda de variantes
    print("\n3. Probando búsqueda de variantes...")
    response = requests.get(f"{BASE_URL}/variants/search", params={
        "query_text": "rs",
        "page": 1,
        "page_size": 10
    })
    print_response(response)

    # Test 4: Estadísticas de cromosoma
    print("\n4. Probando estadísticas de cromosoma...")
    response = requests.get(f"{BASE_URL}/stats/chromosome/1")
    print_response(response)

    # Test 5: Variantes con filtros
    print("\n5. Probando variantes con filtros...")
    response = requests.get(f"{BASE_URL}/variants/1", params={
        "start_pos": 1000,
        "end_pos": 2000,
        "sample_id": "output_CH1",
        "page": 1,
        "page_size": 10
    })
    print_response(response)

def print_response(response):
    print(f"Status Code: {response.status_code}")
    try:
        print("Response:", json.dumps(response.json(), indent=2))
    except:
        print("Response:", response.text)
    print("Time:", response.elapsed.total_seconds(), "seconds")

if __name__ == "__main__":
    test_endpoints()