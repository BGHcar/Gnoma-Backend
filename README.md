```markdown
# Gnoma-Backend

Este proyecto es un backend para indexar información genética de genomas de uva utilizando MongoDB, Python y Angular. Está diseñado para manejar archivos TXT con información sobre genomas y procesarlos en paralelo para optimizar el rendimiento.

## Requisitos previos

Asegúrate de tener los siguientes requisitos instalados en tu sistema antes de comenzar:

- **Python 3.12 o superior** (se recomienda utilizar un entorno virtual para administrar las dependencias)
- **MongoDB** (debe estar corriendo localmente o en un servidor accesible)
- **Git** (opcional, para clonar el repositorio)
- **Node.js y npm** (para la interfaz en Angular)

## Configuración del Backend

### 1. Clonar el repositorio

Primero, clona el repositorio del proyecto en tu máquina local:

```bash
git clone https://github.com/tu_usuario/gnoma-backend.git
cd gnoma-backend/app
```

### 2. Crear un entorno virtual de Python

Para mantener las dependencias organizadas, se recomienda crear un entorno virtual. Sigue estos pasos:

#### En Windows:

```bash
python -m venv venv
venv\Scripts\activate
```

#### En Linux/MacOS:

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar las dependencias de Python

Instala las dependencias necesarias utilizando `pip`:

```bash
pip install -r requirements.txt
```

Si no tienes un archivo `requirements.txt`, aquí tienes una lista de las dependencias necesarias que puedes instalar directamente:

```bash
pip install fastapi uvicorn motor pymongo python-dotenv
```

- **`fastapi`**: Framework web para construir APIs rápidas.
- **`uvicorn`**: Servidor ASGI para correr aplicaciones de FastAPI.
- **`motor`**: Cliente asincrónico para MongoDB.
- **`pymongo`**: Cliente síncrono para MongoDB.
- **`python-dotenv`**: Para cargar variables de entorno desde un archivo `.env`.

### 4. Configurar variables de entorno

Crea un archivo `.env` en el directorio raíz del proyecto (donde está `app`) para configurar la conexión a la base de datos y otras variables de configuración. El archivo `.env` debe tener el siguiente formato:

```
MONGO_URI=mongodb://localhost:27017
DATABASE_NAME=nombre_de_tu_base_de_datos
```

- **`MONGO_URI`**: La URL de conexión a tu servidor de MongoDB. Por defecto, si es local, debería ser `mongodb://localhost:27017`.
- **`DATABASE_NAME`**: Nombre de la base de datos que deseas usar.

### 5. Ejecutar el servidor

Para iniciar el servidor FastAPI, usa el siguiente comando:

```bash
uvicorn server:app --reload
```

- **`server`**: El nombre del archivo Python que contiene la instancia de la aplicación FastAPI (`app`).
- **`app`**: El nombre de la instancia FastAPI dentro del archivo `server.py`.
- **`--reload`**: Activa la recarga automática cuando hay cambios en el código.

El servidor debería estar ejecutándose en: [http://127.0.0.1:8000](http://127.0.0.1:8000).

### 6. Probar la API

Para verificar que la API esté funcionando correctamente, accede a la documentación interactiva de la API proporcionada por FastAPI:

- **Swagger UI**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc**: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

Puedes usar estas interfaces para probar los endpoints del proyecto.


## Procesamiento de Archivos

### 1. Subir un Archivo

Para procesar un archivo de genoma, utiliza la API `POST /procesar_archivo`. Puedes hacer esto desde la línea de comandos usando `curl`:

```bash
curl -X POST -F "file=@../data/tu_archivo_genoma.txt" http://127.0.0.1:8000/process_file
```

O bien, usa la interfaz Swagger UI para cargar el archivo directamente.

### 2. Indexación en Paralelo

El backend utiliza procesamiento paralelo para indexar el archivo de genoma, lo que permite una mayor eficiencia en la carga de datos grandes. Asegúrate de que la configuración esté optimizada para el hardware que estás utilizando.

## Solución de Problemas

1. **Error: `daemonic processes are not allowed to have children`**:
   - Asegúrate de no usar procesos secundarios dentro de procesos de `multiprocessing`. Si ves este error, verifica que la función que se está utilizando no esté creando subprocesos.
   - Utiliza `multiprocessing.set_start_method('spawn')` al inicio de tu código en lugar de `fork` si estás en Windows.

2. **Error: `TypeError: name must be an instance of str`**:
   - Verifica que las variables de entorno se carguen correctamente y que el nombre de la base de datos (`DATABASE_NAME`) esté configurado como una cadena en tu archivo `.env`.

3. **Error de Conexión a MongoDB**:
   - Asegúrate de que tu servidor de MongoDB esté corriendo y accesible en la URI proporcionada en `MONGO_URI`.
   - Revisa la configuración del cortafuegos o permisos de acceso.