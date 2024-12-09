import paho.mqtt.client as mqtt
import os
import threading
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import requests
import json
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, APIRouter, Form
import logging
from jose import JWTError, jwt
from datetime import datetime, timedelta

router = APIRouter(prefix="/login", tags=["Authentication User"])

# Load environment variables
load_dotenv()
api_key = os.getenv("API_KEY_BREVO")

# MQTT setup
mqtt_broker = 'broker.hivemq.com'
mqtt_port = 1883
mqtt_topic = 'user/registration'

client = mqtt.Client()

# Configure Jinja2 for templates
templates_dir = Path(__file__).parent / 'templates'
env = Environment(loader=FileSystemLoader(templates_dir))

SECRET_KEY = "IndexerGenoma2024"
ALGORITHM = "HS256"

def create_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=1)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    logging.info("Token Generado: ", encoded_jwt)
    return encoded_jwt

def send_welcome_email(to_email, user_name):
    # Generate token
    token = create_token({"sub": to_email})
    
    # Get the template
    template = env.get_template('welcome.html')
    
    # Render the template with dynamic data
    html_content = template.render(
        user_name=user_name,
        year=2024,
        company_name="Universidad de Caldas",
        to_email=to_email,
        token=token,
    )
    
    # Brevo API configuration
    url = "https://api.brevo.com/v3/smtp/email"
    
    headers = {
        "accept": "application/json",
        "api-key": api_key,
        "content-type": "application/json"
    }
    
    payload = {
        "sender": {
            "name": "Indexer GENOMA",
            "email": "juliancam24708@gmail.com"  # Cambia esto por tu email verificado en Brevo
        },
        "to": [{
            "email": to_email,
            "name": user_name
        }],
        "subject": "¡Bienvenido!",
        "htmlContent": html_content
    }

    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 201:
            print(f"Email sent successfully to {to_email}")
            return {"success": True, "message": "Email sent successfully"}
        else:
            print(f"Error sending email: {response.text}")
            return {"success": False, "message": f"Error: {response.text}"}
            
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return {"success": False, "message": str(e)}

@router.post("/login")
async def login_user(email: str = Form(...), token: str = Form(...)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("sub") == email:
            return {"message": "Login successful!"}
        else:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# MQTT worker to handle user registration messages
def registration_worker():
    def on_message(client, userdata, message):
        # Process the registration message
        payload = message.payload.decode('utf-8')
        username, email = payload.split(',')
        print(f"Received registration for {username} with email {email}")

    client.message_callback_add(mqtt_topic, on_message)
    client.subscribe(mqtt_topic)
    client.loop_start()  # Start the MQTT client loop

# Connect and keep MQTT active in a separate thread
def start_mqtt():
    client.connect(mqtt_broker, mqtt_port, 60)
    client.loop_forever()

@router.post("/register")
async def register_user(
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    email: str = Form(...)
):
    # Send the welcome email in the background
    background_tasks.add_task(send_welcome_email, email, name)
    
    return {"name": name,
            "message": "Email sent successfully"}

@router.on_event("startup")
async def startup_event():
    try:
        # Start the registration worker in a separate thread to listen for MQTT messages
        threading.Thread(target=registration_worker, daemon=True).start()
        threading.Thread(target=start_mqtt, daemon=True).start()
        logging.info("Registration worker started")
        
    except Exception as e:
        logging.error(f"Error serve MQTT: {str(e)}")
        raise