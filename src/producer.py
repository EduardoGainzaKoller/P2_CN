import boto3
import json
import time
import random
from datetime import datetime
from loguru import logger

# --- CONFIGURACIÓN ---
STREAM_NAME = "EventDataStream" 
REGION = "us-east-1"

# Configuración de Loguru para guardar logs en un archivo además de la consola
logger.add("logs/producer_{time}.log", rotation="10 MB", level="INFO")

def generate_event():
    """Genera un evento de transacción simulado."""
    event_types = ['PURCHASE', 'LOGIN', 'LOGOUT', 'ADD_TO_CART', 'CLICK']
    
    return {
        "event_id": str(random.randint(10000, 99999)),
        "event_type": random.choice(event_types),
        "user_id": f"USER_{random.randint(1, 100)}",
        "timestamp": datetime.now().isoformat(),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "currency": "EUR"
    }

def run_producer():
    
    try:
        kinesis_client = boto3.client('kinesis', region_name=REGION)
        logger.info(f"Conectado a Kinesis en la región {REGION}. Stream: {STREAM_NAME}")
    except Exception as e:
        logger.error(f"Error al conectar con AWS: {e}")
        return

    logger.info("Iniciando envío de datos...")
    
    try:
        while True:
            
            data = generate_event()
            
            
            response = kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(data),
                PartitionKey=data['user_id'] 
            )
            
            logger.success(f"Evento enviado: {data['event_id']} - Tipo: {data['event_type']} - ShardId: {response['ShardId']}")
            
            # Aqui esperamos un poco para no saturar el stream, que si no lo vamos a petar 
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        logger.warning("Productor detenido.")
    except Exception as e:
        logger.critical(f"Error inesperado: {e}")

if __name__ == "__main__":
    run_producer()