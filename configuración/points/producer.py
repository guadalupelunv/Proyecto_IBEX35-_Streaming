from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

# Configuración
TICKS_PER_SECOND = 1
DURATION_MINUTES = 60 

# Precios iniciales
acciones = {
    "ACCIONA": 129.6000,
    "ACCIONA ENERGIA": 16.3700,
    "ACERINOX": 10.3400,
    "ACS": 57.2500,
    "AENA": 229.8000,
    "AMADEUS": 70.3200,
    "ARCELORMITTAL": 26.9100,
    "BANCO SANTANDER": 6.3840,
    "BANCO DE SABADELL": 2.5440,
    "BANKINTER": 10.5200,
    "BBVA": 12.4400,
    "CAIXABANK": 6.8540,
    "CELLNEX": 35.5200,
    "ENAGAS": 13.5900,
    "ENDESA": 26.3200,
    "FERROVIAL SE": 43.6000,
    "FLUIDRA": 20.9800,
    "GRIFOLS CL. A": 8.5460,
    "IAG": 3.2950,
    "IBERDROLA": 15.9350,
    "INDITEX": 47.7700,
    "INDRA A": 29.6200,
    "INMOBILIARIA COLONIAL": 5.8200,
    "LOGISTA": 30.7200,
    "MAPFRE": 3.2180,
    "MERLIN": 10.1900,
    "NATURGY": 26.1600,
    "PUIG BRANDS": 16.9200,
    "REDEIA": 17.8300,
    "REPSOL": 10.8450,
    "ROVI": 52.3500,
    "SACYR": 3.2800,
    "SOLARIA": 6.5140,
    "TELEFONICA": 4.4730,
    "UNICAJA": 1.7460
}

def variar_precio(precio):
    variacion = random.uniform(-0.0025, 0.0025)
    return round(precio * (1 + variacion), 4)

def main():
    producer = KafkaProducer(
        bootstrap_servers='192.168.67.10:9094',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    end_time = datetime.utcnow() + timedelta(minutes=DURATION_MINUTES)

    while datetime.utcnow() < end_time:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for nombre, precio in acciones.items():
            nuevo_precio = variar_precio(precio)
            acciones[nombre] = nuevo_precio
            data = {
                "timestamp": timestamp,
                "company": nombre,
                "points": nuevo_precio
            }
            producer.send('ibex35', value=data)
            print(f"Sent: {data}")
        time.sleep(1 / TICKS_PER_SECOND)

if __name__ == "__main__":
    main()