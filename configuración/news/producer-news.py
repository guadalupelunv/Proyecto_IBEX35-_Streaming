from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

INTERVAL_SECONDS = 5

empresas = [
    "ACCIONA", "ACCIONA ENERGIA", "ACERINOX", "ACS", "AENA", "AMADEUS", "ARCELORMITTAL",
    "BANCO SANTANDER", "BANCO DE SABADELL", "BANKINTER", "BBVA", "CAIXABANK", "CELLNEX",
    "ENAGAS", "ENDESA", "FERROVIAL SE", "FLUIDRA", "GRIFOLS CL. A", "IAG", "IBERDROLA",
    "INDITEX", "INDRA A", "INMOBILIARIA COLONIAL", "LOGISTA", "MAPFRE", "MERLIN", "NATURGY",
    "PUIG BRANDS", "REDEIA", "REPSOL", "ROVI", "SACYR", "SOLARIA", "TELEFONICA", "UNICAJA"
]

# Noticias por tipo (solo se reemplaza {empresa})
noticias_positivas = [
    "{empresa} registra beneficios récord en el último trimestre.",
    "Las acciones de {empresa} suben tras anunciar una expansión internacional.",
    "{empresa} sorprende al mercado con un nuevo producto innovador.",
    "{empresa} recibe una mejora en su calificación crediticia.",
    "El mercado reacciona positivamente al anuncio de dividendos de {empresa}.",
    "{empresa} lidera el sector con su estrategia de sostenibilidad.",
    "{empresa} anuncia un acuerdo estratégico con una multinacional tecnológica.",
    "{empresa} supera las previsiones de los analistas por segundo trimestre consecutivo.",
    "Fuerte demanda impulsa los resultados de {empresa}.",
    "{empresa} es reconocida como una de las empresas más sostenibles del índice."
]

noticias_neutras = [
    "{empresa} mantiene sus previsiones para el próximo trimestre.",
    "Sin cambios significativos en las operaciones de {empresa} según su último informe.",
    "{empresa} ha presentado su plan estratégico para los próximos 3 años.",
    "{empresa} celebra su junta anual sin novedades destacables.",
    "{empresa} continúa con su actividad en línea con el mercado.",
    "El informe semestral de {empresa} no muestra sorpresas para los inversores.",
    "{empresa} anuncia cambios menores en su estructura organizativa.",
    "{empresa} se mantiene estable en un entorno económico incierto.",
    "{empresa} actualiza sus políticas internas sin impacto en sus operaciones.",
    "{empresa} ha comunicado resultados dentro de lo esperado por los analistas."
]

noticias_negativas = [
    "{empresa} sufre una caída del 10% tras resultados peores de lo esperado.",
    "Problemas regulatorios afectan a {empresa} en varios mercados.",
    "{empresa} anuncia recortes de personal ante la disminución de ingresos.",
    "Los analistas rebajan la recomendación de {empresa} tras un trimestre débil.",
    "La incertidumbre económica impacta en los ingresos de {empresa}.",
    "{empresa} es investigada por presuntas prácticas irregulares.",
    "Fallo técnico paraliza temporalmente la actividad principal de {empresa}.",
    "Los costes operativos lastran los beneficios de {empresa}.",
    "{empresa} pierde cuota de mercado frente a sus principales competidores.",
    "El mercado reacciona negativamente a la emisión de deuda de {empresa}."
]

def generar_noticia():
    empresa = random.choice(empresas)
    tipo = random.choices(["positiva", "neutra", "negativa"], weights=[0.3, 0.4, 0.3])[0]

    if tipo == "positiva":
        contenido = random.choice(noticias_positivas).format(empresa=empresa)
        scoring = round(random.uniform(0.5, 1.0), 2)
    elif tipo == "neutra":
        contenido = random.choice(noticias_neutras).format(empresa=empresa)
        scoring = round(random.uniform(-0.2, 0.2), 2)
    else:
        contenido = random.choice(noticias_negativas).format(empresa=empresa)
        scoring = round(random.uniform(-1.0, -0.5), 2)

    return {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "company": empresa,
        "content": contenido,
        "scoring": scoring
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='192.168.67.10:9094',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        while True:
            noticia = generar_noticia()
            producer.send('ibex35-news', value=noticia)
            print(f"Sent: {noticia}")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Generador detenido.")

if __name__ == "__main__":
    main()
