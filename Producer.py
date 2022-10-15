from confluent_kafka import Producer
import csv, avro.schema
from avro.io import DatumReader, DatumWriter
import requests, json, os, io
from urllib.request import urlopen

cluster_hostname = os.environ['cluster_hostname']
cluster_port = '24733'
schema_port = '24736'
cluster_username = os.environ['cluster_username']
cluster_password = os.environ['cluster_password']
topic = os.environ['topic']
avro_schema = os.environ['avro_schema']
SOURCE_FILE_NAME = os.environ['SOURCE_FILE_NAME']

def delivery_report(err, msg):
    if err:
        if str(type(err).__name__) == "KafkaError":
            print(f"Message failed with error : {str(err)}")
            print(f"Message Retry? :: {err.retriable()}")
        else:
            print(f"Message failed with error : {str(err)}")
    else:
        print(f"Message delivered to partition {msg.partition()}; Offset Value - {msg.offset()}")
        print(f"{msg.value()}")

def download_schema(schema_url):
    response = requests.get(schema_url)
    schema_obj = json.loads(response.text)
    return schema_obj['schema']
	
def csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(unicode_csv_data)
    for row in csv_reader:
        yield [cell for cell in row]

def run_producer():
    p = Producer({'bootstrap.servers': f"{cluster_hostname}:{cluster_port}",
                  'security.protocol':'ssl',
                  'ssl.ca.location':'./ca.pem','ssl.certificate.location':'./service.cert','ssl.key.location':'./service.key',
                  'acks':'-1','partitioner':'consistent_random','batch.num.messages':'1000',
                  'linger.ms':'100',
                  'queue.buffering.max.messages':'10000'})
    # Get the avro schema to be used for encoding
    schema_url = f"https://{cluster_username}:{cluster_password}@{cluster_hostname}:{schema_port}/subjects/{avro_schema}/versions/latest"
    schema_obj = download_schema(schema_url)
    
    schema = avro.schema.parse(schema_obj)
    writer = avro.io.DatumWriter(schema)
    
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    msg_header = {"source" : b'DEM'}
    
    with open(SOURCE_FILE_NAME, 'r') as csvfile:
        reader = csv_reader(csvfile, delimiter=',')
        for row in reader:
            try:
                avro_data = {"car": row[0],
    			"mpg": float(row[1]),
    			"cylinders": int(row[2]),
    			"displacement": float(row[3]),
    			"horsepower": float(row[4]),
    			"weight": float(row[5]),
    			"acceleration": float(row[6]),
    			"model": int(row[7]),
    			"origin": row[8]
				}
                writer.write(avro_data, encoder)
                p.poll(timeout=0)
                p.produce(topic=topic, value=bytes_writer.getvalue(), headers=msg_header, on_delivery=delivery_report)
            except IndexError:
                print ("Bad record, skip.")
			
        p.flush()

if __name__ == "__main__":
    run_producer()