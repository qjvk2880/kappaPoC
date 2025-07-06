import pandas as pd
from kafka import KafkaProducer
import json

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# txt 파일 읽기
df = pd.read_csv('data/서울특별시.txt', sep='|')

# 각 row를 produce
for _, row in df.iterrows():
    data = row.to_dict()
    producer.send('real-estate-topic', data)

producer.flush()
print("✅ All data produced to Kafka.")