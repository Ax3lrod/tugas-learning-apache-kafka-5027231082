import json
import time
import random
from kafka import KafkaProducer

# Konfigurasi producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Daftar ID gudang
gudang_ids = ["G1", "G2", "G3", "G4"]

try:
    while True:
        # Kirim data suhu untuk setiap gudang
        for gudang_id in gudang_ids:
            # Buat data suhu acak antara 70°C dan 90°C
            suhu = round(random.uniform(70.0, 90.0), 1)
            
            # Buat pesan
            data_suhu = {
                "gudang_id": gudang_id,
                "suhu": suhu
            }
            
            # Kirim data ke topik suhu
            producer.send('sensor-suhu-gudang', data_suhu)
            print(f"[SUHU] Mengirim: {data_suhu}")
            
        # Tunggu 1 detik
        producer.flush()
        time.sleep(1)
        
except KeyboardInterrupt:
    print("Producer suhu dihentikan")
finally:
    producer.close()