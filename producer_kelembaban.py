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
        # Kirim data kelembaban untuk setiap gudang
        for gudang_id in gudang_ids:
            # Buat data kelembaban acak antara 60% dan 80%
            kelembaban = round(random.uniform(60.0, 80.0), 1)
            
            # Buat pesan
            data_kelembaban = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban
            }
            
            # Kirim data ke topik kelembaban
            producer.send('sensor-kelembaban-gudang', data_kelembaban)
            print(f"[KELEMBABAN] Mengirim: {data_kelembaban}")
            
        # Tunggu 1 detik
        producer.flush()
        time.sleep(1)
        
except KeyboardInterrupt:
    print("Producer kelembaban dihentikan")
finally:
    producer.close()