import json
import time
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta

# Konfigurasi consumer
consumer_suhu = KafkaConsumer(
    'sensor-suhu-gudang',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='monitoring-group'
)

consumer_kelembaban = KafkaConsumer(
    'sensor-kelembaban-gudang',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='monitoring-group'
)

# Data gudang untuk menyimpan status terbaru (dalam memori)
data_gudang = defaultdict(lambda: {'suhu': None, 'kelembaban': None, 'timestamp_suhu': None, 'timestamp_kelembaban': None})

# Function untuk mencetak status gudang
def cetak_status_gudang(gudang_id, data):
    suhu = data['suhu']
    kelembaban = data['kelembaban']
    
    if suhu is None or kelembaban is None:
        return  # Jika belum ada data lengkap, jangan cetak status
    
    # Cek kondisi bahaya
    status = "Aman"
    suhu_tinggi = suhu > 80
    kelembaban_tinggi = kelembaban > 70
    
    if suhu_tinggi and kelembaban_tinggi:
        status = "Bahaya tinggi! Barang berisiko rusak"
    elif suhu_tinggi:
        status = "Suhu tinggi, kelembaban normal"
    elif kelembaban_tinggi:
        status = "Kelembaban tinggi, suhu aman"
    
    # Cetak status terbaru setiap gudang
    print(f"\n[LAPORAN STATUS] Gudang {gudang_id}:")
    print(f"- Suhu: {suhu}°C")
    print(f"- Kelembaban: {kelembaban}%")
    print(f"- Status: {status}")
    
    # Cetak peringatan jika kondisi melebihi ambang batas
    if suhu_tinggi:
        print(f"[Peringatan Suhu Tinggi] Gudang {gudang_id}: Suhu {suhu}°C")
    if kelembaban_tinggi:
        print(f"[Peringatan Kelembaban Tinggi] Gudang {gudang_id}: Kelembaban {kelembaban}%")
    
    # Cetak peringatan kritis jika kedua kondisi melebihi ambang batas
    if suhu_tinggi and kelembaban_tinggi:
        print(f"[PERINGATAN KRITIS] Gudang {gudang_id}:")
        print(f"- Suhu: {suhu}°C")
        print(f"- Kelembaban: {kelembaban}%")
        print(f"- Status: {status}")

# Fungsi untuk polling data dari consumer
def poll_and_process(consumer, is_suhu=True):
    msg_pack = consumer.poll(timeout_ms=1000)
    current_time = datetime.now()
    
    for tp, messages in msg_pack.items():
        for message in messages:
            data = message.value
            gudang_id = data['gudang_id']
            
            if is_suhu:
                data_gudang[gudang_id]['suhu'] = data['suhu']
                data_gudang[gudang_id]['timestamp_suhu'] = current_time
            else:
                data_gudang[gudang_id]['kelembaban'] = data['kelembaban']
                data_gudang[gudang_id]['timestamp_kelembaban'] = current_time
            
            # Jika data di gudang yang sama ada dan masih dalam window 10 detik,
            # lakukan analisis dan cetak status
            ts_suhu = data_gudang[gudang_id]['timestamp_suhu']
            ts_kelembaban = data_gudang[gudang_id]['timestamp_kelembaban']
            
            if ts_suhu and ts_kelembaban:
                time_diff = abs((ts_suhu - ts_kelembaban).total_seconds())
                # Jika data kelembaban dan suhu berada dalam window waktu 10 detik
                if time_diff <= 10:
                    cetak_status_gudang(gudang_id, data_gudang[gudang_id])

try:
    print("Mulai memantau gudang. Tekan Ctrl+C untuk keluar.")
    while True:
        # Poll dan proses data suhu
        poll_and_process(consumer_suhu, is_suhu=True)
        
        # Poll dan proses data kelembaban
        poll_and_process(consumer_kelembaban, is_suhu=False)
        
        # Tunggu sedikit sebelum polling lagi
        time.sleep(0.1)
        
except KeyboardInterrupt:
    print("Sistem monitoring dihentikan")
finally:
    consumer_suhu.close()
    consumer_kelembaban.close()