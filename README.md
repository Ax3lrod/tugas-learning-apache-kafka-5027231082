# Langkah-Langkah Pengerjaan

1. **Jalankan Kafka Container**:

   ```bash
   docker run -d --name=kafka -p 9092:9092 apache/kafka
   ```

   Kontainer ini sudah termasuk Zookeeper dan Kafka broker.

2. **Verifikasi Cluster**:

   ```bash
   docker exec -ti kafka /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server :9092
   ```

   Contoh output:

   ```
   Cluster ID: 5L6g3nShT-eMCtK--X86sw
   ```

3. **Buat Topik Kafka**:

   ```bash
   # Topik untuk sensor suhu
   docker exec -ti kafka /opt/kafka/bin/kafka-topics.sh \
     --create --topic sensor-suhu-gudang \
     --bootstrap-server :9092 --replication-factor 1 --partitions 3

   # Topik untuk sensor kelembaban
   docker exec -ti kafka /opt/kafka/bin/kafka-topics.sh \
     --create --topic sensor-kelembaban-gudang \
     --bootstrap-server :9092 --replication-factor 1 --partitions 3
   ```
4. **Install Dependencies**

```bash
pip install kafka-python
```

5. **Jalankan Tiap Proses di Tab/Window Terpisah**:

   * Buka beberapa PowerShell windows.
   * Di window 1, mulai kontainer Kafka:

     ```powershell
     docker run -d --name=kafka -p 9092:9092 apache/kafka
     ```
   * Di window 2, jalankan producer suhu:

     ```powershell
     python producer_suhu.py
     ```
   * Di window 3, jalankan producer kelembaban:

     ```powershell
     python producer_kelembaban.py
     ```
   * Di window 4, jalankan stream monitor:

     ```powershell
     python stream_monitor.py
     ```