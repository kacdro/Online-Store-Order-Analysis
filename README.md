# Online-Store-Order-Analysis
## 📌 Opis projektu
Projekt symuluje działanie prostego sklepu internetowego.  
Zamówienia są generowane przez **Kafka Producer** w Pythonie i wysyłane do **Apache Kafka**.  
Dane o zamówieniach są następnie analizowane w czasie rzeczywistym przez **Spark Structured Streaming**, który wylicza najpopularniejsze produkty (liczbę zamówień i łączny przychód).

Całość działa w architekturze strumieniowej (real-time).  

---

## ⚙️ Architektura

1. **Kafka (Docker)**  
   - przechowuje zdarzenia (topic `orders`),  
   - wystawia port `9092` dla producenta i konsumentów,  
   - Kafka-UI (na porcie `8080`) pozwala podejrzeć wiadomości i stan klastra.

2. **Kafka Producer (Python)**  
   - generuje losowe zamówienia: `order_id`, `product_id`, `price`, `timestamp`,  
   - dla każdego `product_id` cena jest wylosowana raz przy starcie i pozostaje stała,  
   - wysyła wiadomości JSON do Kafki w topicu `orders`.

3. **Spark Structured Streaming (Python, PySpark)**  
   - pobiera strumień wiadomości z topicu `orders`,  
   - parsuje JSON do tabeli Sparkowej,  
   - grupuje dane po `product_id` i liczy:
     - `order_count` → liczba zamówień,  
     - `total_revenue` → łączny przychód,  
   - wynik wypisywany jest:
     - w konsoli (posortowany wg przychodu malejąco),  
     - do plików Parquet (snapshot overwrite),  
     - do plików CSV (osobne katalogi per batch).

---

## 🚀 Jak uruchomić

### 1. Wymagania
- Docker + Docker Compose  
- Python 3.9+  
- Zainstalowane biblioteki:  
  pip install kafka-python pyspark
- (Windows) pobrany winutils.exe i skonfigurowane HADOOP_HOME → C:\hadoop\bin.

### 2. Start Kafki
Uruchom Kafkę i UI:
	docker-compose up -d

### 3. Producent (symulacja zamówień)
Uruchom producenta:
	python producer.py

### 4. Analiza w Spark
Uruchom skrypt analityczny:
	python stream_totals.py

Wyniki będą pojawiać się w konsoli w batchach (co kilka sekund), np.:
<pre> ``` +----------+-----------+-------------+ |product_id|order_count|total_revenue| +----------+-----------+-------------+ |1004 |8 |1116.24 | |1005 |15 |1071.90 | |1001 |13 |488.15 | |1002 |5 |639.30 | |1003 |8 |218.16 | +----------+-----------+-------------+ ``` </pre>
