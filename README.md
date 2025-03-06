# Kafka Producer-Consumer Project

## Overview

This project demonstrates a simple **Kafka Producer-Consumer** implementation using Python. The producer sends stock market data to a Kafka topic, while the consumer reads messages and stores them in **Amazon S3**. The data stored in S3 is then analyzed using **AWS Glue Crawler** and **Amazon Athena**.


## Data Pipeline Architecture
![Kafka Data Pipeline](https://github.com/ShreemoyN97/Kafka_Stock_Market_Project/blob/main/Kafka-Pipeline.png)

## Prerequisites

Ensure you have the following installed on your **EC2 instance**:

- **Java 17** (Amazon Corretto)
- **Apache Kafka 3.9.0**
- **Python 3** with necessary libraries (`kafka-python`, `s3fs`)
- **AWS Glue** setup for data crawling
- **Amazon Athena** for querying the data

## Installation & Setup

### **1. Download & Extract Kafka**

```bash
wget https://downloads.apache.org/kafka/3.9.0/kafka_2.12-3.9.0.tgz
tar -xvf kafka_2.12-3.9.0.tgz
cd kafka_2.12-3.9.0
```

### **2. Install Java**

```bash
sudo dnf install java-17-amazon-corretto -y
java -version  # Verify installation
```

### **3. Start Zookeeper**

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### **4. Start Kafka Broker**

```bash
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
bin/kafka-server-start.sh config/server.properties
```

**Note:** Edit `config/server.properties` to set `advertised.listeners` to your **public EC2 IP**.

### **5. Create a Kafka Topic**

```bash
bin/kafka-topics.sh --create --topic demo_testing2 \
  --bootstrap-server <Your-EC2-Public-IP>:9092 \
  --replication-factor 1 --partitions 1
```

## Running Producer & Consumer

### **Start Producer**

```bash
bin/kafka-console-producer.sh --topic demo_testing2 \
  --bootstrap-server <Your-EC2-Public-IP>:9092
```

### **Start Consumer**

```bash
bin/kafka-console-consumer.sh --topic demo_testing2 \
  --bootstrap-server <Your-EC2-Public-IP>:9092
```

## Python Producer & Consumer

### **Install Dependencies**

```bash
pip install kafka-python s3fs
```

### **Run Kafka Producer**

```bash
python Kafka-Producer.ipynb
```

### **Run Kafka Consumer**

```bash
python Kafka-Consumer.ipynb
```

## **How to Store Kafka Messages in S3**

The **consumer** reads messages and saves them in an S3 bucket:

```python
with s3.open("s3://sn-kafka-stock-market-project/stock_market_{count}.json", 'w') as file:
    json.dump(i.value, file)
```

## **Processing Data with AWS Glue & Athena**

### **1. Create a Glue Crawler**
- Navigate to the **AWS Glue Console**.
- Create a **new Glue Crawler**.
- Set the **data source** to the S3 bucket where Kafka messages are stored.
- Define a **database and table** to store metadata.
- Run the crawler to populate the Glue Data Catalog.

### **2. Query Data in Amazon Athena**
- Navigate to the **AWS Athena Console**.
- Select the Glue Database where the crawler stored metadata.
- Run SQL queries to analyze stock market data, e.g.,:
  
  ```sql
  SELECT * FROM stock_market_data LIMIT 10;
  ```

## Troubleshooting

- **Kafka Broker Timeout:** Ensure port **9092** is open in your AWS **Security Group**.
- **Consumer Not Receiving Messages:** Check topic existence with:
  ```bash
  bin/kafka-topics.sh --list --bootstrap-server <Your-EC2-Public-IP>:9092
  ```
- **Memory Issues:** Reduce Kafka memory usage:
  ```bash
  export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
  ```

## License

This project is open-source under the **MIT License**.
