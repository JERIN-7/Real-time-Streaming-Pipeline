# **IntraDay Sales – Real-Time Data Engineering Pipeline**

This project demonstrates a **complete end-to-end real-time data engineering pipeline**. It captures streaming data via **Apache Kafka**, processes it using **Apache Spark**, and stores the transformed data in **Cassandra** for efficient querying. The workflow is orchestrated with **Apache Airflow**, while **Superset** provides real-time visualization and analytics.

All components are fully containerized with **Docker**, ensuring **seamless deployment, scalability, and portability** across environments.

---

## **Project Architecture**

<img width="1536" height="1024" alt="ChatGPT Image Sep 28, 2025, 05_40_23 PM" src="https://github.com/user-attachments/assets/5005bffb-df3b-4dbb-8b9b-d256991c76b5" />


The pipeline flow can be summarized as:

1. **Data Source:** Real-time streaming data ingested via **Apache Kafka**.  
2. **Apache Kafka & Zookeeper:** Handle **real-time streaming**, ensuring smooth coordination between producers and consumers.  
3. **Control Center & Schema Registry:** Monitor Kafka streams and manage message schemas effectively.  
4. **Apache Spark:** Performs **real-time processing**, transformations, and aggregations on the streaming data.  
5. **Cassandra:** Stores processed data for **fast and scalable querying**.  
6. **Superset:** Visualizes the processed data through **interactive dashboards**, enabling real-time insights.

---

## **Technologies Used**

| Technology | Purpose |
|------------|---------|
| **Python** | Programming language for ETL scripts, Spark jobs, and Kafka producers/consumers |
| **Apache Kafka** | Real-time data streaming |
| **Apache Zookeeper** | Kafka cluster coordination |
| **Control Center & Schema Registry** | Kafka monitoring & schema management |
| **Apache Spark** | Distributed data processing |
| **Cassandra** | NoSQL database for fast analytics |
| **PostgreSQL** | Storage for raw or historical data |
| **Superset** | Interactive dashboards and visual analytics |
| **Docker** | Containerization for consistent deployment |

---


## **Real-Time Analysis**

The pipeline enables **minute-level sales analysis** and real-time dashboards. Below is a sample visualization of sales over minutes:

<img width="1833" height="841" alt="Screenshot 2025-09-28 171226" src="https://github.com/user-attachments/assets/60f8b688-6ec2-4099-83b5-9ec2f5de2478" />


---

## **Key Highlights**

- Fully containerized stack ensures **portability and reproducibility**.  
- Real-time streaming and processing pipeline with **low latency**.  
- Seamless integration of **Airflow, Kafka, Spark, Cassandra, and Superset**.  
- **Scalable architecture** suitable for large-scale streaming data.

---


