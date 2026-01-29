# Big-Data-Analytics-FINAL-PROJECT
# E-commercebigdata analytics system 

- **NOTE**: For dataset used in this project is found on the branch of **Master**, not **Main** to view it, therefore you should click **Main** on the top-left and choose **Master** it is named as **Dataset.7z**. 
Thanks.

# Project Overview
Modern e-commerce platforms generate different types of data: products, customers, purchases, and browsing sessions. A single database system often struggles to handle all these efficiently.

This project demonstrates how **multiple database technologies** can work together to process and analyze e-commerce data effectively.

I combined:

- **MongoDB** for business data storage,
- **HBase** for large session logs,
- **Apache Spark** for large-scale analytics processing.

The goal is to show how **polyglot persistence** (using multiple databases in one system) improves performance and scalability in real systems.

# Abstract
Modern e-commerce platforms generate heterogeneous data such as product catalogs, customer profiles, purchase transactions, and high-frequency browsing sessions. A single database technology often struggles to efficiently handle all these workloads including flexible document storage, heavy write throughput logs, and large-scale analytics.

This project implements a multi-model big data architecture using MongoDB (document model), Apache HBase (wide-column model), and Apache Spark (distributed batch processing) to store and analyze a synthetic e-commerce dataset.

MongoDB stores business entities and supports aggregation analytics. HBase stores time-ordered session logs and enables fast retrieval through row-key prefix filtering. Spark computes batch analytics including revenue by category, top spenders, and frequently bought-together product pairs.
while an integrated cross-system analytical query combines HBase engagement metrics with MongoDB spending metrics to study the relationship between user engagement and spending. The results demonstrate how polyglot persistence improves scalability, query efficiency, and analytical flexibility in an e-commerce environment.

# Technologies Used
- MongoDB
- HBase
- Apache Spark
- Python
- PySpark

# Repository Structure
Main files in this repository include:

- **data_generator.py** : Generates synthetic e-commerce data.
- **spark_analysis.py** : Runs Spark analytics queries.
- **integrated_query_engagement_vs_spend.py** : Cross-system analytics.
- **plots_from_spark_outputs.py** : Visualization scripts.
- **integratedplots_query.py** :visualization for integrated analytics.


These scripts together build, process, and analyze the dataset across systems.

# Key Analytics Performed
The project computes:

- Revenue by product category
- Top spending customers
- Frequently bought product pairs
- Engagement vs spending analysis
- Customer behavior insights

# Key Contribution
This project shows that:
- Different databases serve different purposes best.
- Combining technologies leads to better performance.
- Big data analytics pipelines benefit from multi-model architectures.

# How to Run
  Below is the workflow:

1. Generate dataset
   using :**python data_generator.py**
2. Load data into databases.

3. Run Spark analytics:
   using:**spark-submit spark_analysis.py**
   
4.Run integrated analytics queries.

# Author
**GAHIGI Robert :101101**  
Big Data Analytics – Final Project

#  Conclusion
The system successfully demonstrates how combining MongoDB, HBase, and Spark can efficiently handle complex e-commerce data analytics tasks. This architecture can scale and adapt to real-world big data challenges.



