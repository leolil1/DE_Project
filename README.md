# DE_Project

This is a simple data engineering project designed to showcase the basic ETL workflow. The data source is Bitcoin historical data in 1-minute intervals. The final output includes four Bitcoin price charts—daily, monthly, quarterly, and yearly- visualized in a mixture of candlesticks and line graphs. 

The workflow consists of __Extraction__ using Airflow, __Loading__ data into Google Cloud Storage data lake, __Transforming__ data with dbt, storing the final results in BigQuery data warehouse, and finally visualizing the data with BI tool Looker Studio

![image](https://github.com/user-attachments/assets/9a04b995-0ca9-4596-bd6a-812d4cd59502)



![image](https://github.com/user-attachments/assets/eb9e80df-1b7d-48b1-8dae-6a5563a47a54)
