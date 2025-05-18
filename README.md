# Airbnb_Data-Pipeline

This project was developed as part of a data analytics bootcamp, focusing on building an end-to-end data pipeline using PySpark. The pipeline processes Airbnb datasets and includes the following key components:

ETL (Extract, Transform, Load): Raw Airbnb data is extracted, cleaned, and transformed using PySpark to ensure consistency and usability. The processed data is then loaded for storage.

Data Validation with Great Expectations: To maintain high data quality, the pipeline integrates Great Expectations (GX) for validating schema, null values, and data types before loading.

Data Loading to MongoDB: After validation, the clean data is stored in MongoDB, a NoSQL database, to support further analysis or application development.

This pipeline is designed to be scalable and automatable, enabling seamless updates to Airbnb data and ensuring reliability for future use cases.

