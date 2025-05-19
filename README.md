# Airbnb Data Pipeline
## Project Overview
This project was created as part of a learning module in a data analytics bootcamp, focusing on building an end-to-end data pipeline using PySpark. The pipeline processes the Airbnb dataset and consists of several key components:

- ETL (Extract, Transform, Load): Raw data from Airbnb is extracted, then cleaned and transformed using PySpark to ensure consistency and readiness for use. The transformed data is then loaded for storage.

- Data Validation with Great Expectations (GX): To maintain data quality, the pipeline includes validation using Great Expectations, such as schema checks, null value detection, and data type verification.

- Loading Data to MongoDB: After validation, the clean data is loaded into MongoDB, a NoSQL database, for further analysis or application development.

This pipeline is designed to be scalable and automated, enabling efficient and reliable updates of Airbnb data for various future needs.

## Business Problems
1. Fragmanted Data Across Multiple
2. Slow Access to Crucial Business Insights
3. Lack of Data Quality and Consistency

## Data Validation with Great Expectations
1. The unique_listing_key should be a unique identifier for each listing. If there are duplicates, it could lead to double-counting or misleading analysis.
2. We’re validating that the price column falls within a realistic range. Negative prices are obviously wrong, and overly high prices could be outliers or errors.
3. Ratings must be in a fixed 1–5 scale:
- 5.0 = Excellent
- 4.0 = Good
- 3.0 = Needs Attention
- 2.0 = Poor
- 1.0 = Very Poor
4. The service_fee column should contain only numeric values. If it's stored as text (e.g., "two hundred"), calculations will break.
5. This column should only contain one of three valid statuses:
- verified → identity confirmed
- unconfirmed → not verified
- Unknown → unclear status
6. This checks a logical relationship — ensuring that host_id values are greater than id. This can help verify the order or structure of the dataset.
7. We want to confirm that the dataset contains exactly the columns we expect — no missing, no extra. This ensures compatibility with our analysis pipeline.

## ETL Process
In this project, I built a modular ETL pipeline using PySpark for data handling and MongoDB as the final storage.
The pipeline consists of three main scripts: extract.py, transform.py, and load.py, each responsible for one key stage of the ETL process.

1. In the extract phase, I created a file called extract.py.
This script is responsible for reading the raw Airbnb dataset using PySpark.
To ensure flexibility and reusability, I defined a custom function that returns a Spark DataFrame as output.
This function can be reused later by other parts of the pipeline, particularly the transform.py script.

2. Once the data has been extracted, it moves to the transform.py script.
Here, we use PySpark again to perform data cleaning and transformation, based on the results from the EDA phase we previously conducted in the notebook
For example, we handled missing values and casted data types properly.
The goal of this step is to ensure the data is clean, structured, and ready to be loaded into a database.

3. The final step is the load process, handled by the load.py script.
In this stage, I used the pymongo library to establish a connection to a MongoDB database.

Then, I converted the cleaned Spark DataFrame into a dictionary format that MongoDB accepts and inserted it into the desired collection.
This enables the data to be stored efficiently and accessed later for further analysis or reporting.

## Tools & Libraries
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)  
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)  
![PySpark](https://img.shields.io/badge/PySpark-FDEE21?style=for-the-badge&logo=apache-spark&logoColor=black)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0A4FE3?style=for-the-badge&logo=python&logoColor=white)
