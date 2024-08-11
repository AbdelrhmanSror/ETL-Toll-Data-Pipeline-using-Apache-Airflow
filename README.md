# ETL Toll Data Pipeline using Apache Airflow

This project showcases an ETL (Extract, Transform, Load) pipeline built with Apache Airflow to process toll data from multiple sources. The pipeline is designed to download, extract, transform, and consolidate data from CSV, TSV, and fixed-width formats into a unified dataset, with a final transformation step that converts a specific column to uppercase.

## Project Overview

The pipeline consists of the following steps:

1. **Download and Unzip Data:** 
   - Downloads a compressed toll data file from a specified URL and extracts it to a designated directory.

2. **Extract Data:**
   - Extracts relevant columns from the CSV, TSV, and fixed-width files.
   - The extracted data is saved as separate CSV files.

3. **Consolidate Data:**
   - Combines the extracted data from the CSV, TSV, and fixed-width files into a single CSV file.

4. **Transform Data:**
   - Transforms the consolidated data by converting the fourth column to uppercase.

## DAG Configuration

- **DAG ID:** `ETL_toll_data`
- **Schedule:** Runs daily
- **Owner:** Sror
- **Email Notifications:** Sent on failure and retry to `abdelrahmanasror@gmail.com`
- **Retry Policy:** Retries once after 5 minutes on failure

## Prerequisites

- Apache Airflow
- Basic knowledge of bash commands and DAG creation in Airflow

## How to Run

1. **Set Up Apache Airflow:**
   - Follow the [Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html) to set up and start the Airflow web server and scheduler.

2. **Create Necessary Directories and Set Permissions:**

   Run the following commands to create the required directories and set the appropriate permissions:

   ```sh
   sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
   sudo chmod -R 777 /home/project/airflow/dags/finalassignment
   ```

3. **Place the DAG file:**
   - Copy `ETL_toll_data.py` to your Airflow `dags` directory.

4. **Trigger the DAG:**
   - In the Airflow web interface, locate the `ETL_toll_data` DAG and trigger it manually or wait for the scheduled interval.

## Author

Abdelrahman Sror - [abdelrahmanasror@gmail.com](mailto:abdelrahmanasror@gmail.com)

Feel free to reach out for any questions or further assistance!
