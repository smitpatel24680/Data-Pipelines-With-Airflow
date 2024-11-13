
# Airflow DAG for Loading and Transforming Data in Redshift

## Overview

This project contains an Airflow Directed Acyclic Graph (DAG) that loads and transforms data from Amazon S3 into Amazon Redshift. The data is divided into event and song datasets, which are staged in Redshift, then used to populate a fact table (`factSongPlay`) and several dimension tables (`dimUser`, `dimSong`, `dimArtist`, `dimTime`). The project also includes data quality checks for each table to ensure the integrity of the data after it’s loaded into Redshift.

## Features

- **Stage Data**: Loads data from S3 into staging tables in Redshift.
- **Load Fact Table**: Loads transformed data into a fact table (`factSongPlay`).
- **Load Dimension Tables**: Loads data into several dimension tables (`dimUser`, `dimSong`, `dimArtist`, `dimTime`).
- **Data Quality Checks**: Ensures that each table has the correct number of records after loading.

## Technologies Used

- **Apache Airflow**: Orchestrates the DAG for ETL operations.
- **Amazon S3**: Stores raw event and song data.
- **Amazon Redshift**: Data warehouse where data is loaded.
- **Python**: The language used to write the custom operators and DAG.
- **SQL**: Used for querying and loading data into Redshift.

## Folder Structure

```
project/
├── dags/
│   └── udac_example_dag.py          # The main DAG file
├── operators/
│   ├── stage_redshift.py            # Custom operator to stage data to Redshift
│   ├── load_fact_operator.py       # Custom operator to load data into the fact table
│   ├── load_dimension_operator.py  # Custom operator to load data into dimension tables
├── helpers/
│   └── sql_queries.py              # SQL queries for loading tables
└── requirements.txt                # List of project dependencies
```

## Requirements

You can install the required dependencies using `pip`:

```bash
pip install -r requirements.txt
```

## Setup Instructions

1. **Airflow Setup**: 
   - Install Apache Airflow and set it up on your local or cloud environment.
   - Ensure that Airflow is configured with connections to Amazon S3 and Redshift (via Airflow UI).
   
2. **AWS Credentials**: 
   - Make sure that your AWS credentials are stored in Airflow's connection settings or as environment variables for authentication.
   - You can store the AWS credentials in Airflow using `aws_credentials` connection ID for accessing S3.

3. **Redshift Setup**: 
   - You should have a Redshift cluster set up and accessible with appropriate credentials.
   - You’ll also need to create the necessary tables in your Redshift cluster, as defined in the SQL queries provided in `sql_queries.py`.

4. **Running the DAG**:
   - Once the setup is complete, you can trigger the DAG either manually or let it run on a scheduled interval (as defined in the `schedule_interval` parameter of the DAG).

   ```bash
   airflow dags trigger udac_example_dag
   ```

## DAG Flow

1. **Stage Events and Songs**: 
   - The event data is staged from `log-data/2018/11/` in S3 into the `staging_events` table in Redshift.
   - The song data is staged from `song-data/A/A/A/` in S3 into the `staging_songs` table in Redshift.

2. **Load Fact Table**:
   - The `Load_songplays_fact_table` operator loads data from the staging tables into the fact table `factSongPlay`.

3. **Load Dimension Tables**:
   - The dimension tables `dimUser`, `dimSong`, `dimArtist`, and `dimTime` are loaded using `LoadDimensionOperator` based on data from the fact table.

4. **Data Quality Checks**:
   - After loading the tables, the `HasRowsOperator` checks that each table contains data. If a table has zero rows, the task will fail and raise an error.

5. **End of Execution**:
   - The DAG concludes with the `Stop_execution` dummy operator.

## SQL Queries

The SQL queries to load the data into Redshift tables are located in the `helpers/sql_queries.py` file. The queries are written to:
- Insert data into the fact table.
- Insert data into dimension tables, ensuring proper handling of duplicate data.

## Configuration

The DAG's configuration includes default parameters for retries, scheduling, and task dependencies:

- **Retries**: Each task retries 3 times on failure with a 5-minute delay.
- **Catchup**: Catchup is turned off, so the DAG only runs on the most recent schedule.
- **Email on failure**: No email notifications will be sent on task retries or failure.
- **Start Date**: The DAG starts running from January 12, 2019.

## Custom Operators

- **`StageToRedshiftOperator`**: Loads raw data from S3 to Redshift staging tables.
- **`LoadFactOperator`**: Loads data from staging tables into the fact table.
- **`LoadDimensionOperator`**: Loads data from the fact table into dimension tables.
- **`HasRowsOperator`**: Performs data quality checks on the loaded tables.

## Conclusion

This project demonstrates how to create an Airflow DAG for performing ETL operations on data stored in S3 and loading it into Redshift. The use of custom operators and data quality checks ensures that the data is properly staged and transformed, and that the tables meet quality standards.
