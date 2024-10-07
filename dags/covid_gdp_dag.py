from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import os 
import pandas as pd
from common_data_loader import download_world_bank_data, download_kaggle_dataset, load_covid_data, load_gdp_data
from data_transformer import calculate_top_vaccination_rates, calculate_daily_vaccination_change, compare_income_groups
from snowflake_loader import create_snowflake_tables, load_to_snowflake


os.environ['KAGGLE_CONFIG_DIR'] = 'C:/Users/gjpra/.kaggle'
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
with DAG(
    'covid_gdp_pipeline',
    default_args=default_args,
    description='A pipeline to process and load COVID vaccination and GDP data',
    schedule_interval='@daily',  # Runs daily, adjust according to your needs
    start_date=days_ago(1),  # Start one day ago, adjust if needed
    catchup=False
) as dag:

    # 1. Task: Ingest COVID-19 Data
    def ingest_covid_data():
        download_kaggle_dataset()
        load_covid_data('D:\DEprojects\Assignment-Airflow\country_vaccinations.csv', 'D:\DEprojects\Assignment-Airflow\country_vaccinations_by_manufacturer.csv')

    ingest_covid_task = PythonOperator(
        task_id='ingest_covid_data',
        python_callable=ingest_covid_data
    )

    # 2. Task: Ingest GDP Data
    def ingest_gdp_data():
        download_world_bank_data()
        load_gdp_data('D:\DEprojects\Assignment-Airflow\gdp_per_capita_world_bank.csv')

    ingest_gdp_task = PythonOperator(
        task_id='ingest_gdp_data',
        python_callable=ingest_gdp_data
    )

    # 3. TaskGroup for data transformations
    with TaskGroup('data_transformation', tooltip="Transformations") as transform_group:
        
        # # Task: Calculate top vaccination rates
        # def transform_top_vaccination_rates():
        #     df1 = pd.read_csv('path_to_vaccinations.csv')
        #     top_10 = calculate_top_vaccination_rates(df1)
        #     top_10.to_csv('top_10_vaccination_countries.csv', index=False)
        
        # top_vaccination_task = PythonOperator(
        #     task_id='calculate_top_vaccination_rates',
        #     python_callable=transform_top_vaccination_rates
        # )

        # Task: Calculate daily vaccination changes
        def transform_daily_vaccination_change():
            df1 = pd.read_csv('path_to_vaccinations.csv')
            vaccination_summary = calculate_daily_vaccination_change(df1)
            vaccination_summary.to_csv('vaccination_summary.csv', index=False)

        daily_vaccination_task = PythonOperator(
            task_id='calculate_daily_vaccination_changes',
            python_callable=transform_daily_vaccination_change
        )

        # # Task: Compare vaccination rates between income groups
        # def transform_income_group_comparison():
        #     df1 = pd.read_csv('path_to_vaccinations.csv')
        #     gdp_data = pd.read_csv('path_to_gdp.csv')
        #     compare_income_groups(df1, gdp_data)

        # compare_income_task = PythonOperator(
        #     task_id='compare_vaccination_income_groups',
        #     python_callable=transform_income_group_comparison
        # )

        # Independent transformation tasks
        [daily_vaccination_task]

    # 4. Task: Create Snowflake tables
    def create_snowflake_table():
        conn = create_snowflake_tables()

    create_snowflake_task = PythonOperator(
        task_id='create_snowflake_table',
        python_callable=create_snowflake_table
    )

    # 5. Task: Load data into Snowflake
    def load_data_to_snowflake():
        df = pd.read_csv('vaccination_summary.csv')
        load_to_snowflake(df)

    load_to_snowflake_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_data_to_snowflake
    )

    # Setting task dependencies
    # Ingest tasks
    [ingest_covid_task, ingest_gdp_task] >> transform_group

    # Transformation tasks (parallel) >> Snowflake table creation >> Data loading
    transform_group >> create_snowflake_task >> load_to_snowflake_task
