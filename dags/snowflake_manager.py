import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

def get_snowflake_connection():
    """Establish a connection to Snowflake and return the connection object."""
    try:
        conn = snowflake.connector.connect(
            user='gjp4812',
            password='Pr@veen30',
            account='bw73961.us-east-2.aws',
            warehouse='COMPUTE_WH',
            database='AIRFLOWDB',
            schema='RAW'
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return None

def create_snowflake_tables(conn):
    """Create necessary tables in Snowflake."""
    try:
        cur = conn.cursor()
        # Create the vaccination table
        cur.execute("""
            CREATE OR REPLACE TABLE VACCINATION_TABLE (
                COUNTRY VARCHAR,
                TOTAL_DAILY_VACCINATIONS FLOAT,
                MEAN_DAILY_VACCINATIONS FLOAT,
                MAX_DAILY_VACCINATIONS FLOAT,
                MIN_DAILY_VACCINATIONS FLOAT
            );
        """)
        print("Table 'VACCINATION_TABLE' created or replaced successfully.")
        cur.close()
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Programming Error during table creation: {e}")
    except Exception as e:
        print(f"An error occurred during table creation: {e}")

def load_to_snowflake(df):
    """Load a DataFrame into Snowflake."""
    try:
        # Establish connection
        conn = get_snowflake_connection()
        if conn is None:
            return

        # Prepare DataFrame for loading
        df.columns = df.columns.str.upper()  # Convert column names to uppercase

        # Load DataFrame into Snowflake
        success, num_chunks, num_rows, output = write_pandas(conn, df, 'VACCINATION_TABLE')

        if success:
            print(f'Successfully loaded {num_rows} rows into Snowflake')
        else:
            print('Failed to load data into Snowflake')

        # Close connection
        conn.close()
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Programming Error during data loading: {e}")
    except Exception as e:
        print(f"An error occurred during data loading: {e}")

# Example usage
if __name__ == "__main__":
    from data_transformer import vaccination_summary  # Import your DataFrame here
    conn = get_snowflake_connection()
    if conn:
        create_snowflake_tables(conn)  # Create the table
        load_to_snowflake(vaccination_summary)  # Load the data
        conn.close()  # Close the connection
