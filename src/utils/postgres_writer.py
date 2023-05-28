from airflow.hooks.base import BaseHook

connection_id = "postgres"
conn = BaseHook.get_connection(connection_id)
host = conn.host
port = conn.port
username = conn.login
password = conn.password
database = conn.schema

def write_to_postgres(df, table_name):
    """
    Write data to postgres database

    Args:
        df (dataframe): Dataframe to be write to postgres database.
        endpoint (str): Name of the data point.
    """
    return df.to_sql(table_name, f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}', if_exists='append', index=False)