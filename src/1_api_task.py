from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from utils.setup_logger import setup_logging
from utils.postgres_writer import write_to_postgres
import pandas as pd
import requests
from datetime import datetime, timedelta
import json

# Set up logging
logger = setup_logging()

# Define callback functions
def on_success_task(dict):
    """Log a success message when a task succeeds."""
    logger.info("Task succeeded: %s", dict)

def on_failure_task(dict):
    """Log an error message when a task fails."""
    logger.error("Task failed: %s", dict)

# Set up default arguments for the DAG
default_args = {
    "owner": "Airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
    "emails": ["sample@email.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_task,
    "on_success_callback": on_success_task,
    "execution_time": timedelta(seconds=60)
}


def paginate_data(response, endpoint, **context):
    """
    Extracts data from paginated API responses and pushes the consolidated results to XCom.

    Args:
        response (str): Initial JSON response from the API.
        endpoint (str): Name of the data point.
        **context: Additional context passed to Xcom.
    """
    logger.info("Initial response from %s.", endpoint)
    json_data = json.loads(response)

    json_results = json_data["results"]

    next_url = json_data["next"]

    while next_url:
        logger.info("Request data from %s.", next_url)
        data = requests.get(next_url).json()
        json_results.extend(data["results"])
        next_url = data["next"]

    context["ti"].xcom_push(key=f"{endpoint}_results", value=json_results)

def process_dim_film(**context):
    """
    Processes the film data and writes it to the 'dim_film' table in the database.
    Args:
        **context: Additional context passed from Xcom.
    """
    logger.info("Read Films Data from XCom")
    films_data = context["ti"].xcom_pull(key="films_results", task_ids="consolidate_films_data")
    films_df = pd.DataFrame(films_data)

    logger.info("Process dim film data")
    films_df["id"] = films_df["url"].str.split("/").str[-2]
    dim_film = films_df
    dim_film["character_count"] = dim_film["characters"].apply(lambda x: len(set(x)))
    dim_film["planet_count"] = dim_film["planets"].apply(lambda x: len(set(x)))
    dim_film["starship_count"] = dim_film["starships"].apply(lambda x: len(set(x)))
    dim_film["vehicle_count"] = dim_film["vehicles"].apply(lambda x: len(set(x)))
    dim_film["species_count"] = dim_film["species"].apply(lambda x: len(set(x)))
    dim_film["created"] = pd.to_datetime(dim_film["created"], utc=True)
    dim_film["edited"] = pd.to_datetime(dim_film["edited"], utc=True)
    dim_film = dim_film[["id", "title", "episode_id", "opening_crawl", "director", "producer", "character_count", "planet_count", "starship_count", "vehicle_count", "species_count", "release_date", "created", "edited"]]

    logger.info("Write dim film data to Postgres")
    write_to_postgres(dim_film, "dim_film")

def process_film_people_map(**context):
    """
    Processes the film-people mapping data and writes it to the 'film_people_map' table in the database.
    Args:
        **context: Additional context passed from Xcom.
    """
    logger.info("Read Films Data from XCom")
    films_data = context["ti"].xcom_pull(key="films_results", task_ids="consolidate_films_data")
    film_people_map = pd.DataFrame(films_data)

    logger.info("Process film people map data")
    film_people_map["film_id"] = film_people_map["url"].str.split("/").str[-2]
    film_people_map["people_id"] = film_people_map["characters"]
    film_people_map = film_people_map[["film_id", "people_id"]]
    film_people_map = film_people_map.explode("people_id")
    film_people_map["people_id"] = film_people_map["people_id"].str.split("/").str[-2]

    logger.info("Write film people map to Postgres")
    write_to_postgres(film_people_map, "film_people_map")

def process_dim_people(**context):
    """
    Processes the people data and writes it to the 'dim_people' table in the database.
    Args:
        **context: Additional context passed from Xcom.
    """

    logger.info("Read People and Planets Data from XCom")
    people_data = context["ti"].xcom_pull(key="people_results", task_ids="consolidate_people_data")
    planets_data = context["ti"].xcom_pull(key="planets_results", task_ids="consolidate_planets_data")
    people_df = pd.DataFrame(people_data)
    planets_df = pd.DataFrame(planets_data)

    logger.info("Process dim people data")
    dim_people = pd.merge(people_df, planets_df, left_on="homeworld", right_on="url")
    dim_people["id"] = dim_people["url_x"].str.split("/").str[-2]
    dim_people["name"] = dim_people["name_x"]
    dim_people["homeworld"] = dim_people["name_y"]
    dim_people["mass"] = pd.to_numeric(dim_people["mass"], errors="coerce")
    dim_people["species_count"] = dim_people["species"].apply(lambda x: len(set(x)))
    dim_people["vehicle_count"] = dim_people["vehicles"].apply(lambda x: len(set(x)))
    dim_people["starship_count"] = dim_people["starships"].apply(lambda x: len(set(x))) 
    dim_people["created"] = pd.to_datetime(dim_people["created_x"], utc=True)
    dim_people["edited"] = pd.to_datetime(dim_people["edited_x"], utc=True)
    dim_people = dim_people[["id", "name", "mass", "hair_color", "skin_color", "eye_color", "birth_year", "gender", "homeworld", "species_count", "vehicle_count", "starship_count", "created", "edited"]]

    logger.info("Write dim people to Postgres")
    write_to_postgres(dim_people, "dim_people")


# Define the DAG
with DAG("1_api_task", start_date=datetime(2023, 5, 27), schedule_interval=timedelta(days=1), default_args=default_args, catchup=False) as dag:

    for endpoint in ["films", "planets", "people"]:
        globals()[f"{endpoint}_swapi_request"] = SimpleHttpOperator(
                task_id=f"http_request_{endpoint}",
                method="GET",
                endpoint=endpoint,
                http_conn_id="swapi",
                log_response=True
            )

        globals()[f"consolidate_{endpoint}_data"] = PythonOperator(
            task_id=f"consolidate_{endpoint}_data",
            python_callable=paginate_data,
            op_kwargs={"response": globals()[f"{endpoint}_swapi_request"].output, "endpoint" : endpoint}
        )

    create_films_table = PostgresOperator(
        task_id="create_films_table",
        postgres_conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS dim_film (
                    id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    title VARCHAR(255),
                    episode_id INTEGER,
                    opening_crawl TEXT,
                    director VARCHAR(255),
                    producer VARCHAR(255),
                    character_count INTEGER,
                    planet_count INTEGER,
                    starship_count INTEGER,
                    vehicle_count INTEGER,
                    species_count INTEGER,
                    release_date DATE,
                    created TIMESTAMP,
                    edited TIMESTAMP
                );
            """
        )

    create_people_table = PostgresOperator(
        task_id="create_people_table",
        postgres_conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS dim_people (
                    id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    name VARCHAR(255),
                    height INTEGER,
                    mass INTEGER,
                    hair_color VARCHAR(255),
                    skin_color VARCHAR(255),
                    eye_color VARCHAR(255),
                    birth_year VARCHAR(255),
                    gender VARCHAR(255),
                    homeworld VARCHAR(255),
                    species_count INTEGER,
                    vehicle_count INTEGER,
                    starship_count INTEGER,
                    created TIMESTAMP,
                    edited TIMESTAMP
                );
            """
        )

    create_films_people_map_table = PostgresOperator(
        task_id="create_films_people_map_table",
        postgres_conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS film_people_map (
                    film_id INTEGER,
                    people_id INTEGER,
                    FOREIGN KEY (film_id) REFERENCES dim_film (id),
                    FOREIGN KEY (people_id) REFERENCES dim_people (id)
                );
            """
        )

    process_dim_film = PythonOperator(
        task_id=f"process_dim_film",
        python_callable=process_dim_film,
        provide_context=True,
        dag=dag
    )

    process_dim_people = PythonOperator(
        task_id=f"process_dim_people",
        python_callable=process_dim_people,
        provide_context=True,
        dag=dag
    )

    process_film_people_map = PythonOperator(
        task_id=f"process_film_people_map",
        python_callable=process_film_people_map,
        provide_context=True,
        dag=dag
    )

    # Set dependencies
    films_swapi_request >> consolidate_films_data
    consolidate_films_data >> create_films_table >> process_dim_film
    [create_films_table, create_people_table] >> create_films_people_map_table >> process_film_people_map
    [create_films_people_map_table, process_dim_film,  process_dim_people] >> process_film_people_map
    people_swapi_request >> consolidate_people_data
    planets_swapi_request >> consolidate_planets_data
    [consolidate_people_data, consolidate_planets_data] >> create_people_table >> process_dim_people
