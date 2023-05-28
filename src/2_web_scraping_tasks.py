import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from utils.setup_logger import setup_logging
from utils.postgres_writer import write_to_postgres
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
from time import sleep
import re

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

def get_movies_list(url, **context):
    """
    Get list of movies url to webscrape
    Args:
        url (str): Initial webpages for movie url webscraping
        **context: Additional context passed from Xcom.
    """

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the response
        soup = BeautifulSoup(response.content, "html.parser")
        movie_links = [a["href"] for a in soup.find_all("a", href=lambda href: href and "/m/" in href)]
        context["ti"].xcom_push(key="movies_list", value=movie_links)
    else:
        logger.error('Request failed code %s for: %s', response.status_code, url)

def get_movie_data(url):
    """
    Get movie data from movie url
    Args:
        url (str): Movie url for webscraping
        **context: Additional context passed from Xcom.
    """


    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content of the response
        soup = BeautifulSoup(response.content, "html.parser")

        logger.info('Data extraction and processing: %s', url)
        # Due to some uncertainty of the extraction of main data would fail due to the html is not loaded yet, so paused for 2 secs then retry again
        try:
            json_data = json.loads(soup.find('script', id='scoreDetails').contents[0])
        except Exception as error:
            logger.error("Retry due to %s", error)
            sleep(2)
            json_data = json.loads(soup.find('script', id='scoreDetails').contents[0])
        data = {
            "year": json_data["scoreboard"]["info"].split(",")[0],
            "title": soup.find('h1').get_text(),
            "reviews": json_data["scoreboard"]["tomatometerScore"]["reviewCount"],
            "tomatometer": json_data["scoreboard"]["tomatometerScore"]["value"] / 100 if json_data["scoreboard"]["tomatometerScore"].get("value") is not None else None,
            "audience_score": json_data["scoreboard"]["audienceScore"]["value"] / 100 if json_data["scoreboard"]["audienceScore"].get("value") is not None else None,
            "synopsis": soup.find('p', attrs={'data-qa': 'movie-info-synopsis'}).get_text(strip=True),
            "rating": None,
            "genre": None,
            "original_language": None,
            "director": None,
            "producer": None,
            "writer": None,
            "release_date_theaters": None,
            "release_date_streaming": None,
            "runtime_mins": None,
            "distributor": None,
            "production_co": None,
            "aspect_ratio": None,
            "url": url
        }

        for info in ["rating", "director", "writer", "producer", "distributor", "original_language", "aspect_ratio"]:
            try:
                data[info] = soup.find('b', text=f"{info.replace('_', ' ').title()}:").find_next('span').get_text(strip=True)
            except AttributeError:
                pass

        for info in ["theaters", "streaming"]:
            try:
                globals()[f"release_date_{info}"] = soup.find('b', text=f"Release Date ({info.title()}):").find_next('time').get_text(strip=True)
                data[f"release_date_{info}"] = datetime.strptime(globals()[f"release_date_{info}"], "%b %d, %Y").strftime("%Y-%m-%d")
            except AttributeError:
                pass

        for info in ["genre", "production_co"]:
            try:
                data[info] = re.sub(r'\s*,\s*', ', ', soup.find('b', text=f"{info.replace('_', ' ').title()}:").find_next('span').get_text(strip=True))
            except AttributeError:
                pass

        try:
            runtime_mins = soup.find('b', text="Runtime:").find_next('span').get_text(strip=True).replace('h', '*60+').replace('m', '')
            data["runtime_mins"] = eval(runtime_mins)
        except AttributeError:
            pass

        return data
    else:
        logger.error('Request failed code %s for: %s', response.status_code, url)

def process_movie_data(**context):
    """
    Consolidate the movie data and writes it to the 'fact_top_movies' table in the database.
    Args:
        **context: Additional context passed from Xcom.
    """

    logger.info("Read Films Data from XCom")
    movie_list = context["ti"].xcom_pull(key="movies_list", task_ids="get_movies_list")
    
    movie_data = []
    for movie in movie_list:
        url = "https://www.rottentomatoes.com" + movie
        movie_details = get_movie_data(url) 
        movie_data.append(movie_details)
    fact_top_movies = pd.DataFrame(movie_data)

    logger.info("Write fact top movies to Postgres")
    write_to_postgres(fact_top_movies, "fact_top_movies")


# Define the DAG
with DAG("2_web_scraping_task", start_date=datetime(2023, 5, 27), schedule_interval=timedelta(days=1), default_args=default_args, catchup=False) as dag:

    get_movies_list = PythonOperator(
        task_id=f"get_movies_list",
        python_callable=get_movies_list,
        op_kwargs={"url" :"https://www.rottentomatoes.com/browse/movies_at_home/sort:popular?page=1"}
    )

    create_fact_top_movies_table = PostgresOperator(
        task_id="create_fact_top_movies_table",
        postgres_conn_id="postgres",
        sql="""
                CREATE TABLE IF NOT EXISTS fact_top_movies (
                    year INTEGER,
                    title VARCHAR(255),
                    reviews INTEGER,
                    tomatometer FLOAT,
                    audience_score FLOAT,
                    synopsis TEXT,
                    rating VARCHAR(255),
                    genre VARCHAR(255),
                    original_language VARCHAR(255),
                    director VARCHAR(255),
                    producer VARCHAR(255),
                    writer VARCHAR(255),
                    release_date_theaters DATE,
                    release_date_streaming DATE,
                    runtime_mins INTEGER,
                    distributor VARCHAR(255),
                    production_co VARCHAR(255),
                    aspect_ratio VARCHAR(255),
                    url VARCHAR(255)
                );
            """
        )

    process_movie_data = PythonOperator(
        task_id=f"process_movie_data",
        python_callable=process_movie_data,
        provide_context=True,
        dag=dag
    )

get_movies_list >> create_fact_top_movies_table >> process_movie_data