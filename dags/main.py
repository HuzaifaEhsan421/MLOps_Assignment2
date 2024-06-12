import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import subprocess

logging.basicConfig(level=logging.INFO)

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract(**kwargs):
    top_links = []
    for source in sources:
        logging.info(f"Extracting from {source}")
        reqs = requests.get(source)
        if reqs.status_code == 200:
            soup = BeautifulSoup(reqs.text, 'html.parser')
            for link in soup.find_all('a', href=True):
                title = link.text.strip()
                link_url = link['href']
                description = ""
                # Find the description if available
                if link.parent.name == "h3":
                    description = link.parent.find_next('p').text.strip()
                elif link.parent.name == "div":
                    description = link.parent.text.strip()
                top_links.append({'title': title, 'link': link_url, 'description': description, 'date': datetime.now(), 'website': source})
            logging.info(f"Extracted {len(top_links)} links from {source}")
        else:
            logging.error(f"Failed to extract from {source}. Status code: {reqs.status_code}")
    if top_links:
        kwargs['ti'].xcom_push(key='extracted_data', value=top_links)
    else:
        logging.error("No data extracted. Unable to push to XCom.")

def transform(**kwargs):
    ti = kwargs['ti']
    top_links = ti.xcom_pull(key='top_links', task_ids='extract_links')
    logging.info("Transforming data")
    df = pd.DataFrame(top_links)
    ti.xcom_push(key='transformed_data', value=df)
    return df

def load(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    logging.info("Loading data")
    df.to_csv('extracted_data.csv', index=False)
    logging.info(f"Length of DataFrame: {len(df)}")

    logging.info("CSV created")

    # Initialize Git
    if not os.path.exists('.git'):
        try:
            subprocess.run(["git", "init"], capture_output=True, text=True, check=True)
            logging.info("Git initialized")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to initialize Git: {e.stderr}")
    else:
        logging.info("Git already initialized")

    # Initialize DVC if not already initialized
    if not os.path.exists('.dvc'):
        try:
            subprocess.run(["dvc", "init"], capture_output=True, text=True, check=True)
            logging.info("DVC initialized")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to initialize DVC: {e.stderr}")
    else:
        logging.info("DVC already initialized")

    # Add Google Drive as a remote storage if not already added
    if not os.path.exists('airflow\dvc\config'):
        # Add Google Drive as remote storage using DVC
        logging.info("Adding Google Drive as remote storage...")
        try:
            subprocess.run(["dvc", "remote", "add", "-d", "gdrive", "gdrive://1_1O_pJu6N3y-iLT4F1U_xlg3-zzpGARg"], capture_output=True, text=True, check=True)
            logging.info("Google Drive added as remote storage")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error adding Google Drive as remote storage: {e}")

    # Add CSV file to DVC
    logging.info("Adding CSV file to DVC...")
    try:
        subprocess.run(["dvc", "add", "top_links.csv"], capture_output=True, text=True, check=True)
        logging.info("CSV file added to DVC")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error adding CSV file to DVC: {e}")

    # Push data to Google Drive using DVC
    logging.info("Pushing data to Google Drive using DVC...")
    try:
        subprocess.run(["dvc", "push"], capture_output=True, text=True, check=True)
        logging.info("Data pushed to Google Drive using DVC")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error pushing data to Google Drive using DVC: {e.stderr}")

default_args = {
    'owner': 'airflow-demo',
    'start_date': datetime(2024, 5, 7),
    'catchup': False,
}

dag = DAG(
    'MLops-Assignment1',
    default_args=default_args,
    description='A simple DAG to extract top links, transform, and push to Google Drive using DVC',
    schedule_interval='@daily',
)

task1 = PythonOperator(
    task_id="extract_links",
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id="load_to_drive",
    python_callable=load,
    provide_context=True,
    dag=dag,
)

task1 >> task2 >> task3
