from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pandas as pd
from datetime import timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def get_data_from_url(symblo):
    url_info_stock = f"https://www.settrade.com/C04_02_stock_historical_p1.jsp?txtSymbol={symblo}&selectPage=2&max=200&offset=0"
    res_stock = requests.get(url_info_stock)
    if res_stock.status_code == 200:
        print("Successful")
    elif res_stock.status_code == 404:
        print("Error 404 page not found")
    else:
        print("Not both 200 and 404")

    soup_stock = BeautifulSoup(res_stock.content, "html.parser")
    table_stock = soup_stock.select_one("div.table-responsive")
    data_all_stock = table_stock.find_all("tr")
    return data_all_stock


def add_to_list(data_all_stock):
    ls = []
    for i in range(len(data_all_stock)):
        ls_data = []
        if i == 0:
            data = data_all_stock[0].find_all("th")
            data = [
                "Date",
                "Open",
                "High",
                "Low",
                "AveragePrice",
                "Close",
                "Change",
                "Percent_Change",
                "Volumex1000",
                "Value_MB",
                "SET_Index",
                "Percent_Change",
            ]
        else:
            data = data_all_stock[i].find_all("td")
        for j in range(len(data)):
            if i == 0:
                ls_data.append(data[j])
            elif j == 0:
                ls_data.append(
                    str(datetime.strptime(data[j].text, "%d/%m/%y").isoformat())
                )
            else:
                ls_data.append(float(data[j].text.replace(",", "")))
        ls.append(ls_data)
    return ls


def list_to_json(ls):
    res_dict = {
        str(ls[i][0]): {ls[0][j]: ls[i][j] for j in range(len(ls[0]))}
        for i in range(1, len(ls))
    }
    return res_dict


def scraping_to_csv():
    symbol = "A"
    data_all_stock = get_data_from_url(symbol)
    ls = add_to_list(data_all_stock)
    json_stock = list_to_json(ls)
    stock_dict = [i for i in json_stock.values()]
    df = pd.DataFrame(stock_dict)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df.to_csv("/home/airflow/gcs/data/result.csv", index=False)


def check_table():
    status = "table_exists"
    client = bigquery.Client()
    table_id = "etlstock.pipeline_stock.testselect"
    try:
        client.get_table(table_id)
        print("Table {} already exists.".format(table_id))
    except NotFound:
        print("Table {} is not found.".format(table_id))
        table = bigquery.Table(table_id)
        table = client.create_table(table)
        status = "table_not_exists"
    return status


def query_max_date(**context):
    client = bigquery.Client()
    query_job = """
        SELECT max(Date) as maxdate
        FROM `etlstock.pipeline_stock.testselect`
        """
    df = client.query(query_job).to_dataframe()
    maxdate = str(df["maxdate"][0])
    context["ti"].xcom_push(key="max_date", value=maxdate)


def check_data_to_update(**context):
    max_date = context["ti"].xcom_pull(
        task_ids="query_max_date", key="max_date", include_prior_dates=True
    )
    result_df = pd.read_csv("/home/airflow/gcs/data/result.csv")
    check_data = result_df["Date"].max() > max_date
    dump = "no_data_to_update"
    if check_data:
        result = result_df[result_df["Date"] > max_date]
        result.to_csv("/home/airflow/gcs/data/new_result.csv", index=False)
        dump = "data_to_update"
    return dump


default_args = {
    "owner": "iFrong",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@daily",
}

dag = DAG(
    "pipeline_stock",
    default_args=default_args,
    description="Pipeline for ETL stock",
)

web_to_csv = PythonOperator(
    task_id="web_to_csv",
    python_callable=scraping_to_csv,
    dag=dag,
)

query_max_date = PythonOperator(
    task_id="query_max_date",
    python_callable=query_max_date,
    provide_context=True,
    dag=dag,
)


check_table = BranchPythonOperator(
    task_id="Check_table_exists",
    python_callable=check_table,
    dag=dag,
)

check_data_to_update = BranchPythonOperator(
    task_id="check_data_to_update",
    python_callable=check_data_to_update,
    dag=dag,
    provide_context=True,
)


load_to_bq = BashOperator(
    task_id="bq_load",
    bash_command="bq load --source_format=CSV --autodetect \
            pipeline_stock.testselect \
            gs://asia-northeast1-etlstock-5cfdc9a7-bucket/data/result.csv",
    dag=dag,
)

insert_to_bq = BashOperator(
    task_id="insert_to_bq",
    bash_command="bq load --source_format=CSV --autodetect \
            pipeline_stock.testselect \
            gs://asia-northeast1-etlstock-5cfdc9a7-bucket/data/new_result.csv",
    dag=dag,
)

table_exists = DummyOperator(task_id="table_exists", dag=dag)

table_not_exists = DummyOperator(task_id="table_not_exists", dag=dag)

data_to_update = DummyOperator(task_id="data_to_update", dag=dag)

no_data_to_update = DummyOperator(task_id="no_data_to_update", dag=dag)


web_to_csv >> check_table >> [table_exists, table_not_exists]
(
    table_exists
    >> query_max_date
    >> check_data_to_update
    >> [data_to_update, no_data_to_update]
)
table_not_exists >> load_to_bq
data_to_update >> insert_to_bq
