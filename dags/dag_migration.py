from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_my_args = {
    'owner': 'daniil', 
    'retries': 5 
}

def get_data_pg_product(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_base_data')
    list_product = postgres_data.get_pandas_df("""
    select p.productname , p.unitprice, c.categoryname, s.companyname 
      from product p
      join category c
        on c.categoryid  = p.categoryid
      join supplier s
        on s.supplierid  = p.supplierid;""")
    ti.xcom_push(key = 'list_product', value = list_product.values.tolist())

def set_data_pg_product(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_etl_data')
    list_product = ti.xcom_pull(task_ids = 'task_get_product', key = 'list_product')
    for product in list_product:
        postgres_data.run(f"""call insert_product('{product[0]}'::character varying,'{product[2]}'::character varying, {product[1]},'{product[3]}'::character varying)""")

def get_data_pg_order(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_base_data')
    list_order = postgres_data.get_pandas_df("""
    select s.shipname, s.orderdate::varchar, s.freight
      from salesorder s;""")
    ti.xcom_push(key = 'list_order', value = list_order.values.tolist())

def set_data_pg_order(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_etl_data')
    list_order = ti.xcom_pull(task_ids = 'task_get_order', key = 'list_order')
    for order in list_order:
        
        postgres_data.run(f"""call insert_order('{order[0]}'::character varying,'{order[1]}'::timestamp, {order[2]})""")

def get_data_pg_customer(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_base_data')
    list_customer = postgres_data.get_pandas_df("""
    select c.contactname, c.contacttitle, c.city, c.country, c.phone  
      from customer c;""")
    ti.xcom_push(key = 'list_customer', value = list_customer.values.tolist())

def set_data_pg_customer(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_etl_data')
    list_customer = ti.xcom_pull(task_ids = 'task_get_customer', key = 'list_customer')
    for customer in list_customer:
        postgres_data.run(f"""call insert_customer('{customer[0]}'::character varying, '{customer[1]}'::character varying, '{customer[2]}'::character varying, '{customer[3]}'::character varying, '{customer[4]}'::character varying)""")

def get_data_pg_link_order_product(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_base_data')
    list_order_product = postgres_data.get_pandas_df("""
    select s.shipname, p.productname, o.unitprice, o.qty
      from salesorder s
      join orderdetail o
        on o.orderid = s.orderid
      join product p
        on p.productid = o.productid;""")
    ti.xcom_push(key = 'list_order_product', value = list_order_product.values.tolist())

def set_data_pg_link_order_product(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_etl_data')
    list_order_product = ti.xcom_pull(task_ids = 'task_get_link_order_product', key = 'list_order_product')
    for order_product in list_order_product:
        postgres_data.run(f"""call insert_order_product('{order_product[0]}'::character varying, '{order_product[1]}'::character varying, {order_product[3]}::smallint, {order_product[2]})""")

def get_data_pg_link_order_customer(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_base_data')
    list_order_customer = postgres_data.get_pandas_df("""
    select s.shipname, c.contactname
      from salesorder s, 
  	       customer c
     where c.custid = s.custid ::integer;""")
    ti.xcom_push(key = 'list_order_customer', value = list_order_customer.values.tolist())

def set_data_pg_link_order_customer(ti):
    postgres_data = PostgresHook(postgres_conn_id = 'postgres_etl_data')
    list_order_customer = ti.xcom_pull(task_ids = 'task_get_link_order_customer', key = 'list_order_customer')
    for order_customer in list_order_customer:
        postgres_data.run(f"""call insert_order_customer('{order_customer[0]}'::character varying, '{order_customer[1]}'::character varying)""")

with DAG(
   dag_id='dag_migration',
   default_args= default_my_args, 
   description='-',
   start_date= datetime(2023, 12, 8, 8,47),
   schedule_interval='@daily',
   catchup= False
) as dag:
    union_task = EmptyOperator(
        task_id = 'union_task'
    )
    task_get_product = PythonOperator(
        task_id='task_get_product',
        python_callable = get_data_pg_product
    )
    task_set_product = PythonOperator(
        task_id='task_set_product',
        python_callable = set_data_pg_product
    )
    task_get_order = PythonOperator(
        task_id='task_get_order',
        python_callable = get_data_pg_order
    )
    task_set_order = PythonOperator(
        task_id='task_set_order',
        python_callable = set_data_pg_order
    )
    task_get_customer = PythonOperator(
        task_id='task_get_customer',
        python_callable = get_data_pg_customer
    )
    task_set_customer = PythonOperator(
        task_id='task_set_customer',
        python_callable = set_data_pg_customer
    )
    task_get_link_order_product = PythonOperator(
        task_id='task_get_link_order_product',
        python_callable = get_data_pg_link_order_product
    )
    task_set_link_order_product = PythonOperator(
        task_id='task_set_link_order_product',
        python_callable = set_data_pg_link_order_product
    )
    task_get_link_order_customer = PythonOperator(
        task_id='task_get_link_order_customer',
        python_callable = get_data_pg_link_order_customer
    )
    task_set_link_order_customer = PythonOperator(
        task_id='task_set_link_order_customer',
        python_callable = set_data_pg_link_order_customer
    )
    finish_task = EmptyOperator(
        task_id = 'finish_task'
    )
    [task_get_product >> task_set_product, task_get_order >> task_set_order, task_get_customer >> task_set_customer] >> union_task >> [task_get_link_order_product >> task_set_link_order_product, task_get_link_order_customer >> task_set_link_order_customer]>>finish_task