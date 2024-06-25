from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишем таски в питоне
from datetime import datetime
import pandas as pd
import random
import numpy as np
import sqlalchemy as sa

#Данные для подключения к БД
engine = sa.create_engine("postgresql+psycopg2://SA:centr@localhost/Centr_invest")
conn = engine.connect()

#Аргументы для подключения к эирфлоу
default_args = {
    'owner': 'Ftotti08', # Владелец операции
    'depends_on_past': True, # Зависимость от прошлых запусков

    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками

    'email': 'ftotti03@gmail.com', # Почта для уведомлений
                }


@dag(default_args=default_args, catchup=False)
def create_tables()
    engine = sa.create_engine("postgresql+psycopg2://SA:centr@localhost/Centr_invest")  # Задаем параметры соединения
    conn = engine.connect()

    @task()
    def create_products(retries=2):
        products_df = {'product_id': [i for i in range(10 ** 5)],
                       'price': [random.choice([j + 0.99 for j in range(4, 5000, 5)]) for i in range(10 ** 5)], \
                       'weight': [random.randrange(50, 5000) / 10 ** 3 for i in range(10 ** 5)], \
                       'size': [str(random.randrange(1, 100)) + '*' + str(random.randrange(1, 100)) + '*' + str(
                           random.randrange(1, 100)) for i in range(10 ** 5)]}
        products_df = pd.DataFrame(data=products_df, index=[i for i in range(10 ** 5)])
        products_df.to_sql(name='products', con=engine)

    create_products()

    @task()
    def create_transaction_products_dict(retries=2):
        tran_id_list = []
        product_id_list = []
        for i in range(10 ** 5):
            for j in range(random.randrange(1, 11)):
                tran_id_list.append(i)
                product_id_list.append(random.choice(products_df['product_id']))
        transaction_products_dict = {'tran_id': tran_id_list, \
                                     'product_id': product_id_list}
        transaction_products_dict = pd.DataFrame(data=transaction_products_dict,
                                                 index=[i for i in range(len(tran_id_list))])
        transaction_products_dict.to_sql(name='transaction_products_dict', con=engine)

    create_transaction_products_dict()

    @task()
    def create_reports(retries=2):
        reports = {'report_id': [i for i in range(10 ** 5)], \
                   'tran_id': [random.choice(products_df['product_id']) for j in range(10 ** 5)], \
                   'number_of_operations': [random.randrange(1, 11) for i in range(10 ** 5)], \
                   'max_price': [np.NaN for i in range(10 ** 5)]}
        reports = pd.DataFrame(data=reports, index=[i for i in range(10 ** 5)])

    create_reports()

    @task()  # update max_price

    query_reports = ''' UPDATE reports
                        SET max_price = t1.max_price
                    	FROM
                        	(SELECT max(price) as max_price, tran_id
                        	FROM transaction_products_dict
                        		INNER JOIN products using(product_id)
                        	GROUP BY tran_id
                        	) as t1
                        WHERE reports.tran_id = t1.tran_id
                    '''
    conn.execute(query_reports)

    @task()  # Создаем результирующую таблицу

    query_result = '''  CREATE TABLE result AS
                            SELECT tran_id AS "Номер транзакции",
                        	COUNT(product_id) AS "Количество товаров",
                        	SUM(price) AS "Сумма чека",
                        	MAX(price) AS "Стоимость самого дорогого товара"
                        FROM transaction_products_dict
                        	INNER JOIN products
                        	USING(product_id)
                        GROUP BY tran_id
                    '''
    conn.execute(query_result)


my_dag = create_tables()