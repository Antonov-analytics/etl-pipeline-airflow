# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Параметры подключения к БД
connection_main = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20250820'
}


connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student-rw',
    'password': '656e2b0c9c',
    'database': 'test'
}

# Параметры, которые прокидываются в таски по умолчанию
default_args = {
    'owner': 'aleksandr_antonov_hnm5755',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 29),
}

# Интервал запуска DAG = 7:00 каждый день
schedule_interval = '0 7 * * *'
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_aleksandr_antonov_hnm5755_ETL():

    # Для каждого юзера посчитаем число просмотров и лайков контента
    @task
    def extract_feed_data():
        feed_data_query = """
        SELECT user_id,
            toDate(time) as event_date,
            countIf(action='like') as likes,
            countIf(action='view') as views,
            os, gender, age
        FROM simulator_20250820.feed_actions
        WHERE toDate(time) = toDate(now()) - 1
        GROUP BY user_id, event_date, os, gender, age
        """
        df_feed = ph.read_clickhouse(query=feed_data_query, connection=connection_main)
        return df_feed

    # Для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    @task
    def extract_message_data():
        message_data_query = """
        WITH
        users_as_receiver as (
        SELECT receiver_id, 
            COUNT(receiver_id) as messages_received,
            uniq(user_id) as users_sent
        FROM simulator_20250820.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY receiver_id
        ),

        users_as_sender as (
        SELECT user_id, 
            toDate(time) as event_date,
            COUNT(user_id) as messages_sent,
            uniq(receiver_id) as users_received,
            age, gender, os            
        FROM simulator_20250820.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id, toDate(time) as event_date, age, gender, os
        )

        SELECT *
        FROM users_as_sender as us
        JOIN users_as_receiver as ur 
        ON us.user_id = ur.receiver_id
        """
        df_message = ph.read_clickhouse(query=message_data_query, connection=connection_main)
        return df_message

    # Объединяем обе таблицы (лента и сообщения)
    @task
    def transform_feed_join_messages(df_feed, df_message):
        joined_df_cube = df_feed.merge(df_message, how='outer', on=['user_id', 'event_date', 'os', 'gender', 'age']).replace(np.nan, 0)
        return joined_df_cube

    # Создаем срез по операционной системе
    @task
    def transform_df_os(joined_df_cube):
        df_os = joined_df_cube.groupby(['os', 'event_date'])[['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].sum().reset_index()
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        df_os.insert(1, 'dimension', 'os')
        return df_os

    # Создаем срез по полу
    @task
    def transform_df_gender(joined_df_cube):
        df_gender = joined_df_cube.groupby(['gender', 'event_date'])[['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].sum().reset_index()
        df_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender

    # Создаем срез по возрастным группам
    @task
    def transform_df_age(joined_df_cube):
        prep_df_age = joined_df_cube.drop(columns=['age']).copy()
        prep_df_age['age_group'] = pd.cut(joined_df_cube['age'], bins = [0, 17 , 25, 35, 45, 100], labels =['0-17', '18-25', '26-35', '36-45', '45+'])
        df_age = prep_df_age.groupby(['age_group', 'event_date'])[['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].sum().reset_index()
        df_age.rename(columns = {'age_group' : 'dimension_value'}, inplace = True)
        df_age.insert(1, 'dimension', 'age')
        return df_age

    # Объединяем все срезы в одну таблицу
    @task
    def transform_df_contact(df_os, df_gender, df_age):
        final_df = pd.concat([df_os, df_gender, df_age]).reset_index(drop=True)
        final_df['likes'] = final_df['likes'].astype('int')
        final_df['views'] = final_df['views'].astype('int')
        final_df['messages_received'] = final_df['messages_received'].astype('int')
        final_df['messages_sent'] = final_df['messages_sent'].astype('int')
        final_df['users_received'] = final_df['users_received'].astype('int')
        final_df['users_sent'] = final_df['users_sent'].astype('int')
        return final_df
    
    @task
    def load(final_df):
        creat_table_query = """
        CREATE TABLE IF NOT EXISTS test.aleksandr_antonov_hnm5755
        (
        event_date Date,
        dimension String,
        dimension_value String,
        likes Int64,
        views Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64,
        users_sent Int64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        """
        ph.execute(creat_table_query, connection=connection_test)
        ph.to_clickhouse(final_df, table='aleksandr_antonov_hnm5755', index=False, connection=connection_test)

    df_feed = extract_feed_data()
    df_message = extract_message_data()
    joined_df_cube = transform_feed_join_messages(df_feed, df_message)
    df_os = transform_df_os(joined_df_cube)
    df_gender = transform_df_gender(joined_df_cube)
    df_age = transform_df_age(joined_df_cube)
    final_df = transform_df_contact(df_os, df_gender, df_age)
    load(final_df)

dag_aleksandr_antonov_hnm5755_ETL = dag_aleksandr_antonov_hnm5755_ETL()
