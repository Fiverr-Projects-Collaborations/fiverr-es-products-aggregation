import configparser
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import json
import datetime
import numpy as np


config = configparser.ConfigParser()
config.read('config.ini')

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def rec_to_actions(df):
    """
    Takes dataframe as an input, and change records into json packets to be feed into elasticsearch.
    """
    for record in df.to_dict(orient="records"):
        yield ('{ "index" : { "_index" : "%s", "_id" : "%s" }}'% (INDEX, record['usid']))
        yield (json.dumps(record, default = myconverter))
        

def database_config():
    """
    Creating database connection using config.ini file.
    """
    user = config['mysqlDB']['user'].strip("'")
    pwd = config['mysqlDB']['pass'].strip("'")
    host = config['mysqlDB']['host'].strip("'")
    db_name = config['mysqlDB']['db'].strip("'")

    db_connection_str = 'mysql+pymysql://{user}:{pwd}@{host}:3306/{db_name}'.format(user=user,pwd=pwd,host=host,db_name=db_name)

    db_connection = create_engine(db_connection_str)
    return db_connection

def get_log_data():
    """
    To check the newly inserted record, check logs before feching rows from table.
    """
    db_connection = database_config()
    with db_connection.connect() as connection:
        result = connection.execute('select max(jobrun_datetime) from etl_log where job_status = 1;')
        return result.first()[0]

def get_etl_query(last_jobrun_datetime):
    table_name = config['mysqlDB']['table_name'].strip("'")
    if last_jobrun_datetime == None:
        query = 'SELECT * FROM {table_name}'.format(table_name=table_name)
    else:
        query = 'SELECT * FROM {table_name} where (product_added > \'{max_usid}\' ) or (image_changed > \'{max_usid}\' ) or (time_added_category > \'{max_usid}\' ) or (time_added_price > \'{max_usid}\' ) or (product_updated > \'{max_usid}\' )'.format(table_name=table_name,max_usid=get_log_data())    
    return query

if __name__ == "__main__":
    
    print("Start Time ",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    last_jobrun_datetime = get_log_data()
    query = get_etl_query(last_jobrun_datetime)
    db_connection = database_config()
    print("Database connected.")
    print("Reading table...")
    for df in pd.read_sql(query, con=db_connection, chunksize=10000):
        print("Number of rows fetched:",df.shape[0])
        if df.shape[0] != 0:
            df = df.replace("null", np.NaN , regex=True)

            df['null_count'] = df.isnull().sum(axis=1)

            df.columns = map(str.lower, df.columns)
            c = ['product_price_1','product_price_2','product_price_3','product_price_4','product_price_5']
            df[c] = df[c].fillna(0)
            df = df.fillna("")
            print("Processing completed.")

            host = config['elasticSearch']['host'].strip("'")
            INDEX = config['elasticSearch']['index'].strip("'")

            e = Elasticsearch([{'host': host, 'port': 9200}])
            if not e.indices.exists(INDEX):
                raise RuntimeError('index does not exists, use `curl -X PUT "localhost:9200/%s"` and try again'%INDEX)
            print("Pushing data into es")

    ##            with open('output.txt', 'w') as f:
    ##                for x in rec_to_actions(df):
    ##                    f.write(str(x))
            r = e.bulk(rec_to_actions(df))

            logs_data = {}

            if not r["errors"]:
                logs_data['job_status'] = 1         
            else:
                logs_data['job_status'] = 0
                
            logs_data['max_usid'] = df['usid'].max()
            logs_data['jobrun_datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logs_data['row_insert'] = df.shape[0]
            logs = pd.DataFrame([logs_data])
            logs.to_sql(name='etl_log', con=db_connection, if_exists = 'append', index=False)
            print("log updated.")

        else:
            print("No new records found")
            


            

