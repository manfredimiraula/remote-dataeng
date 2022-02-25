import json
import time
from time import sleep
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import numpy as np
# Postgres DB
from sqlalchemy import create_engine,  Integer, Numeric, Text
import psycopg2
from sqlalchemy.sql.expression import column


def read_json(file_path):
    """
    Read json config file

    param: file path of the .json

    returns the loaded json in the form of a python dictionary
    """
    with open(file_path, "r") as f:
        return json.load(f)


def infer_schema(df):
    """
    """
    schema_dtype = list(df.infer_objects().dtypes)
    schema_col_name = list(df.columns.str.lower())
    tmp_dtype_to_sql = []
    for type in schema_dtype:
        if type == np.dtype('float64'):
            tmp_dtype_to_sql.append('float')
        # here we chose the type text to include all string types
        elif type == np.dtype('O'):
            tmp_dtype_to_sql.append('text')
        elif type == np.dtype('<M8[ns]'):
            tmp_dtype_to_sql.append('date')
        elif type == np.dtype('int'):
            tmp_dtype_to_sql.append('int')
        else:
            tmp_dtype_to_sql.append('text')
    schema_dtype = list(tmp_dtype_to_sql)
    return schema_dtype, schema_col_name


def build_table_structure(df, schema, table):
    """
    Build the table structure based on the table name. It automatically builds the SQL table structure with correct datatypes by inferring from the DataFrame.

    :param df -> panda DataFrame containing the dat

    returns the table structure based on the df
    """
    col_dtypes, col_name = infer_schema(df)

    create_statement = f'CREATE TABLE IF NOT EXISTS {schema}.{table} ('
    for i in range(len(col_dtypes)):
        create_statement = create_statement + '\n' + \
            col_name[i] + ' ' + col_dtypes[i] + ', '
    create_statement = create_statement[:-2] + ')'
    #index_keys = """(coin_id, name)"""
    return create_statement  # , index_keys


def initialize_tables_in_db(config, dct, table_schema):
    """
    Initialize tables
    """
    conn = build_connection_engine(config)
    cur = conn.cursor()

    for key in dct:
        table_name = config.get("DATABASE").get("TABLES").get(key)
        df = dct[key]
        print(table_name)
        cur.execute(
            "select * from information_schema.tables where table_name=%s and table_schema = %s", (table_name, table_schema))
        check = bool(cur.rowcount)
        print(check)
        if check == False:
            print("Initializing table")
            table_structure = build_table_structure(
                df, table_schema, table_name)
            # create schema if doesn't exist
            cur.execute(
                f"""CREATE SCHEMA IF NOT EXISTS {table_schema} AUTHORIZATION {config.get("DATABASE").get("POSTGRES").get("USERNAME")};"""
            )
            # initialize table if doesn't exist
            cur.execute(
                f"""
                    DROP TABLE if EXISTS {table_schema}.{table_name};
                    {table_structure}
                    WITH (
                            OIDS = FALSE
                            )
                            TABLESPACE pg_default;
                            ALTER TABLE {table_schema}.{table_name}
                            OWNER to manfredi;
                            
                    """
            )
        else:
            print('The database is already initialized')
    conn.commit()  # <--- makes sure the change is shown in the database
    conn.close()
    cur.close()


def build_connection_engine(config, type='p'):
    """
    Create the PostgresDB connection using config and return the connection object. Based on type we create a psycopg2 engine or SQLAlchemy engine 

    :param -> username
    :param -> passowrd
    :param -> host
    :param -> port
    :param -> db

    return psycopg2 connection object
    """
    username = config.get("DATABASE").get("POSTGRES").get("USERNAME")
    password = config.get("DATABASE").get("POSTGRES").get("PASSWORD")
    host = config.get("DATABASE").get("POSTGRES").get("HOST")
    port = config.get("DATABASE").get("POSTGRES").get("PORT")
    db = config.get("DATABASE").get("POSTGRES").get("DB")
    if type == 'p':
        conn = psycopg2.connect(database=db, user=username,
                                password=password, host=host, port=port)
    else:
        conn = create_engine('postgresql+psycopg2://{}:{}@{}:{}/postgres'
                             .format(username,
                                     password,
                                     host,
                                     port
                                     ), echo=False)
    return conn


def load_to_postgres(config, dct, table_schema):
    """
    This function will upload the data extracted from the MCM page from a single coin history to the table. 
    This is an iterative process, thus we will check the latest data available and append new data

    :param 

    returns
    """
    # create the list of entries that are already present at the db
    conn = build_connection_engine(config, 's')

    for key in dct:
        table_name = config.get("DATABASE").get("TABLES").get(key)
        df = dct[key]
        # Map the lowering function to all column names
        df.columns = map(str.lower, df.columns)
        print(table_name)

        df.to_sql(name=table_name,
                  schema=table_schema,
                  con=conn,
                  if_exists='append',
                  index=False,
                  chunksize=1000,
                  method='multi',
                  )

        print('Inserted '+str(len(df))+' rows ' + "in " +
              str(table_schema) + "." + str(table_name))


################
