import json
import pandas as pd
import numpy as np
# Postgres DB
from sqlalchemy import create_engine,  Integer, Numeric, Text
import psycopg2
from sqlalchemy.sql.expression import column
import sqlite3

def initialize_sqllite_db(name):
    """"""
    sqlite3.connect(name)
    con = sqlite3.connect(name)
    
    return con


def read_raw(file_name):
    """
    Simple function that reads an Excel file with multiple sheets, ingesting all the sheets in a dictionary of the format 
        {
            sheet_name: df,
            ...
        }
    
    :param xslx

    return dict
    """
    sheets = pd.ExcelFile(file_name)
    dct = pd.read_excel(sheets, sheet_name=None)
    return dct


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
    Simple function to evaluate the datatype of dataframe columns to prepare loading into DB

    :param pd.DataFrame

    return schema_dtype (list of dtypes of columns) and schema_col_name (list of column name for table)
    """
    schema_dtype = list(df.infer_objects().dtypes)
    schema_col_name = list(df.columns)
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


def build_table_structure(df, table):
    """
    Build the table structure based on the table name. It automatically builds the SQL table structure with correct datatypes by inferring from the DataFrame.

    :param df -> panda DataFrame containing the dat

    returns the table structure based on the df
    """
    col_dtypes, col_name = infer_schema(df)

    create_statement = f'CREATE TABLE IF NOT EXISTS {table} ('
    for i in range(len(col_dtypes)):
        create_statement = create_statement + '\n' + \
            col_name[i] + ' ' + col_dtypes[i] + ', '
    create_statement = create_statement[:-2] + ')'
    return create_statement 

def initialize_tables_in_db(con, df, table_schema, table_name, index_keys = None):
    """
    Initialize tables for DB loading. Leverages psycopg2 library. Based on a config file, it reads the database connection string.

    :param con (SQLLite connection) 
    :param  df (pd.DataFrame) the dataframe that will form the basis for the table structure
    :param table_schema the name of the table schema
    :param table_name the name of the table
    :param (optional) index_keys; when loading into a normalized table, if provided with index key it will generate a basic indexing at the table level

    return execute SQL creation statement
    """
    conn = con
    cur = conn.cursor()
    print("Initializing table")
    table_structure = build_table_structure(df,  table_name)
    
    # initialize table if doesn't exist
    if index_keys is not None:
        cur.execute(f"DROP TABLE if EXISTS {table_name}")
        cur.execute(f"{table_structure}")
        cur.execute(f"CREATE UNIQUE  INDEX {table_name}_pkid ON {table_name} {index_keys};")
        
    else:
        cur.execute(f"DROP TABLE if EXISTS {table_name}")
        cur.execute(f"{table_structure}")


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
        conn = create_engine("sqlite://", connect_args={'timeout': 15})
        conn = conn.connect()
    return conn


def load_to_postgres(con, df, table_schema, table_name):
    """
    This function will upload the data extracted from the MCM page from a single coin history to the table. 
    This is an iterative process, thus we will check the latest data available and append new data

    :param config (json) the configuration json file
    :param  df (pd.DataFrame) the dataframe that will form the basis for the table structure
    :param table_schema the name of the table schema
    :param table_name the name of the table

    returns execute load into DB
    """
    # create the list of entries that are already present at the db
    engine = create_engine('sqlite:///remote.db', echo=True)
    conn = engine.connect()

    df.to_sql(table_name, conn, if_exists='replace')

    print('Inserted '+str(len(df))+' rows ' + "in " +
          str(table_schema) + "." + str(table_name))


# Data validation checks

def count_and_surface_duplicates(df):
    """
    This function takes a dataframe, check for duplication and generate a dataframe with the duplicated rows for further exploration. It leverages the function  count_and_surface_duplicates()

    :param df -> pd.DataFrame of the raw data

    :returns duplicates (pd.DataFrame) and the number of duplicates
    """
    dup_cnt = df.duplicated().sum()
    duplicates = df.loc[df.duplicated(), :]

    print(f"the dataframe has {dup_cnt} duplicated rows.'")

    if dup_cnt > 0:
        duplicates['error_type'] = 'duplicated_row_in_raw_table'

    return duplicates, dup_cnt


def transfrom_date(df, col, key_col):
    """
    This function is based on the analysis conducted on the table DimCustomers. It generates a new column, optimized_BirthDate which imputes the median values of the BirthDates seen in the data to try solve data corruption in those dates that have a format nn/nn/nn without an expicit year

    :param df (pd.DataFrame) the dataframe target for manipulation
    :param col the specific column to be manipulated
    :param key_col  the specific col at which level we aggregate 

    returns the modified pd.DataFrame

    """
    tmp = []
    for ix, row in df.iterrows():
            tmp.append(pd.to_datetime(row[col], infer_datetime_format=True, errors='coerce'))

    date_exploration = df[[key_col, col]]
    date_exploration['tmp_date'] = tmp
    date_exploration['year'] = date_exploration.tmp_date.dt.year
    df[f'{col}_flag'] = np.where(df[col].str.len() > 8, 0, 1)

    df[f'{col}_left'] = df[col].str[:6]
    df[f'{col}_right'] = df[col].str[6:]
    df[f'optimized_{col}'] = np.where(df[f'{col}_flag'] == 0,  df[col], df[f'{col}_left']+str(
        date_exploration['year'].median())[:2]+df[f'{col}_right'])
    
    tmp = []
    for ix, row in df.iterrows():
            tmp.append(pd.to_datetime(row[f'optimized_{col}'], infer_datetime_format=True))
    df[col] = tmp

    return df


def report_build(query, name, con, filepath):
    """
    Execute query to the local SQLlite db and store the answer to simple question in a report. It returns the report for further analysis or charting

    :param query (str) the query to be used
    :param name (str) the name of the file to be saved
    :con the SQLLite connection
    :filepath the directory to save the file

    return pd.DataFrame
    """
    # Questions answered - Generate reports
    df = pd.read_sql_query(query,con=con)
    df.to_csv(filepath+name+'.csv') 

    return df

