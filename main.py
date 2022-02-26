
from operator import index
from modules.utils.utils import read_json, initialize_tables_in_db, load_to_postgres,  count_and_surface_duplicates, transfrom_date, read_raw, build_connection_engine
from pathlib import Path
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

p = Path(".")

config = read_json(p / "modules"/"config.json")
table_schema = 'remote'

print('Starting reading raw file ...')
# 1. Load raw data
file_name = 'Data Analyst Assignment.xlsx'
dct = read_raw(file_name)

# here we load the data from the sheets into pandas DF
# this will generate a dictionary that maps all the sheets

print('Starting DB staging ...')
# 2. Initialize raw data table and load raw data into Postgres
for key in dct:
    table_name = config.get("DATABASE").get("TABLES").get(key)
    df = dct[key]
    print(f"Initializing {table_name}, loading data if needed")
    initialize_tables_in_db(config, df, table_schema, table_name)
    load_to_postgres(config, df, table_schema, table_name)

print('Starting performing validation checks and normalization...')
conn_s = build_connection_engine(config, 's')

# 3. Perform checks on raw data and load data errors that cannot be automtically solved
for key in dct:

    # DimAccounts
    logger_table = config.get("DATABASE").get("TABLES").get(
        key).lower()+'_corrupted_data_logger'
    normalized_table = config.get("DATABASE").get("TABLES").get(
        key).lower()+'_normalized'
    if key == "DimAccounts": 
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        print(table_name)
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        data_error = df[df['CustomMembers'].notna()]
        data_error['error_type'] = 'potential_data_corruption'

        tmp = [dups, data_error]

        error_dump = pd.concat(tmp, axis=0, ignore_index=True)

        initialize_tables_in_db(config, error_dump, table_schema, logger_table)
        load_to_postgres(config, error_dump, table_schema, logger_table)

        df['AccountKey'] = df['AccountKey'].astype(int)
        index_keys = """(AccountKey)"""
        initialize_tables_in_db(config, df, table_schema, normalized_table, index_keys)
        load_to_postgres(config, df, table_schema, normalized_table)

    if key == "DimCustomer":
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
  
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # date transform
        cols = ['BirthDate', 'DateFirstPurchase']
        key_col = 'CustomerKey'
        for col in cols:
            tmp_df = transfrom_date(df, col, key_col)
        error_dump = df[(tmp_df['BirthDate_flag'] == 1) | (tmp_df['DateFirstPurchase_flag'] == 1)]
        error_dump['error_type'] = 'date_inconcistency'

        
        tmp = [dups, error_dump]
        error_dump = pd.concat(tmp, axis=0, ignore_index=True)

        initialize_tables_in_db(config, error_dump, table_schema, logger_table)
        load_to_postgres(config, error_dump, table_schema, logger_table)

        # phone number transform
        tmp_df['Phone'] = tmp_df.Phone.str.replace('(', '').str.replace(')', '').str.replace('-', '').str.replace(' ', '').astype(int)
        tmp_df = tmp_df[list(df.columns)]
        # create normalized table
        df_norm = tmp_df.copy()
        df_norm['CustomerKey'] = df_norm['CustomerKey'].astype(int)
        index_keys = """(CustomerKey)"""
        initialize_tables_in_db(config, df_norm, table_schema, normalized_table, index_keys)
        load_to_postgres(config, df_norm, table_schema, normalized_table)

    if key == "DimProduct":
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'

        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # flag potentially dismissed products
        dismissed_prod = df[(df['StartDate'].isna()) | (df['Status'].isna())]
        dismissed_prod['error_type'] = 'potentially_dismissed_prod'
        
        tmp = [dups, dismissed_prod]
        error_dump = pd.concat(tmp, axis=0, ignore_index=True)

        initialize_tables_in_db(config, error_dump, table_schema, logger_table)
        load_to_postgres(config, error_dump, table_schema, logger_table)

        # make PrimaryKey
        df[['ProductKey']] = df[['ProductKey']].astype(int)

        # create normalized table
        df_norm = df.copy()
        index_keys = """(ProductKey)"""
        initialize_tables_in_db(config, df_norm, table_schema, normalized_table, index_keys)
        load_to_postgres(config, df_norm, table_schema, normalized_table)
    
    if key == "DimSalesTerritory":
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # remove corrupted row
        df = df[~ df.SalesTerritoryRegion.isna()]
        # create normalized table
        df_norm = df.copy()
        index_keys = """(SalesTerritoryKey)"""
        initialize_tables_in_db(config, df_norm, table_schema, normalized_table, index_keys)
        load_to_postgres(config, df_norm, table_schema, normalized_table)

    if key == "DimScenario":
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs

        # create normalized table
        df_norm = df.copy()
        index_keys = """(ScenarioKey)"""
        initialize_tables_in_db(config, df_norm, table_schema, normalized_table, index_keys)
        load_to_postgres(config, df_norm, table_schema, normalized_table)
    
    if key == "FactFinance":
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # Converting primary key to int
        df['FinanceKey'] = df.FinanceKey.astype(int)
        
        # create normalized table
        df_norm = df.copy()
        index_keys = """(FinanceKey)"""
        initialize_tables_in_db(config, df_norm, table_schema, normalized_table, index_keys)
        load_to_postgres(config, df_norm, table_schema, normalized_table)

    if key == "FactResellerSales":
        df = pd.read_sql_query(f"select * from remote.{key.lower()} ",con=conn_s)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # Converting primary key to int
        df[['ProductKey',	'OrderDateKey',	'DueDateKey', 'ShipDateKey', 'ResellerKey', 'EmployeeKey', 'PromotionKey', 'CurrencyKey', 'SalesTerritoryKey']] = df[['ProductKey',	'OrderDateKey',	'DueDateKey', 'ShipDateKey', 'ResellerKey', 'EmployeeKey', 'PromotionKey', 'CurrencyKey', 'SalesTerritoryKey']].astype(int)
        
        # create normalized table
        df_norm = df.copy()
        initialize_tables_in_db(config, df_norm, table_schema, normalized_table)
        load_to_postgres(config, df_norm, table_schema, normalized_table)