
from utils.utils import read_json, initialize_tables_in_db, load_to_postgres,  count_and_surface_duplicates, transfrom_date
from pathlib import Path

p = Path(".")

config = read_json(p / "modules"/"config.json")
table_schema = 'remote'


# 1. Load raw data
sheets = pd.ExcelFile('Data Analyst Assignment.xlsx')

# here we load the data from the sheets into pandas DF
# this will generate a dictionary that maps all the sheets
dct = pd.read_excel(sheets, sheet_name=None)

# 2. Initialize raw data table and load raw data into Postgres
for key in dct:
    table_name = config.get("DATABASE").get("TABLES").get(key)
    df = dct[key]
    print(f"Initializing {table_name}, loading data if needed")
    initialize_tables_in_db(config, df, table_schema, table_name)
    load_to_postgres(config, df, table_schema, table_name)


# 3. Perform checks on raw data and load data errors that cannot be automtically solved
for key in dct:
    table_name = config.get("DATABASE").get("TABLES").get(
        key).lower()+'_corrupted_data_logger'
    if key == "DimAccounts":
        df = dct[key]
        print(table_name)
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        data_error = df[df['CustomMembers'].notna()]
        data_error['error_type'] = 'potential_data_corruption'

        tmp = [dups, data_error]

        df = pd.concat(tmp)

        initialize_tables_in_db(config, df, table_schema, table_name)
        load_to_postgres(config, df, table_schema, table_name)

    if key == "DimCustomer":
        df = dct[key]
        print(table_name)
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # date transform
        cols = ['BirthDate', 'DateFirstPurchase']
        key_col = 'CustomerKey'
        for col in cols:
            df = transfrom_date(df, col, key_col)
        df = df[(df['BirthDate_flag'] == 1) | (
            df['DateFirstPurchase_flag'] == 1)]
        df['error_type'] = 'date_inconcistency'

        initialize_tables_in_db(config, df, table_schema, table_name)
        load_to_postgres(config, df, table_schema, table_name)
