
from modules.utils import read_json, initialize_tables_in_db, load_to_db,  count_and_surface_duplicates, transfrom_date, read_raw,  initialize_sqllite_db, report_build
from pathlib import Path
import shutil
import os
import pandas as pd
import numpy as np
import warnings
import seaborn as sns
import matplotlib.pyplot as plt


warnings.filterwarnings("ignore")

p = Path(".")

config = read_json(p / "modules"/"config.json")
table_schema = 'remote.db'

con = initialize_sqllite_db('remote.db')
 
print('Load raw data ...')
# 1. Load raw data
file_name = 'Data Analyst Assignment.xlsx'
dct = read_raw(file_name)

# here we load the data from the sheets into pandas DF
# this will generate a dictionary that maps all the sheets

print('Starting DB staging ...')
# 2. Initialize raw data table and load raw data into DB
for key in dct:
    table_name = config.get("DATABASE").get("TABLES").get(key)
    df = dct[key]
    print(f"Initializing {table_name}, loading data if needed")
    initialize_tables_in_db(con, df, table_schema, table_name)
    load_to_db( df, table_schema, table_name)

print('Starting performing validation checks and normalization...')
# 3. Perform checks on raw data and load data errors that cannot be automtically solved
for key in dct:

    # DimAccounts
    logger_table = config.get("DATABASE").get("TABLES").get(
        key).lower()+'_corrupted_data_logger'
    normalized_table = config.get("DATABASE").get("TABLES").get(
        key).lower()+'_normalized'
    if key == "DimAccounts": 
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
        print(table_name)
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        data_error = df[df['CustomMembers'].notna()]
        data_error['error_type'] = 'potential_data_corruption'

        tmp = [dups, data_error]

        error_dump = pd.concat(tmp, axis=0, ignore_index=True)
        error_dump = error_dump.drop('index', axis = 1, )

        initialize_tables_in_db(con, error_dump, table_schema, logger_table)
        load_to_db( error_dump, table_schema, logger_table)

        df['AccountKey'] = df['AccountKey'].astype(int)
        df = df.drop('index', axis = 1, )
        index_keys = """(AccountKey)"""
        initialize_tables_in_db(con, df, table_schema, normalized_table, index_keys)
        load_to_db( df, table_schema, normalized_table)

    if key == "DimCustomer":
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
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
        error_dump = error_dump.drop('index', axis = 1, )
        initialize_tables_in_db(con, error_dump, table_schema, logger_table)
        load_to_db( error_dump, table_schema, logger_table)

        # phone number transform
        tmp_df['Phone'] = tmp_df.Phone.str.replace('(', '').str.replace(')', '').str.replace('-', '').str.replace(' ', '').astype(int)
        tmp_df = tmp_df[list(df.columns)]
        # create normalized table
        df_norm = tmp_df.copy()
        df_norm['CustomerKey'] = df_norm['CustomerKey'].astype(int)
        df_norm = df_norm.drop('index', axis = 1)
        index_keys = """(CustomerKey)"""
        initialize_tables_in_db(con, df_norm, table_schema, normalized_table, index_keys)
        load_to_db( df_norm, table_schema, normalized_table)

    if key == "DimProduct":
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'

       # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # flag potentially dismissed products
        dismissed_prod = df[(df['StartDate'].isna()) | (df['Status'].isna())]
        dismissed_prod['error_type'] = 'potentially_dismissed_prod'

        size_issue = df[(df['Size'].str.contains('S|M|L|XL')) & (~df['Size'].isna())]
        size_issue['error_type'] = 'size_inconcistency'
        
        tmp = [dups, dismissed_prod, size_issue]
        error_dump = pd.concat(tmp, axis=0, ignore_index=True)
        error_dump = error_dump.drop('index', axis = 1, )
        initialize_tables_in_db(con, error_dump, table_schema, logger_table)
        load_to_db( error_dump, table_schema, logger_table)

        # make PrimaryKey
        df[['ProductKey']] = df[['ProductKey']].astype(int)

        # create normalized table
        df_norm = df.copy()
        df_norm = df_norm.drop('index', axis = 1)
        index_keys = """(ProductKey)"""
        initialize_tables_in_db(con, df_norm, table_schema, normalized_table, index_keys)
        load_to_db( df_norm, table_schema, normalized_table)
    
    if key == "DimSalesTerritory":
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # remove corrupted row
        df = df[~ df.SalesTerritoryRegion.isna()]
        # create normalized table
        df_norm = df.copy()
        df_norm = df_norm.drop('index', axis = 1)
        index_keys = """(SalesTerritoryKey)"""
        initialize_tables_in_db(con, df_norm, table_schema, normalized_table, index_keys)
        load_to_db( df_norm, table_schema, normalized_table)

    if key == "DimScenario":
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs

        # create normalized table
        df_norm = df.copy()
        df_norm = df_norm.drop('index', axis = 1, )
        index_keys = """(ScenarioKey)"""
        initialize_tables_in_db(con, df_norm, table_schema, normalized_table, index_keys)
        load_to_db( df_norm, table_schema, normalized_table)
    
    if key == "FactFinance":
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # Converting primary key to int
        df['FinanceKey'] = df.FinanceKey.astype(int)
        
        # create normalized table
        df_norm = df.copy()
        df_norm = df_norm.drop('index', axis = 1)
        index_keys = """(FinanceKey)"""
        initialize_tables_in_db(con, df_norm, table_schema, normalized_table, index_keys)
        load_to_db( df_norm, table_schema, normalized_table)

    if key == "FactResellerSales":
        df = pd.read_sql_query(f"select * from {key.lower()} ",con=con)
        logger_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_corrupted_data_logger'
        normalized_table = config.get("DATABASE").get("TABLES").get(key).lower()+'_normalized'
        # Common error checks for all the tables
        dups, dup_cnt = count_and_surface_duplicates(df)

        # Table specific checks generated through EDAs
        # Converting primary key to int
        df[['ProductKey',	'OrderDateKey',	'DueDateKey', 'ShipDateKey', 'ResellerKey', 'EmployeeKey', 'PromotionKey', 'CurrencyKey', 'SalesTerritoryKey']] = df[['ProductKey',	'OrderDateKey',	'DueDateKey', 'ShipDateKey', 'ResellerKey', 'EmployeeKey', 'PromotionKey', 'CurrencyKey', 'SalesTerritoryKey']].astype(int)
        
        # create normalized table
        df_norm = df.copy()
        df_norm = df_norm.drop('index', axis = 1)
        initialize_tables_in_db(con, df_norm, table_schema, normalized_table)
        load_to_db( df_norm, table_schema, normalized_table)


# 4. Questions answered - Generate reports
    
if not os.path.exists('reports'):
    os.makedirs('reports')
else:
    shutil.rmtree('reports')
    os.makedirs('reports')
filepath = 'reports/'


qa1 = """
WITH data AS (
SELECT 
	dp."ProductKey", 
	strftime('%Y', frs."OrderDate") AS year_order,
	strftime('%m',  frs."OrderDate") AS month,
	frs."OrderDate" as order_date, 
	frs."SalesAmount" as sales_amount
FROM 
	factresellersales_normalized frs
JOIN 
	dimproduct_normalized dp
	ON dp."ProductKey" = frs."ProductKey"
WHERE 
	dp."ProductName" LIKE 'Sport-100 Helmet, Red'
	AND strftime('%Y', frs."OrderDate") = '2012'
)

SELECT 
	month, sales_amount, order_date
FROM (
	SELECT 
		month, sales_amount, order_date, 
		RANK() OVER (
		PARTITION BY month order by sales_amount desc ) as rnk
	FROM data
	ORDER BY month desc, sales_amount desc
) as base
WHERE 
	rnk = 1
order by month asc
"""
df = report_build(qa1, 'qa1', con, filepath)

sns.barplot(df.month, df.sales_amount.values, palette="Blues_d") 
plt.savefig(filepath+'qa1.png')

qa2 = """WITH data AS (
SELECT 
 dp."ProductName" AS product_name, 
 dst."SalesTerritoryCountry" AS sales_territory_country,
 strftime('%m',  frs."ShipDate") AS month,
 SUM(frs."SalesAmount") AS sales_amount

FROM 
	factresellersales_normalized frs
JOIN 
	dimproduct_normalized dp
	ON dp."ProductKey" = frs."ProductKey"
JOIN 
	dimsalesterritory_normalized dst
	ON frs."SalesTerritoryKey" = dst."SalesTerritoryKey"
WHERE 
	strftime('%Y',  frs."ShipDate") = '2012'
GROUP BY product_name, sales_territory_country, month
)

SELECT 
	DISTINCT
	month, sales_territory_country, product_name, sales_amount
FROM (
	SELECT 
		month, sales_territory_country, product_name, sales_amount,
		RANK() OVER (
		PARTITION BY month order by sales_amount asc ) AS rnk
	FROM data
	ORDER BY month DESC, sales_amount ASC
) as base
WHERE 
	rnk = 1
ORDER BY month ASC"""

df = report_build(qa2, 'qa2', con, filepath)

sns.barplot(df.month, df.sales_amount.values, hue=df.sales_territory_country)
plt.savefig(filepath+'qa2a.png')

sns.barplot(df.month, df.sales_amount.values, hue=df.product_name)
plt.savefig(filepath+'qa2b.png')

qa3 = """SELECT 
	da."AccountType",
	AVG(CASE WHEN ds."ScenarioName" LIKE 'Actual' THEN ff."Amount" END) AS ActualScenario,
	AVG(CASE WHEN ds."ScenarioName" LIKE 'Budget' THEN ff."Amount" END) as BudgetScenario, 
	AVG(CASE WHEN ds."ScenarioName" LIKE 'Forecast' THEN ff."Amount" END) AS ForecastScenario 
FROM 
	factfinance_normalized ff
JOIN 
	dimaccounts_normalized da
	ON da."AccountKey" = ff."AccountKey"
JOIN 
	dimscenario_normalized ds
	ON ds."ScenarioKey" = ff."ScenarioKey"
WHERE 
	strftime('%Y',  ff."Date")  = '2011'
GROUP BY da."AccountType"
"""

df = report_build(qa3, 'qa3', con, filepath)

img = pd.DataFrame(
    {
        'ActualScenario': list(df.ActualScenario), 
        'BudgetScenario': list(df.BudgetScenario), 
        'ForecastScenario': list(df.ForecastScenario), 
    },
    index = list(df.AccountType)
)

plot = img.plot.bar()
fig = plot.get_figure()
fig.savefig(filepath+'qa3.png')

qa4 = """
SELECT 
	dp."ProductKey",
	SUM(frs."SalesAmount") as SalesAmount,
	strftime('%m',  frs."OrderDate") AS OrderMonth
FROM 
	factresellersales_normalized frs
JOIN 
	dimproduct_normalized dp
	ON dp."ProductKey" = frs."ProductKey"
WHERE 
	strftime('%Y',  frs."OrderDate") = '2012'
	AND frs."SalesAmount" > 0
GROUP BY dp."ProductKey", OrderMonth
ORDER BY dp."ProductKey", OrderMonth asc
"""

df = report_build(qa4, 'qa4', con, filepath)

x_axis = df.OrderMonth.unique()
product_lst = list(df.ProductKey.unique())

width = 0.35      
fig, ax = plt.subplots()

for prod in product_lst:
    y = [0] * len(x_axis)
    for ix, row in df[df['ProductKey']==prod].iterrows():
        y[np.int(row['OrderMonth']) -1] =  row['SalesAmount']
    
    ax.bar(x_axis, y, width, label=str(prod))

ax.set_ylabel('SalesAmount')
ax.set_title('SalesAmount by Product by Month in 2012')

fig = ax.get_figure()
fig.savefig(filepath+'qa4.png')

qa5 = """
WITH data AS (
SELECT 
	dc."MaritalStatus" AS marital_status, 
	dc."Gender" AS gender, 
	CAST(strftime('%Y',  DATE('now')) AS integer) - CAST(strftime('%Y',  dc."BirthDate") AS intger) as age
FROM 
	dimcustomer_normalized dc
) 

SELECT 
	marital_status, 
	gender, 
	SUM(CASE WHEN age < 35 THEN 1 ELSE 0 END) AS age_less_than_35,
	SUM(CASE WHEN age >= 35 AND age <=50 THEN 1 ELSE 0 END) AS age_between_35_50,
	SUM(CASE WHEN age > 50 THEN 1 ELSE 0 END) AS age_more_than_50
FROM 
	data
GROUP BY marital_status, 
	gender
"""

df = report_build(qa5, 'qa5', con, filepath)

gender = df.groupby('gender', as_index = False)[['age_less_than_35', 'age_between_35_50', 'age_more_than_50']].sum()

img = pd.DataFrame(
    {
        'Age<35': list(gender.age_less_than_35), 
        'Age between 35-50': list(gender.age_between_35_50), 
        'Age>50': list(gender.age_more_than_50), 
    },
    index = list(gender.gender)
)

plot = img.plot.bar()
fig = plot.get_figure()
fig.savefig(filepath+'qa5a.png')

marital_status = df.groupby('marital_status', as_index = False)[['age_less_than_35', 'age_between_35_50', 'age_more_than_50']].sum()

img = pd.DataFrame(
    {
        'Age<35': list(marital_status.age_less_than_35), 
        'Age between 35-50': list(marital_status.age_between_35_50), 
        'Age>50': list(marital_status.age_more_than_50), 
    },
    index = list(marital_status.marital_status)
)
plot = img.plot.bar()
fig = plot.get_figure()
fig.savefig(filepath+'qa5b.png')
