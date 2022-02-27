# remote-dataeng

The objective of this repo is to generate a pipeline that load the data from a single spreadsheet, cleanse and transform the data to handle data corruption, performs some preliminary analysis on the data.

Lastly, using the cleansed data, I will anwer the specific questions asked.

# How-to

I created an init-script that should bootstrap the environment and packages needed to run the main.py (please consider the code in the "code" folder as the main working prototype). This is the main script that creates the ETL pipe, the DB, and generate the plots to answer the questions. The steps to use this repo:

1. git clone this repo in your working directory
2. within the Terminal use `bash Miniconda3-latest-MacOSX-x86_64.sh` to install MiniConda (you should be in the parent directory with the bash file already present)
3. inside the /code folder launch `sh init-script.sh` using Terminal. This script should initialize the environment and install all the required packages. It also runs the main script directly.

You probably need to answer few interactive questions in the terminal due to conda installation, however, these interactions should be minimal.

Once the script is loaded, the pipeline is run, the local SQLite DB is created and the reports are saved in the folder "reports".

The File "Data Analyst Assignment_2.xlsx" can be used to test the pipeline with only two sheets.

I also provided the Jupyter notebooks I used to do some basic EDA on each table and the full answers, in case something goes wrong with the main script.

# The rationale behind the data ingestion workflow

I attempted to use simple technologies, given the size of the dataset and the timeframe available. The decisions made, may not be the right ones as the data grows in size and as the flow becomes more complex. Where possible, I will provide my perspective of what I think can/should be improved to scale the flow.

The ingestion pipeline alone (read raw data, cleansing and loading into a SQLLite) takes ca 3min. It is a lot, given that we have few data. This is definitely a bottleneck and further iterations should increase the speed of this process using parallelization. For instance, we should think about implementing Dask or PySpark to parallelize the code and use either multiple nodes Virtual machines or cloud resources with multiple corse to speed up the ingestion as the data size scales. git

The principles I'm trying to follow to create this pipeline:

1. Assuming this is a first data ingestion of raw data. From a unique raw source, my objective is to load the raw data as quickly as possible and "untouched". This means that I decided to add two steps where I first ingest the data into a raw table and then load again the same data polished for future Analytics endeavours.

   - ideally, we would want to scale this system using some form of snapshot, such as delta tables that carry the information as timestamp of when the table is created. This is helpful if failures during the data ingestion occurs.
   - To simulate this, I added two columns (created_at, updated_at) to the tables that in the absence of delta tables, could be used to determine the updates of the tables and the date of initial creation

2. The pipeline is as much as possible parametrized and built using "atomic" components. I tried as much as possible to use simple and atomic functions that receive generic parameters. This approach, in my opinion, has at least two advantages:

   - The atomic components can be used elsewhere from other members of the team. the fact that this is parametrized, it means that is as much as possible portable and generalizable
   - The atomization of the process increases the chances of debugging effectiveness and rapidity. Having atomic components that works as a step-wise "flow" helpd determining the point of failure

3. The presence of step-wise atomic components should facilitate the repeatibility and repititivity of the pipeline. The initial process is repeatable, in the sense that riunning the pipeline multiple times doesn't generate duplicates, as every time the tables are recreated, not inserted.
   - This is probably not scalable as the data grows in size and this is where I see delta tables come in handy

# The checks implemented and their rationale

All the validation checks are applied after we load the raw data into the DB. Once the checks are performed, the sanitized data is loaded again into the DB ina new table with the suffix "\_normalized". These checks, once found a data issues that cannot be easily corrected using best judgement, will log the issue (row of the dataframe) into a specific error_logger table containing the row of the original dataframe and the type of issue found (i.e. "duplication"). This should help further investigation from the team, leveraging SMEs in the case of business related data.

1. The first check I want to do is to check for duplication in the rows. This is extremely important as we want to load the raw data into tables that will serve Analytics purposes. Especially, if the data need to be joined togheter, we want to check if the data has duplication and if so, if it is correct that there should be a duplication in key values. For example, a master key table that should serve as a junction point between multiple tables should always have primary keys (thus not duplicated rows). Even though it is important to have business context, I tried to use my best judgement here.
   - From my current exploration it seems that all the tables have unique rows.
   - The duplication check performed goes into the tables and evaluate the number of duplicated rows. It then slice the data for these duplicated rows and log them into a separate table with the suffix "\_corrupted_data_logger"
2. The second check I want to perform is to evaluate the consistency of the data types. The following checks are performed and when an issue is found, is often automatically corrected in the "\_normalized" table:
   - Date consistency. All the raw dates are converted into timestamps

# Analysis on each table

1. DimAccounts

   - This table looks like a primary key table used to perform further joints.
   - The "AccountKey" is unique. So it can be used as PrimaryKey
   - From my exploration I noticed a potentially corrupted cell in the column "customermembers". All the cells are empty, but one has a list of items. However, this list is not explicative in the context of the table. I decide to log this value into the data error logger for this table to further exploration down the line.

2. DimCustomer

   - This table looks like a list of customers with customer info
   - The column "CustomerKey" is unique and thus can be used as primaryKey for the table
   - From my exploration, it looks like some automatic data validation checks can be performed to improve the dates, the phone number format
   - Both the BirthDate column and the DateFirstPurchase have dateformat issues. In particular, some rows have the format of "dd/mm/yy". In those cases, it is hard to impute these values automatically, as we are missing the important information about the year. In these cases, there are two potential approaches, that I can come up with:
     - Impute the year based on the context of the dataframe. Based on the plot of the year dates of the Birthdates, I decided to impute the dates of the BirthDate as all belonging to the 1900. However, this might not be exactly correct and further investigation might be needed. Thus I also logged this rows into the data_logger table. A similar approach is taken for the FirstSalesDate, however in that case, the imputation of the date seem to fall always within the range of the broader dataset.

3. DimProducts

- Also this table looks like a product list with key for further joins. It doesn't look like there are duplicates.
- The column ProductKey is unique and can be used as PrimaryKey
- There are few products with no StartDate. I'd be curious to know if this means if these products have never been sold or are prototype. i also notice that these products have a "None" status. Without additional context, I decide to keep these rows inside the normalized table, but flag them inside the error_logger table for further inspection.
- I will probably create the answers to the final questions with and without these productsm just in case. I have the feeling that these products are either dismissed or not sold yet!
- Plotting the StandardCost and the DealerPrice I notice consistency between the two. This makes sense, as I would expect that the DealerPrice is within range (but higher) than the product costs. It seems that this is the case, as the distribution seems shifted toward the right for the DealerPrice
- I notice that ProductSubCategory can be nan. However, without additional context, I decide to leave the Nan values.

4. DimSalesTerritory

- This seems a junction table. Not much to add here, rather than the last row seems a corrupted one with all NULL values. I decide to remove this from the normalized table
- The column "SalesTerritoryKey" is unique and can be used as PrimaryKey

5. DimScenario

- No additional EDA needed for this one
- The column ScenarioKey is unique and can be used as PrimaryKey

6. FactFinance

   - No apparent Nan values
   - The column FinanceKey is unique and can be used as PrimaryKey
   - A quick visualization on the Finance shows that there is a seasonal trend of increasing Amount (of gross sales? ) year over year and also, yearly. There is a "Sales" depression every January. It seems that the company has a recurring cycle YoY but the trend is growing over time
   - I decide to copy into the normalized table directly, without further modification if not for the Key. I'm ok having the DateKey being left as int, as this could come in handy for visualization and it could be used to standardize the time as ISOYEARWEEK, if neede down the line

7. FactResellerSales

   - No apparent Nan values
   - This table is a fact table and has multiple foreign key. It seems to have weekly timeframe.
   - The analysis of the OrderQuantity distribution shows that the Order are compressed within 0-10. The order trends seems fairly constant in the entire lenght of the timeframe, however, it seems that we have some order spikes in the last part of 2013. When I plot the OrderQuantity using the Shipment date, there is more coherence with the past trend. At this stage, I'm not sure if this behaviour is a corruption of the OrderDate or a trend that happened. However, it looks like there is indeed a depression of Orders when looking at the ShipmentDate and OrderQuantity.
   - The analysis of the Price Unit distribution is in line with what observed in the FactFinance table.

# Further improvements

1. It would be imperative that, if this script needs to be passed through a team, a more reliable and robust way of setting up environments is provided. Using an init script is fine but it is very dependant on the system of the local machine and prior configurations. An ideal case scenario would be to use Docker to create a portable container with the project environment and basic requirements to run the pipeline.
2. The use of Pandas to load into a DB is a bottleneck as the data size scale. I propose to move to Dask or PySpark and parallelize the load and transform.
3. Loggers to facilitate debugging but also to track the performance of the pipeline and the recording of metrics
4. A more robust DB integration. SQLLite is simple and ok for prototyping and for proof of concepts (or to share code between colleagues, if the datum is not extensive). However, it doesn't have the power of a fully fledged DB like Postgres or SQL server. At scale, a more robust implementation should be considered. However, I decided to use this simple DB version because it comes directly with the Python installation and this means high level of portability. I thought about implementing everything in Postgres and create a dockerization environment, but it would have been an over-engineerization of the scope. However, I provide an additional folder in the repo named "postgres_proof_of_concept" where the pipeline can be executed while connecting to a Postgres DB. The config json provides an easy way to configure the DB connection if needed. This is supposed to be an example about how the pipeline can be easily converted to be used with other storage and DB. This version of the script is less curated and it is very much a proof of concept leveraging the same steps and functions used in the main pipeline.
