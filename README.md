# remote-dataeng

The objective of this repo is to generate a pipeline that load the data from a single spreadsheet, cleanse and transform the data to handle data corruption, performs some preliminary analysis on the data and provides some useful KPIs to interpret the data (both in the form of metrics and visualizations.)

Lastly, using the cleansed data, I will anwer the specific questions asked.

# The rationale behind the data ingestion workflow

I attempted to use simple technologies, given the size of the dataset and the timeframe available. The decisions made, may not be the right ones as the data grows in size and as the flow becomes more complex. Where possible, I will provide my perspective of what I think can/should be improved to scale the flow.

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
2. The second check I want to perform is to evaluate the consistency of the data types. The following checks are performed and when an issue is found, is often automatically corrected in the \_normalized table:
   - Date consistency. All the raw dates are converted into timestamps

# Analysis on each table

1. DimAccounts

   - This table looks like a primary key table used to perform further joints.
   - From my exploration I noticed a potentially corrupted cell in the column "customermembers". All the cells are empty, but one has a list of items. However, this list is not explicative in the context of the table. I decide to log this value into the data error logger for this table to further exploration down the line.

2. DimCustomer
   - This table looks like a list of customers with customer info
   - From my exploration, it looks like some automatic data validation checks can be performed to improve the dates, the phone number format
   - Both the BirthDate column and the DateFirstPurchase have dateformat issues. In particular, some rows have the format of "dd/mm/yy". In those cases, it is hard to impute these values automatically, as we are missing the important information about the year. In these cases, there are two potential approaches, that I can come up with:
     - Impute the year based on the context of the dataframe. Based on the plot of the year dates of the Birthdates, I decided to impute the dates of the BirthDate as all belonging to the 1900. However, this might not be exactly correct and further investigation might be needed. Thus I also logged this rows into the data_logger table. A similar approach is taken for the FirstSalesDate, however in that case, the imputation of the date seem to fall always within the range of the broader dataset.

# Things I would like to add

1. Loggers to facilitate debugging but also to track the performance of the pipeline and the recording of metrics
2. The usage of Pyspark. Refactoring the code in pyspark would make it BigData ready
