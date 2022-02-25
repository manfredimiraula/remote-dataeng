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

# Things I would like to add

1. Loggers to facilitate debugging but also to track the performance of the pipeline and the recording of metrics
2. The usage of Pyspark. Refactoring the code in pyspark would make it BigData ready
