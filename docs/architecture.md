## Architecture

![Architecture](./architecture.png)

* 1 ingestor process that will read lines from NEM12 file, consolidate the 200 Block and pass it to the Taskmanager.
* Taskmanager will distribute the 200 blocks to executors.
* N executor processes that will process the 200 block, sum up the consumption and output them into either a CSV or SQL file.

> Need to use multi-processing to avoid GIL for CPU-bound workloads.

## Section to Fill

### Q1: What does your repository do?

NEM12 data loader is a CLI tool that will ingest a NEM12 metering data file and generate the corresponding SQL `INSERT` statements or CSV files with aggregated daily consumption per NMI.

### Q2: What are the advantages of the technologies you used for the project?

The main programming language used is Python, which is universally used in data engineering. Many large-scale data processing frameworks/platforms have Python SDKs, e.g Airflow, Apache Spark, Apache Parquet etc.

### Q3: How is the code designed and structured?

* 1 ingestor process that will read lines from NEM12 file, consolidate the 200 Block and pass it to the Taskmanager.
* Taskmanager will distribute the 200 blocks to executors.
* N executor processes that will process the 200 block, sum up the consumption and output them into either a CSV or SQL file.

### Q4: How does the design help to make the codebase readable and maintainable for other engineers?
### Q5: Discuss any design patterns, coding conventions, or documentation practices you implemented to enhance readability and maintainability?

Given the scope of the project, we adopt a simple flat directory that stores all the necessary logic. Each python module is coherent and contains code under the same domain.

The main entry point is found in `main.py` and we use dependency injection to make it clear which part of the code depends on which other parts. Dependency injection helps to keep the code loosely coupled and make writing testing code easier.

### Q6: What will you do better next time?
### Q7: Reflect on areas where you see room for improvement and describe how you would approach them differently in future projects?

* Support remote file ingestion from a data lake like S3.
* Support statistics generation so that we can validate the correctness of the data pipeline.

### Q8: What other ways could you have done this project?
### Q9: Explore alternative approaches or technologies that you considered during the development of the project?

An alternative approach that was considered was to use Apache Spark as the data processing framework to process the NEM12 meter data.

The following steps illustrates the data pipeline:

1. Augment 300 records with their NMIs during the ingestion phase and stored the augmented records in a temporary CSV.
2. Use Apache Spark to
   1. Sum up the interval values for each 300 record row into a new column `consumption`
   2. GroupBy (nmi, interval_date) and sum `consumption` across rows with the same key.
   3. Dump the dataframe into a CSV or SQL `INSERT` statements.

Check out `spark.py` for a example of using Apache Spark.

For larger cities like Tokyo, Shanghai, this approach might be more reliable and scalable as we break up the data pipeline into multiple steps that can be checkpointed while the large dataset can be processed by multiple executors concurrently.

However, this assumes that all the NEM12 200 records uses the same interval length. If it doesn't, during the ingestion stage, we can coalesce smaller intervals into larger intervals (e.g sum 2 15mins to get 1 30min).
