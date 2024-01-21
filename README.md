# End-to-End AWS Data Pipeline for Spotify Streaming Analytics
![image](https://github.com/nikitgoku/aws_data_engineering_e2e/assets/114753615/88d7cf3e-9f88-45a9-8389-ace0e1d05b36)

## **Overview**
This project consist of an end-end data pipeline where personal Spotify streaming data is utilized stored in `.csv` format and seamlessly uploaded it into `Amazon S3`. A database is then established employed by `Glue Crawler` to intricately analyze the streaming data stored in `S3`, determining its schema. Subsequently, `Amazon Glue ETL job` was used to orchestrate the data pipeline, using an `Apache Spark` script to adeptly convert the `.csv` data into the efficient `.parquet` format. The transformed data was then stored back in `S3`, paving the way for insightful and interactive queries. To achieve this, Amazon Athena was employed, allowing to extract meaningful insights from the parquet database.

## **Extract**
The personal streaming data was requested and then stored in `.csv` file in `Amazon S3`. The required IAM role and IAM policies were employed in order to include AWS Glue for the jobs moving forward.

## **Transform**
To start the transformation job, a `Glue Data Catalog` service with a database and table was created. This table stores the metadata associated the object which in the current scenario is the `.csv`. `AWS GLue Crawler` was incorporated to infer the schema of the S3 object.

After sucessfully incorporating the table with the necessary schema a `Glue ETL job` was created whose main was to modify the source CSV file using the Glue Data Catalog and upload the modified data frame in the parquet format into S3 and create a corresponding target data catalog that keeps the metadata information of the target object.

A `Spark` script was developed which utilized Glue's `dynamic frame` converted into `spark dataframe` to drop unnecessary columns, remove NULL values, rename the columns with relevant names and extract month and day details from timestamp columns. The final `spark dataframe` was converted back into Glue `dynamic frame` and saved as a `.parquet` file back into `S3` along with the corresponding table in the Glue Data Catalog.

With the Glue Data Catalog created after running the `spark script`, Amazon Athena was used to run queries on the data to get relevan insigths from the data.
