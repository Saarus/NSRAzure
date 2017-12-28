
Data processing code sample using pySpark,PyDocumentDB for CosmosDB and Azure Blob Storage

Setup resources/config.json with your environment specific settings

Create Python virtual environment using requirements.txt file

Setup pyCharm debug environment with these settings:

script - NSRAzure/nsr-dp/src/_main_.py

script parameters - "sample-data-file.csv" "../resources/config.json"

PYTHONPATH = $PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip

PYTHONUNBUFFERED = 1

SPARK_HOME= $spark-install-directory/spark-2.2.0-bin-hadoop2.7

PYSPARK_SUBMIT_ARGS= --packages com.databricks:spark-csv_2.11:1.5.0 pyspark-shell -file

Working directory  = NSRAzure/nsr-dp/src
