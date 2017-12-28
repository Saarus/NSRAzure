from azure.storage.blob import BlockBlobService
from org.tpm.connectors import blobconnector
from org.tpm.connectors import cosmosconnector
import os,json,traceback
import zipfile
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext


def main():
    try:
        exc_info = sys.exc_info()
        with open(sys.argv[2]) as f:
                config = json.load(f)
        config["azureblob"]["FILE"] = sys.argv[1]
        sc = SparkContext('local')
        sqlContext = SQLContext(sc)
        blbconnector = blobconnector.connectBlob()
        df_from_blob_withcolheaders = blbconnector.downloadBlobs(config,sc,sqlContext)
        csmsconnector = cosmosconnector.connectCosmos()
        df_from_blob_withcolheaders.rdd.foreach(lambda x: csmsconnector.insertDocument(x,config))
    except:
        traceback.print_exc()
        pass
    finally:
            traceback.print_exception(*exc_info)
            del exc_info


if __name__ == "__main__":
    main()

