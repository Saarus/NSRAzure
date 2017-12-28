from azure.storage.blob import BlockBlobService
import os,json
import zipfile

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

class connectBlob:

    def __init__(self):
        return

    def downloadBlobs(self,config,sc,sqlContext):
        df_withcolheaders = ""
        blob_service = BlockBlobService(account_name=config["azureblob"]["ACCOUNT_NAME"], account_key=config["azureblob"]["ACCOUNT_KEY"])
        # name of the container
        blobs = blob_service.list_blobs(config["azureblob"]["CONTAINER_NAME"], config["azureblob"]["FOLDER_PATH"])
        # code below lists all the blobs in the container and downloads one blob that matches the file and break the loop
        for blob in blobs:
            print(blob.name)
            print("{}".format(blob.name))
            # check if the path contains a folder structure, create the folder structure
            if "/" in "{}".format(blob.name):
                print("there is a path in this")
                # extract the folder path and check if that folder exists locally, and if not create it
                head, tail = os.path.split("{}".format(blob.name))
                print(head)
                print(tail)
                print(config["azureblob"]["FILE"])
                if (tail.lower() == str(config["azureblob"]["FILE"]).lower()):
                    if (os.path.isdir(os.getcwd() + "/" + head)):
                        print("directory and sub directories exist")
                        blob_service.get_blob_to_path(config["azureblob"]["CONTAINER_NAME"], blob.name, os.getcwd() + "/" + head + "/" + tail)
                        df = sc.textFile(os.getcwd() + "/" + head + "/" + tail)
                        data_rdd = df.map(lambda line: [x for x in line.split(',')])
                        #df_withcolheaders = data_rdd.toDF([config["columns"]["columnheaders"]])
                        df_withcolheaders = data_rdd.toDF(
                            ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10',
                             'col11', 'col12', 'col13', 'col14', 'col15', 'col16', 'col17', 'col18', 'col19',
                             'col20', 'col21', 'col22', 'col23', 'col24', 'col25', 'col26', 'col27', 'col28',
                             'col29'
                             ])
                        break
                    else:
                        # create the diretcory and download the file to it
                        print("directory doesn't exist, creating it now")
                        os.makedirs(os.getcwd() + "/" + head)
                        print("directory created, download initiated")
                        blob_service.get_blob_to_path(config["azureblob"]["CONTAINER_NAME"], blob.name, os.getcwd() + "/" + head + "/" + tail)
                        df = sc.textFile(os.getcwd() + "/" + head + "/" + tail)
                        data_rdd = df.map(lambda line: [x for x in line.split(',')])
                        #df_withcolheaders = data_rdd.toDF([config["columns"]["columnheaders"]])
                        df_withcolheaders = data_rdd.toDF(
                            ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10',
                             'col11', 'col12', 'col13', 'col14', 'col15', 'col16', 'col17', 'col18', 'col19',
                             'col20', 'col21', 'col22', 'col23', 'col24', 'col25', 'col26', 'col27', 'col28',
                             'col29'
                             ])
                        break
            else:
                continue
        return df_withcolheaders
