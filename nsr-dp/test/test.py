from azure.storage.blob import BlockBlobService
import os
import zipfile

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from org.tpm.connectors import blobconnector
from org.tpm.connectors import cosmosconnector

import pydocumentdb;
import pydocumentdb.document_client as document_client
import sys,json



def main():
    csmsconnector = cosmosconnector.connectCosmos()
    with open(sys.argv[2]) as f:
        config = json.load(f)
    config["azureblob"]["FILE"] = sys.argv[1]
    client = document_client.DocumentClient(config["azurecosmos"]['ENDPOINT'],
                                            {'masterKey': config["azurecosmos"]['MASTERKEY']})
    recDict = {'col14': u'"CNY"', 'col15': u'"1"', 'col16': u'"4159"', 'col17': u'"7324.76"', 'col10': u'"1"', 'col11': u'"1"', 'col12': u'"-1"', 'col13': u'"-1"', 'col18': u'"0.000000000000"', 'col19': u'"0.000000000000"', 'id': 'b7d62c65-6007-454b-bd3c-959c8f69e293', 'col8': u'"-1"', 'col9': u'"-1"', 'col6': u'"CHA7"', 'col7': u'"DSD"', 'col4': u'"999"', 'col5': u'"CGC04"', 'col2': u'"20160401"', 'col3': u'"Grain-XM"', 'col1': u'"204"', 'col29': u'"201708150800"', 'col28': u'"41590"', 'col25': u'"41590"', 'col24': u'', 'col27': u'"41590"', 'col26': u'', 'col21': u'', 'col20': u'', 'col23': u'', 'col22': u'', 'tran_id': '20420160401Grain-XM999CGC04CHA7'}
    recAsJsonDoc = json.dumps(recDict)
    recAsJsonDoc = recAsJsonDoc.encode('ascii','replace').replace('\\"','')
    #print recAsJsonDoc
    #return
    # Read databases and take first since id should not be duplicated.
    #db = next((data for data in client.ReadDatabases() if data['id'] == config["azurecosmos"]["DOCUMENTDB_DATABASE"]))
    # Read collections and take first since id should not be duplicated.
    #coll = next((coll for coll in client.ReadCollections(db['_self']) if
                 #coll['id'] == config["azurecosmos"]["DOCUMENTDB_COLLECTION"]))
    # Create document
    #document = client.CreateDocument(coll['_self'], json.loads(recAsJsonDoc))
    csmsconnector = cosmosconnector.connectCosmos()
    #csmsconnector.deleteDocument(recAsJsonDoc, config)
    tranList = ["20420160401Grain-XM999CGC08CHA7","20420160401Grain-XM999CGC05CHA7"]
    csmsconnector.queryDocumentByTranID(tranList,config)
    return

if __name__ == "__main__":
    main()

