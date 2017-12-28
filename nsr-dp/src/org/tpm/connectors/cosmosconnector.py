from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from org.tpm.utils.utilities import converttostr
import pydocumentdb
import pydocumentdb.document_client as document_client
import sys,json,collections
import time,ast,uuid


class connectCosmos:

    def __init__(self):
        return
    def convert(self,data):
        return data

    def insertDocument(self,record,config):
        client = document_client.DocumentClient(config["azurecosmos"]['ENDPOINT'],
                                                {'masterKey': config["azurecosmos"]['MASTERKEY']})

        recDict = record.asDict()
        recDict['tran_id'] = str(recDict['col1'] + recDict['col2'] + recDict['col3'] + recDict['col4'] + recDict['col5'] + recDict['col6']).replace('"','')

        recDict['id'] = str(recDict['col1'] + recDict['col2'] + recDict['col3'] + recDict['col4'] + recDict['col5'] + recDict['col6']).replace('"','')
        #recAsJsonDoc = json.dumps(recDict).encode('ascii', 'replace').replace('\\"', '')
        # Read databases and take first since id should not be duplicated.
        db = next((data for data in client.ReadDatabases() if data['id'] == config["azurecosmos"]["DOCUMENTDB_DATABASE"]))
        # Read collections and take first since id should not be duplicated.
        coll = next((coll for coll in client.ReadCollections(db['_self']) if coll['id'] == config["azurecosmos"]["DOCUMENTDB_COLLECTION"]))
        # Create document
        document = client.CreateDocument(coll['_self'],json.loads(json.dumps(recDict).encode('ascii', 'replace').replace('\\"', '')))
        return

    def deleteDocument(self,record,config):
        client = document_client.DocumentClient(config["azurecosmos"]['ENDPOINT'],
                                                {'masterKey': config["azurecosmos"]['MASTERKEY']})

        recDict = record.asDict()
        recDict['tran_id'] = str(
            recDict['col1'] + recDict['col2'] + recDict['col3'] + recDict['col4'] + recDict['col5'] + recDict[
                'col6']).replace('"', '')

        recDict['id'] = str(
            recDict['col1'] + recDict['col2'] + recDict['col3'] + recDict['col4'] + recDict['col5'] + recDict[
                'col6']).replace('"', '')
        recordStr = json.dumps(recDict).encode('ascii', 'replace').replace('\\"', '')
        documents=  self.queryDocument(recordStr,config)
        for document in documents:
            client.DeleteDocument(document['_self'])

        return

    def queryDocument(self,jsonDocStr,config):
        client = document_client.DocumentClient(config["azurecosmos"]['ENDPOINT'],
                                                {'masterKey': config["azurecosmos"]['MASTERKEY']})
        # query documents
        documents = list(client.QueryDocuments(
            self.GetDocumentCollectionLink(self.GetDatabaseLink(config,client),config,client)['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.tran_id=@tran_id',
                'parameters': [
                    {'name': '@tran_id', 'value': json.loads(jsonDocStr)['tran_id']}
                ]
            }))
        print "Number of documents returned: "+str(len(documents))
        return documents

    def queryDocumentByTranID(self,tranids,config):
        client = document_client.DocumentClient(config["azurecosmos"]['ENDPOINT'],
                                                {'masterKey': config["azurecosmos"]['MASTERKEY']})
        # query documents
        documents = list(client.QueryDocuments(
            self.GetDocumentCollectionLink(self.GetDatabaseLink(config,client),config,client)['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.tran_id IN ({@tran_id})',
                'parameters': [
                    {'name': '@tran_id', 'value': tranids}
                ]
            }))
        print "Number of documents returned: "+str(len(documents))
        return documents

    def GetDatabaseLink(self, config,client):
        databaselink = next((data for data in client.ReadDatabases() if data['id'] == config["azurecosmos"]["DOCUMENTDB_DATABASE"]))
        return databaselink

    def GetDocumentCollectionLink(self, databaselink, config, client):
        # Read collections and take first since id should not be duplicated.
        collectionlink = next((coll for coll in client.ReadCollections(databaselink['_self']) if
                     coll['id'] == config["azurecosmos"]["DOCUMENTDB_COLLECTION"]))
        return collectionlink

    def callcosmosDB(self,config):
        # Initialize the Python DocumentDB client
        client = document_client.DocumentClient(config["azurecosmos"]['ENDPOINT'],
                                                {'masterKey': config["azurecosmos"]['MASTERKEY']})
        #df_withcolheaders.registerAsTable("input_transactions")
        #sqlContext.sql('select col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11 from input_transactions').show(5)
        return

