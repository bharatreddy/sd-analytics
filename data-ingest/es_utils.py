class ElasticUtils(object):
    """
    A class for performing certain functions in ES
    """
    def __init__(self, ES_HOST):
        import elasticsearch
        # set up connection to ES host
        self.es = elasticsearch.Elasticsearch( hosts = [ES_HOST] )

    def createIndex( self, indexName, requestBody=None, deleteOld=False ):
        # create an index in elasticsearch
        if self.es.indices.exists(indexName):
            if deleteOld:
                print "DELETING AND RECREATING EXISTING INDEX--->",\
                     indexName
                res = self.es.indices.delete(index=indexName)
                if requestBody is not None:
                    res = self.es.indices.create( index = indexName,\
                         body = requestBody )
                else:
                    res = self.es.indices.create( index = indexName )
            else:
                print "INDEX ", indexName, " exists! Recheck!"
        return

    def get_dst_json(self, inpDstFile, indexName='indices',\
         typeName="dst_data"):
        # Get dst data from csv as an array of jsons
        import pandas
        import datetime
        dstRecs = [] 
        # read data into a pandas DF
        dstDF = pandas.read_csv(inpDstFile, sep=' ')
        # Iterate over rows and create dict
        # to upload to elasticsearch
        for ind, row in dstDF.iterrows():
            dstDict = {}
            dstDict["dst_index"] = row["dst_index"]
            epoch = datetime.datetime.utcfromtimestamp(0)
            currDtObj = datetime.datetime.strptime(row["dst_date"], '%Y-%m-%d %H:%M:%S')
            newDtObj = currDtObj.strftime( "%Y-%m-%dT%H:%M:%S" )
            dstDict["ts"] = newDtObj
            print dstDict
            op_dict = {
                "index": {
                    "_index": indexName, 
                    "_type": typeName, 
                    "_id": ind
                }
            }
            dstRecs.append(op_dict)
            dstRecs.append(dstDict)
        return dstRecs

    def insert_data_recs(self, indexName, dataRecs):
        # Insert data into elastic search
        print "inserting data into elastic search"
        res = self.es.bulk(index = indexName,\
             body = dataRecs, refresh = True)
        