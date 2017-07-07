
if __name__ == "__main__":
    import elasticInput
    esU = elasticInput.IngestElastic({"host" : "128.173.145.158",\
             "port" : 9200})
    indexName = 'indices'
    typeName = 'dst_data'
    reqBody = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },

        'mappings': {
            typeName: {
                'properties': {
                    'dst_index': {'type': 'float'},
                    'ts': {'type': 'date', 'format':'dateOptionalTime'},
               }}}
    }
    esU.createIndex( indexName, \
        requestBody=reqBody, deleteOld=True )
    dataRecs = esU.get_dst_json("dst_out_file.csv")
    esU.insert_data_recs(indexName, dataRecs)


class IngestElastic(object):
    """
    A class to insert data into Elasticsearch
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
        import pandas
        import datetime
        # Get dst data from csv as an array of jsons
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
        