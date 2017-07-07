
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
                    'date': {'type': 'string'},
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
                     INDEX_NAME
                res = es.indices.delete(index=INDEX_NAME)
                if requestBody is not None:
                    res = es.indices.create( index = INDEX_NAME,\
                         body = requestBody )
                else:
                    res = es.indices.create( index = INDEX_NAME )
            else:
                print "INDEX ", INDEX_NAME, " exists! Recheck!"
        return

    def get_dst_json(self, inpDstFile, indexName='indices',\
         typeName="dst_data"):
        import pandas
        # Get dst data from csv as an array of jsons
        dstRecs = [] 
        # read data into a pandas DF
        dstDF = pandas.read_csv(inpDstFile, sep=' ')
        # Iterate over rows and create dict
        # to upload to elasticsearch
        for ind, row in dstDF.iterrows():
            dstDict = {}
            dstDict["dst_index"] = row["dst_index"]
            dstDict["date"] = row["dst_date"]
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
        res = self.es.bulk(index = indexName,\
             body = dataRecs, refresh = True)
        