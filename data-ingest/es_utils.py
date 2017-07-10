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

    def insert_data_recs(self, indexName, dataRecs):
        # Insert data into elastic search
        print "inserting data into elastic search"
        res = self.es.bulk(index = indexName,\
             body = dataRecs, refresh = True, request_timeout=300)
        