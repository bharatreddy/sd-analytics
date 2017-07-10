if __name__ == "__main__":
    import es_utils
    import pandas
    import datetime
    esU = es_utils.ElasticUtils({"host" : "128.173.145.158",\
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
    # Get dst data from csv as an array of jsons
    dstRecs = [] 
    # read data into a pandas DF
    inpDstFile = "dst_out_file.csv"
    dstDF = pandas.read_csv(inpDstFile, sep=' ')
    # droppint duplicates
    dstDF = dstDF.drop_duplicates()
    # Iterate over rows and create dict
    # to upload to elasticsearch
    for ind, row in dstDF.iterrows():
        # We are inserting one record at a time for now
        # since the bulk insert isn't working well!
        # newdstRecs = []
        dstDict = {}
        dstDict["dst_index"] = row["dst_index"]
        epoch = datetime.datetime.utcfromtimestamp(0)
        currDtObj = datetime.datetime.strptime(row["dst_date"], '%Y-%m-%d %H:%M:%S')
        newDtObj = currDtObj.strftime( "%Y-%m-%dT%H:%M:%S" )
        dstDict["ts"] = newDtObj
        op_dict = {
            "index": {
                "_index": indexName, 
                "_type": typeName, 
                "_id": ind
            }
        }
        dstRecs.append(op_dict)
        dstRecs.append(dstDict)
        # newdstRecs.append(op_dict)
        # newdstRecs.append(dstDict)
    esU.insert_data_recs(indexName, dstRecs)