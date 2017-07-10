if __name__ == "__main__":
    import fit_es_input
    frU = fit_es_input.FitESUtils()
    frU.create_fit_index()
    frU.insert_fit_records("test.txt")

class FitESUtils(object):
    """
    A class to read fitacf data records
    and write them to es
    """
    def __init__(self):
        import es_utils
        # set up connections
        self.esU = es_utils.ElasticUtils({"host" : "128.173.145.158",\
             "port" : 9200})
        self.fitIndName = "data-superdarn"
        self.fitTypeName = 'fit_data'
        # Once we cross the limit insert the recs in to es
        self.insertRecLimit = 50000

    def create_fit_index(self):
        # create fit data index for es
        reqBody = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },

            'mappings': {
                self.fitTypeName: {
                    'properties': {
                        'gates': {'type': 'integer'},
                        'ranges': {'type': 'integer'},
                        'pwr_0': {'type': 'float'},
                        'pwr_l': {'type': 'float'},
                        'vel': {'type': 'float'},
                        'gsct': {'type': 'int'},
                        'vel_err': {'type': 'float'},
                        'width': {'type': 'float'},
                        'glat': {'type': 'float'},
                        'glon': {'type': 'float'},
                        'gazm': {'type': 'float'},
                        'mlat': {'type': 'float'},
                        'mlon': {'type': 'float'},
                        'mlt': {'type': 'float'},
                        'mazm': {'type': 'float'},
                        'ts': {'type': 'date', 'format':'dateOptionalTime'},
                        'radname': {'type': 'keyword'},
                        'filetype': {'type': 'keyword'},
                        'bmnum': {'type': 'integer'},
                        'tfreq': {'type': 'integer'},
                        'scanflg': {'type': 'keyword'},
                        'npnts': {'type': 'integer'},
                        'nrang': {'type': 'integer'},
                        'channel': {'type': 'keyword'},
                        'cpid': {'type': 'integer'},
                   }}}
        }
        self.esU.createIndex( self.fitIndName, \
            requestBody=reqBody, deleteOld=True )
        print "created index--->", self.fitIndName

    def insert_fit_records(self, fileName):
        # given a fit record file read its contents
        # this function is mostly taken/copied from DaVitPy.
        # open the file
        import datetime
        from davitpy.models import aacgm
        try: 
            fp = open(fileName)
        except Exception, e:
            print e
            print 'problem opening the file %s', fileName
            return None

        # read the first line
        line = fp.readline()
        cnt = 1
        totRecCnt = 0
        fitRecs = []
        while line:
            # split the lines by whitespace
            cols = line.split()
            # check for first header line
            if cnt == 1:
                d, t = cols[0], cols[1]
                # parse the line into a datetime object
                currDt = datetime.datetime(int(d[:4]), int(d[5:7]), int(d[8:]),
                                   int(t[:2]), int(t[3:5]), int(t[6:]))
                radname = cols[2]
                filetype = cols[3]
            # check for second header line
            elif cnt == 2:
                bmnum = int(cols[2])
                tfreq = int(cols[5])
                scanflg = cols[17]
            # check for third header line
            elif cnt == 3:
                npnts = int(cols[2])
                nrang = int(cols[5])
                channel = cols[8]
                cpid = int(cols[11])
            # check for fourth header line
            elif cnt == 4:
                # empty lists to hold the fitted parameters
                gates, ranges, pwr_0, pwr_l, vel, gsct, vel_err, width = [], [], [], [], [], [], [], []
                glat, glon, gazm, mlat, mlon, mazm = [], [], [], [], [], []
                # Store the fit recs in an array to ingest them to es later
                # read all of the reange gates
                for i in range(npnts):
                    line = fp.readline()
                    cols = line.split()
                    # Enter the values in a dict
                    fitDict = {}
                    fitDict["gates"] = int(cols[0])
                    fitDict["ranges"] = int(cols[1])
                    fitDict["pwr_0"] = float(cols[2])
                    fitDict["pwr_l"] = float(cols[4])
                    fitDict["vel"] = float(cols[5])
                    fitDict["gsct"] = int(cols[6])
                    fitDict["vel_err"] = float(cols[7])
                    fitDict["width"] = float(cols[8])
                    fitDict["glat"] = float(cols[9])
                    fitDict["glon"] = float(cols[10])
                    fitDict["gazm"] = float(cols[11])
                    fitDict["mlat"] = float(cols[12])
                    fitDict["mlon"] = float(cols[13])
                    fitDict["mazm"] = float(cols[14])
                    fitDict["ts"] = currDt.strftime( "%Y-%m-%dT%H:%M:%S" )
                    fitDict["radname"] = radname
                    fitDict["filetype"] = filetype
                    fitDict["bmnum"] = bmnum
                    fitDict["tfreq"] = tfreq
                    fitDict["scanflg"] = scanflg
                    fitDict["npnts"] = npnts
                    fitDict["nrang"] = nrang
                    fitDict["channel"] = channel
                    fitDict["cpid"] = cpid
                    # get the mlt value
                    calcMlt = aacgm.mltFromYmdhms(currDt.year, \
                        currDt.month,currDt.day, currDt.hour,\
                        currDt.minute, currDt.second, fitDict["mlon"])
                    fitDict["mlt"] = round( calcMlt, 2 )
                    # store in the array
                    fit_op_dict = {
                        "index": {
                            "_index": self.fitIndName, 
                            "_type": self.fitTypeName
                        }
                    }
                    fitRecs.append(fit_op_dict)
                    fitRecs.append(fitDict)
                    totRecCnt += 1
                # read the blank line after each record
                line = fp.readline()
                # reset the count
                cnt = 0
                if cnt == 0:
                    # if there are too many records
                    # insert them into es and read fresh one's
                    if len( fitRecs ) > self.insertRecLimit:
                        self.esU.insert_data_recs(self.fitIndName, fitRecs)
                        print "---dumped " + str(totRecCnt) + " records into es---"
                        fitRecs = []
                        print "reading fresh records!"
            # read the next line
            line = fp.readline()
            cnt += 1
        print "---dumping final " + str(len(fitRecs)) + " records into es---"
        self.esU.insert_data_recs(self.fitIndName, fitRecs)