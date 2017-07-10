if __name__ == "__main__":
    import fit_dwnld_data
    import datetime
    sTime = datetime.datetime( 2011, 1, 1, 0, 0 )
    eTime = datetime.datetime( 2011, 2, 1, 0, 0 )
    parentDir = "/home/bharat/Documents/sd-es-vels/"
    fdU = fit_dwnld_data.FitDownloadRecs(sTime, eTime, parentDir)
    selRadList = fdU.get_active_rads()
    fdU.download_data(selRadList)

class FitDownloadRecs(object):
    """
    A class to fit data recs
    """
    def __init__(self, startTime, endTime, parentDir="./"):
        import datetime
        # set up connections
        self.startTime = startTime
        self.endTime = endTime
        self.parentDir = parentDir

    def get_active_rads(self):
        # get a list of all active radars
        from davitpy.pydarn.radar import *
        allRads = network()
        radCodeList = []
        for rads in allRads.radars:
            # only get those radars which started
            # before starttime of the class
            if rads.stTime <= self.startTime:
                radCodeList.append( rads.code[0] )
        return radCodeList

    def download_data(self, radList):
        # loop through days and create a data file
        # for each radar for each day
        import datetime
        from davitpy import pydarn
        cDate = self.startTime
        nDate = cDate + datetime.timedelta(days=1)
        while cDate < self.endTime:
            for cRad in radList:
                currFName = self.parentDir + \
                    cDate.strftime( "%Y-%m-%dT%H:%M:%S" ) + "--" +\
                     nDate.strftime( "%Y-%m-%dT%H:%M:%S" ) + "--" +\
                      cRad + ".txt"
                print "working with", currFName
                res = pydarn.plotting.printRec.fitPrintRec(cDate, nDate,\
                         cRad, currFName )

            cDate = nDate
            nDate += datetime.timedelta(days=1)