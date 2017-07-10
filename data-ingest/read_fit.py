class FitReadUtils(object):
    """
    A class to read fitacf data into arrays
    """
    def __init__(self):
        # set up urls
        self.homepage = "http://wdc.kugi.kyoto-u.ac.jp/"
        self.realTime = "dst_realtime"
        self.provisional = "dst_provisional"
        self.final = "dst_final"

    def read_fit_record_file(self, fileName):
        # given a fit record file read its contents
        # this function is taken/copied from DaVitPy.
        # open the file
        try: 
            fp = open(fileName)
        except Exception, e:
            print e
            print 'problem opening the file %s', fileName
            return None

        # read the first line
        line = fp.readline()
        cnt = 1
        while line:
            # split the lines by whitespace
            cols = line.split()
            # check for first header line
            if cnt == 1:
                d, t = cols[0], cols[1]
                # parse the line into a datetime object
                time = dt.datetime(int(d[:4]), int(d[5:7]), int(d[8:]),
                                   int(t[:2]), int(t[3:5]), int(t[6:]))
                radname = cols[2]
                filetype = cols[3]
                logging.debug(str(time))
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
                gates, pwr, vel, gsct, vel_err, width = [], [], [], [], [], []
                glat, glon, gazm, mlat, mlon, mazm = [], [], [], [], [], []

                # read all of the reange gates
                for i in range(npnts):
                    line = fp.readline()
                    cols = line.split()
                    # parse the line into the corresponding lists
                    gates.append(int(cols[0]))
                    pwr.append(float(cols[3]))
                    vel.append(float(cols[4]))
                    gsct.append(int(cols[5]))
                    vel_err.append(float(cols[6]))
                    width.append(float(cols[7]))
                    glat.append(float(cols[8]))
                    glon.append(float(cols[9]))
                    gazm.append(float(cols[10]))
                    mlat.append(float(cols[11]))
                    mlon.append(float(cols[12]))
                    mazm.append(float(cols[13]))
                # read the blank line after each record
                line = fp.readline()
                # reset the count
                cnt = 0

            # read the next line
            line = fp.readline()
            cnt += 1