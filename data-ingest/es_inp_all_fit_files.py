if __name__ == "__main__":
    import fit_es_input
    import os
    indir = '/home/bharat/Documents/sd-es-vels/'
    # create index
    frU = fit_es_input.FitESUtils()
    frU.create_fit_index()
    for root, dirs, filenames in os.walk(indir):
        for indf, fn in enumerate( filenames ):
            print "current file------------------------>", \
                fn, str(indf+1) + "/" + str( len(filenames) )
            try: 
                frU.insert_fit_records(indir+fn)
            except Exception, e:
                print e
                print '****problem indexing the file------------->', fn
