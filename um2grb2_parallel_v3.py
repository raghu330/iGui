#!/usr/bin/env python

__author__ = 'raghav, arulalant'

"""
What does this code piece do?
This code converts 6-hourly UM fields file data into grib2 format
after regridding the data to 0.25x0.25 degree spatial resolution.
This is just test code as of now and is meant for a specific purpose only
This code conforms to pep8 standards.

Parallel:
As for now, we are using multiprocessing to make parallel run on different files
like pb, pd, pe and its creating child porcess with respect to no of forecast
hours. To make more parallel threads on variable, timeIndx level we 
may need to use OpenMPI-Py. 

Output:
This script produce output files as multiple 6 hourly forecasts data from 
different input files such as pd, pd, pe, etc., So all 6 hourly forecasts data 
of differnt input files will be append to same 6 hourly grib2 outfiles.

Contributors:
#1. Mr. Raghavendra S. Mupparthy (MNRS)
#2. Dr. Devjyoti Dutta (DJ)
#3. Mr. Arulalan T (AAT)
#4. Dr. Jayakumar A. (JA)

Testing
#1. Mr. Kuldeep Sharma (KS)
#2. Mr. Raghavendra S. Mupparthy (MNRS)
#3. Dr. Raghavendra Ashrit (RA)
#4. Mr. Gopal Raman Iyengar (GRI)

Acknowledgments:
#1. IBM Team @ NCMRWF for installation support on Bhaskara - Shivali & Bangaru
#2. Dr. Rakhi R, Dr. Jayakumar A, Dr. Saji Mohandas and Mr. Bangaru for N768

Code History:
1. Jul 22nd, 2015: First version by MNRS
2. Jul 24th, 2015: Grib section editor - version-0.1,
                 : Automation of filenames started
                 : Extraction of required variables
                 : Interpolation scheme to 0.25 degree
3. Sep 11th, 2015: Recasted for 6-hourly ouputs
4. Nov 05th, 2015: Changed dummy to a string in getVarIdx()
5. Nov 07th, 2015: Added to iGui project on github
6. Nov 09th, 2015: Made it parallel (by AAT)
7. Nov 10th, 2015: Spawned multiple versions for input..
8. Nov 12th, 2015: Appending same 6 hourly forecast data of differnt input 
                   files into same 6hourly grib2 files. (by AAT)

References:
1. Iris. v1.8.1 03-Jun-2015. Met Office. UK. https://github.com/SciTools/iris/archive/v1.8.1.tar.gz
2. myLog() based on http://mail.python.org/pipermail/python-list/2007-May/438106.html
3. Data understanding: /gpfs2/home/umfcst/ShortJobs/Subset-WRF/ncum_subset_24h.sh

Copyright: ESSO-NCMRWF,MoES, 2015.
"""

# -- Start importing necessary modules
import os, sys, time
import numpy, scipy
import iris
import gribapi
import netCDF4
import iris.unit as unit
import multiprocessing as mp
# We must import this explicitly, it is not imported by the top-level
# multiprocessing module.
import multiprocessing.pool as mppool
import types

from datetime import datetime
# End of importing business

# -- Start coding

# create a class for capturing stdin, stdout and stderr
class myLog():
    def __init__(self, logfile):
        self.stdout = sys.stdout
        self.flush = sys.stdout.flush
        self.log = open(logfile, 'w')

    def write(self, text):
        self.stdout.write(text)
        self.log.write(text)
        self.log.flush()

    def close(self):
        self.stdout.close()
        self.log.close()

# start definition files
def getCubeData(umFname):
    """
    This module is meant to read the input file name and location as a
    string and it returns the path as aa string. An upgraded version uses
    a GUI to read the file.
    inputs:
    #1. Data directory path
    #2. UM fieldsfile filename
    returns:
    #1. Iris cube for the corresponding data file
    """

    cubes = iris.load(dataDir+umFname)

    return cubes
# end of definition-1

def getVarIdx(dummy,cube):
    """
    This module gets the required variables from the passed cube as per the
    WRF-Variables.txt file. (matches the contents of pgp06prepDDMMYY)
    inputs:
    1. a dummy variable for file name
    returns:
    1. The cube index
    2. Number of variables
    3. Number of levels
    4. Time slices or Index
    """

    if dummy == 'umglaa_pi':              # umglaa_pi
        varIndx = [1,2]
        nVars = len(cube)
        varLvls = 4
        timeIndx = []
    elif dummy == 'umglaa_pd':            # umglaa_pd
        # consider variable
        varIndx = [1,2,3,6,7]
        nVars = len(cube)
        varLvls = 18
        timeIndx = [1,3,5,7]
    elif dummy == 'umglaa_pb':            # umglaa_pb
        varIndx = [25,29,30,27,28,26]
        nVars = len(cube)
        varLvls = 0
        timeIndx = [1,3,5,7]
    elif dummy == 'umglaa_pe':            # umglaa_pe
        varIndx = [4,5,7,8,10,12,13,14,16]
        nVars = len(cube)
        varLvls = 0
        timeIndx = [5,11,17,23]

    # end if-loop

    return varIndx, nVars, varLvls, timeIndx
# end of definition-2

def getDataAttr(tmpCube):
    """
    This module returns basic coordinate info about the data cube.
    Its input is:
    #1. temporary cube containing a single geophysical field/parameter
    Its outputs are:
    #1. fcstTm -- forecast time period for eg: 00, 06, 12 etc -- units as in hours
    #2. refTm -- reference time -- units as date  in Gregorian
    #3. lat as scalar -- units as degree (from 90S to 90N)
    #4. lon as scalar -- units as degree (from 0E to 360E)
    """

    stdNm = tmpCube.standard_name
    fcstTm = tmpCube.coord('forecast_period')
    refTm = tmpCube.coord('forecast_reference_time')
    lat = tmpCube.coord('latitude')
    lon = tmpCube.coord('longitude')

    return stdNm, fcstTm, refTm, lat, lon
# end of definition-3


class NoDaemonProcess(mp.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(mppool.Pool):
    ### http://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
    ## refer the above link to invoke child processes
    Process = NoDaemonProcess
    
def regridAnlFcstFiles(arg):
    """
    function : regridAnlFcstFiles
    Args : fnames, hr
        fnames : common filename
        hr : forecast hour 
    Output : This function read the data from fieldsfile and do linear regrid to 
             0.25x0.25 resolution and write into grid2 output file per analysis
             /forecast files.
    """
    fnames, hr = arg 
    fname = fnames + hr
    
    outfile = fnames.split('_')[0] + '_pb_pd_pe' #+ hr
    
    print "Started Processing the file: %s.. \n" %fname
    # call definition to get cube data
    cubes = getCubeData(fname)
    # call definition to get variable indices
    varIdx, nVars, varLvls, timeIndx = getVarIdx(fname[0:-3],cubes)
    
    # open for-loop-3 -- for all the variables in the cube
    for ii in range(len(varIdx)):
        stdNm, _, _, _, _, = getDataAttr(cubes[varIdx[ii]])
        print "  Working on variable: %s \n" %stdNm
        for jj in timeIndx:
            # parallel loop-3 -- runs through the selected time slices - synop hours            
            # create the no of child parallel processes
            _, fcstTm, _, _, _ = getDataAttr(cubes[varIdx[ii]][jj])
            print "   Working on forecast time: %02dz\n" %fcstTm.points
            # grab the variable which is f(t,z,y,x)
            # tmpCube corresponds to each variable for the SYNOP hours
            tmpCube = cubes[varIdx[ii]][jj]
            # get original lats and lons
            _, _, _, lat0, lon0 = getDataAttr(tmpCube)
            # interpolate it 0,25 deg resolution by setting up sample points based on coord
            print "    Regridding data to 0.25x0.25 deg spatial resolution \n"            
            regdCube = tmpCube.interpolate(sp,iris.analysis.Linear())
            # get the regridded lat/lons
            stdNm, fcstTm, refTm, lat1, lon1 = getDataAttr(regdCube)
            # save the cube in append mode as a grib2 file
            outFn = opPath + outfile + '_' + current_date +'_'+str(int(fcstTm.points)).zfill(3)+'.grib2'
            iris.save(regdCube, outFn, append=True)
            ## edit location section in grib2 to point to the right RMC
            # gribapi.grib_set(outFn,'centre','28')
            # os.system('source /gpfs2/home/umtid/test/grb_local_section.sh')
        # end of for jj in timeIndx:
    # end of for ii in range(len(varIdx)):
    # make memory free
    del cubes
    
#    print "  Finished converting file: %s into grib2 format for fcst time: %02dz \n" %(fname,fcstTm.points)
    print "  Time taken to convert the file: %8.5f seconds \n" %(time.time()-startT)
    print " Finished converting file: %s into grib2 format for fcst file: %s \n" %(fname,hr)
# end of def regridAnlFcstFiles(fname):


def doConvert(fname):
    fcst_times = ['000','024','048','072','096','120','144','168','192','216']
    fcst_filenames = [(fname, hr) for hr in fcst_times]
    nchild = len(fcst_times)
    # create the no of child parallel processes
    inner_pool = mp.Pool(processes=nchild)
    print "Creating %i (daemon) workers and jobs in child." % nchild
    
    # pass the forecast hours as argument to take one fcst file per process / core to regrid it.
    results = inner_pool.map(regridAnlFcstFiles, fcst_filenames)
    # closing and joining child pools      
    inner_pool.close() 
    inner_pool.join()
    # parallel end
    print " Time taken to convert the all fcst files: %8.5f seconds \n" %(time.time()-startT)
# end def doConvert(fname):
    

# Start the main function
def main(fnames1):
    """
    Main function calling all the sub-functions
    """
    ## get the no of files and 
    nprocesses = len(fnames1)
    # lets create no of parallel process w.r.t no of files.
    pool = MyPool(nprocesses)
    print "Creating %d (non-daemon) workers and jobs in main process." % nprocesses
    results = pool.map(doConvert, fnames1)
    # closing and joining master pools         
    pool.close()     
    pool.join()
    # parallel ended
        
    print "Total time taken to convert %d files was: %8.5f seconds \n" %(len(fnames1),(time.time()-startT))
    cmdStr1 = 'mv log1.log '+wrkngDir+fnames1[0][0:7] + 'stdout_'+ current_date +'.log'
    os.system(cmdStr1)
    return
# end of def main(fnames1):


if __name__ == '__main__':
    
    # filenames partial name
    fnames1 = ['umglaa_pb', 'umglaa_pd','umglaa_pe']
    
    # get the current date in YYYYMMDD format
    current_date = time.strftime('%Y%m%d')
    print "\n current_date is %s" % current_date
    sys.stdout = myLog("log1.log")

    # start the timer now
    startT = time.time()

    # set-up base folders
    wrkngDir = '/gpfs2/home/umtid/test/'
    dataDir = '/gpfs3/home/umfcst/NCUM/fcst/' + current_date + '/00/'
    opPath = '/gpfs2/home/umtid/test/GRIB-parallel/'
    # target grid as 0.25 deg resolution by setting up sample points based on coord
    sp = [('longitude',numpy.linspace(0,360,1440)),('latitude',numpy.linspace(-90,90,721))]
    main(fnames1)
    

# -- End code
