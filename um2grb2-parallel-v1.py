#!/usr/bin/env python

__authors__ = 'raghav, arulalant'

"""
What does this code piece do?
This code converts 6-hourly UM fields file data into grib2 format
after regridding the data to 0.25x0.25 degree spatial resolution.
This is just test code as of now and is meant for a specific purpose only
This code conforms to pep8 standards.

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

Parallel:
As for now, we are using multiprocessing to make parallel run on different
forecast hours. To make more parallel threads on variable, timeIndx level we 
may need to use OpenMPI-Py. 

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
6. Nov 09th, 2015: Made it parallel by Arul
7. Nov 10th, 2015: Spawned multiple versions for input..

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
from datetime import datetime
# End of importing business

# -- Start coding
# set-up lut of metadata
print ('\n Enter the file you want to process, choices are pb, pd and pe only')
input0 = raw_input(" Enter your choice: ").strip()
# fnames1 = ['umglaa_pb','umglaa_pd','umglaa_pe']
fnames1 = 'umglaa_'+input0

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

sys.stdout = myLog("log1.log")

# ask for input data -- temporarily interactive, cron version to follow
input1 = raw_input("\n Enter the date you want to process in YYYYMMDD format: ")
print "\n Your input was %s" %input1
# start the timer now
startT = time.time()

# set-up base folders
wrkngDir = '/gpfs2/home/umtid/test/'
dataDir = '/gpfs3/home/umfcst/NCUM/fcst/'+str(input1)+'/00/'
opPath = '/gpfs2/home/umtid/test/GRIB-parallel/'
# target grid as 0.25 deg resolution by setting up sample points based on coord
sp = [('longitude',numpy.linspace(0,360,1440)),('latitude',numpy.linspace(-90,90,721))]

# start definition files
def getCubeData(umFname):
    """
    This module is meant to read the input file name and location as a
    string and it returns the path as a string. An upgraded version uses
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
    
def regridAnlFcstFiles(arg):
    """
    function : regridAnlFcstFiles
    Args : fnames, lol
        fnames : common filename
        lol : forecast hour 
    Output : This function read the data from fieldsfile and do linear regrid to 
             0.25x0.25 resolution and write into grid2 output file per analysis
             /forecast files.
    """
    fnames, lol = arg 
    fname = fnames + lol
    print "Started Processing the file: %s.. \n" %fname
    # call definition to get cube data
    cubes = getCubeData(fname)
    # call definition to get variable indices
    varIdx, nVars, varLvls, timeIndx = getVarIdx(fname[0:-3],cubes)    
   
    # open for-loop-1 -- for all the variables in the cube
    for ii in range(len(varIdx)):
        stdNm, _, _, _, _, = getDataAttr(cubes[varIdx[ii]])
        print "  Working on variable: %s \n" %stdNm
        # for loop-2 -- runs through the selected time slices - synop hour
        for jj in timeIndx:
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
            outFn = opPath+fname+'_'+str(input1)+'_'+str(int(fcstTm.points)).zfill(2)+'.grib2'
            iris.save(regdCube, outFn, append=True)
            ## edit location section in grib2 to point to the right RMC
            # gribapi.grib_set(outFn,'centre','28')
            # os.system('source /gpfs2/home/umtid/test/grb_local_section.sh')
        # end of for jj in timeIndx:
    # end of for ii in range(len(varIdx)):
    # make memory free
    del cubes
    
    print "  Finished converting file: %s into grib2 format for fcst time: %02dz \n" %(fname,fcstTm.points)
    print "  Time taken to convert the file: %8.5f seconds \n" %(time.time()-startT)
    print " Finished converting file: %s into grib2 format for fcst file: %s \n" %(fname,lol)
# end of def regridAnlFcstFiles(fname):

# Start the main function
def main(nprocesses):
    """
    Main function calling all the sub-functions
    """
    # open for-loop-1 -- works on all the files selected in fnames1
    for fnames in fnames1:                
        fcst_times = ['000','024','048','072','096','120','144','168','192','216']
        fcst_filenames = [(fnames, lol) for lol in fcst_times]
        # create the no of parallel processes
        pool = mp.Pool(nprocesses)
        print "Creating %d (non-daemon) workers and jobs in main process." % nprocesses
        # pass the forecast hours as argument to take one fcst file per process / core to regrid it.
        results = pool.map(regridAnlFcstFiles, fcst_filenames)      
        pool.close() 
        pool.join()
        # parallel end
        print " Time taken to convert the all fcst files: %8.5f seconds \n" %(time.time()-startT)
    # end for-1
    print "Total time taken to convert %d files was: %8.5f seconds \n" %(len(fnames1),(time.time()-startT))
    cmdStr1 = 'mv log1.log '+wrkngDir+fnames[0][0:7]+'stdout_'+str(input1)+'.log'
    os.system(cmdStr1)
    return


if __name__ == '__main__':
    
    nprocesses = int(raw_input('Enter the no of processors : '))
    cc = mp.cpu_count()
    if nprocesses > cc:
        raise ValueError("The given no of processes %d exceeds the available processes %d" % (nprocesses, cc))
    main(nprocesses)
    

# -- End code
