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
4. Nov 05th, 2015: Changed fname to a string in getVarIdx()
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

def getVarIdx(fname,cube):
    """
    This module gets the required variables from the passed cube as per the
    WRF-Variables.txt file. (matches the contents of pgp06prepDDMMYY)
    inputs:
    1. fname : file name 
    2. cube 
    returns:
    1. The cube index
    2. Number of variables
    3. Number of levels
    4. Time slices or Index
    """
    
    
    nVars = len(cube)    
            
    if fname.startswith('umglaa_pb'):              # umglaa_pb
#        varIndx = [19, 24, 26, 30, 31, 32, 33, 34] # needed
        varIndx = [ 24, 26, 30 ] # available 
        varLvls = 0        
        # the cube contains Instantaneous data at every 3-hours.        
        # but we need to extract only every 6th hours instantaneous.
        timeIndx = [1,3,5,7]
        do6HourlyMean = False
        
    elif fname.startswith('umglaa_pd'):            # umglaa_pd
        # consider variable
#        varIndx = [1,2,3,4,5,6,7] # needed
        varIndx = [1,2,3,4, 6,7] # available 
        varLvls = 18
        # the cube contains Instantaneous data at every 3-hours.
        # but we need to extract only every 6th hours instantaneous.
        timeIndx = [1,3,5,7]
        do6HourlyMean = False
        
    elif fname.startswith('umglaa_pe'):            # umglaa_pe
        varIndx = [1,4,5,7,8,10,12,13,14,16]
        varLvls = 0        
        # the cube contains Instantaneous data at every 1-hours.
        # but we need to extract only every 6th hours instantaneous.
        do6HourlyMean = False
        timeIndx = [5,11,17,23]
        
    elif fname.startswith('umglaa_pf'):            # umglaa_pf
        # other vars (these vars will be created as 6-hourly averaged)
        varIndx1 = [4, 23, 24, 25, 26, 28, 31, 32, 33, 34, 35, 36]
        # rain and snow vars (these vars will be created as 6-hourly accumutated)
        varIndx2 = [12, 13, 17, 18, 20, 21]
        # all vars 
        varIndx = varIndx2 #+ varIndx2
        varLvls = 0        
        # the cube contains data of every 3-hourly average or accumutated.
        # but we need to make only every 6th hourly average or accumutated.
        timeIndx = [(0, 1), (2, 3), (4, 5), (6, 7)]       
        do6HourlyMean = True
        
    else:
        raise ValueError("Filename not implemented yet!")
    # end if-loop

    return varIndx, nVars, varLvls, timeIndx, do6HourlyMean
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

def cubeAverager(tmpCube, action='mean', intervals='hourly'):
    """
    cubeAverager : This function do average over the time dimensional of the 
        passed cube.
    Args :
        tmpCube : cube data which time dimension length must be more than 1.
        action  : mean | sum
        intervals : String to represent in the print statement and add  
                    comments to the return cube data. 
                    eg : '1-hourly' / '3-hourly'
    Returns :
        return the averaged / accumutated over passed time dimension of cube.
                    
    Author : Arulalan.T (AAT)
    Date : 16-Nov-2015
    """
       
    meanCube = tmpCube[0]
    tlen = len(tmpCube.coord('time').points)
    for ti in range(1, tlen):
        meanCube = iris.analysis.maths.add(meanCube, tmpCube[ti])
    # end of for ti in range(1, len(tmpCube)):
    
    if action == 'mean':
        meanCube /= float(tlen)
        print "Converted cube to %s mean" % intervals
    else:
        print "Converted cube to %s accumutation" % intervals
    # end of if not isAccumulation:

    # get the time coord and set to mean
    timeAxFirst = tmpCube[0].coords('time')[0]
    timeAxLast = tmpCube[-1].coords('time')[0]
    # get the bounds and time points from two extremes    
    bounds = [timeAxFirst.bounds[0][0], timeAxLast.bounds[-1][-1]]
    timepoint = [bounds[0] + ((bounds[-1] - bounds[0]) / 2.0)]
    # update the time coordinate with new time point and time bounds 
    timeAxFirst.points = timepoint
    timeAxFirst.bounds = bounds
    # add the updated time coordinate to the meanCube
    meanCube.add_aux_coord(timeAxFirst)
    
    # get the fcst time coord and set to mean
    fcstAxFirst = tmpCube[0].coords('forecast_period')[0]
    fcstAxLast = tmpCube[-1].coords('forecast_period')[0]
    # get the bounds and time points from two extremes    
    bounds = [fcstAxFirst.bounds[0][0], fcstAxLast.bounds[-1][-1]]
    fcstpoint = [bounds[0] + ((bounds[-1] - bounds[0]) / 2.0)]
    # update the time coordinate with new fcst time point and fcst time bounds 
    fcstAxFirst.points = fcstpoint
    fcstAxFirst.bounds = bounds
    
    # add the updated fcst time coordinate to the meanCube
    meanCube.add_aux_coord(fcstAxFirst)

    # add attributes back to meanCube
    meanCube.attributes = tmpCube.attributes  
        
    # add standard_name
    meanCube.standard_name = tmpCube.standard_name
    meanCube.long_name = tmpCube.long_name
    
    print meanCube.long_name, tmpCube.long_name
    
    # generate cell_methods
    if action == 'mean':
        cm = iris.coords.CellMethod('mean', 'time', intervals, 
                                     comments=intervals+' mean')
    else:
        cm = iris.coords.CellMethod('sum', 'time', intervals, 
                                     comments=intervals+' accumutation')
    # add cell_methods to the meanCube                                     
    meanCube.cell_methods = (cm,)
    print meanCube
    # make memory free 
    del tmpCube
    
    # return mean cube 
    return meanCube
# end of def cubeAverager(tmpCube):

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
    
#    outfile = fnames.split('_')[0] + '_prg' #+ hr
    outfile = 'um_prg'
    print "Started Processing the file: %s.. \n" %fname
    # call definition to get cube data
    cubes = getCubeData(fname)
    # call definition to get variable indices
    varIndecies, nVars, varLvls, timeIndx, do6HourlyMean = getVarIdx(fname[0:-3],cubes)
    accumutationType = ['rain', 'precip', 'snow']
    
    # open for-loop-1 -- for all the variables in the cube
    for varIdx in varIndecies:
        stdNm, _, _, _, _ = getDataAttr(cubes[varIdx])
        print "stdNm", stdNm
        if stdNm is None:
            print "Unknown variable standard_name for varIdx[%d] of %s. So skipping it" % (varIdx, fname)
            continue
        # end of if 'unknown' in stdNm: 
        print "  Working on variable: %s \n" %stdNm
        for jj in timeIndx:
            # loop-2 -- runs through the selected time slices - synop hours            
            
            stdNm, fcstTm, _, _, _ = getDataAttr(cubes[varIdx][jj])            
            print "   Working on forecast time: %02dz\n" %fcstTm.points
            if do6HourlyMean: 
                if not isinstance(jj, tuple):
                    raise ValueError("Time Indecies must be tuple, when do6HourlyMean is True")
                # grab the variable which is f(t,z,y,x)
                # tmpCube corresponds to each variable for the SYNOP hours from
                # start to end of short time period mean (say 3-hourly)
                tmpCube = cubes[varIdx][jj[0] : jj[1]+1]
                
                action = 'mean'
                cubeName = tmpCube.standard_name    
                # to check either do we have to do accumutation or not.
                for acc in accumutationType:
                    if cubeName and acc in cubeName:
                        action = 'sum'
                        break 
                # end of for acc in accumutationType:
                
                # convert 3-hourly data into 6-hourly mean or accumutation
                tmpCube = cubeAverager(tmpCube, action, intervals='6-hourly')            
            else:
                # grab the variable which is f(t,z,y,x)
                # tmpCube corresponds to each variable for the SYNOP hours
                tmpCube = cubes[varIdx][jj]
            # get original lats and lons

            _, _, _, lat0, lon0 = getDataAttr(tmpCube)
            # interpolate it 0,25 deg resolution by setting up sample points based on coord
            print "    Regridding data to 0.25x0.25 deg spatial resolution \n"            
            regdCube = tmpCube.interpolate(targetGrid, iris.analysis.Linear())
            print "regrid done"
            # make memory free 
            del tmpCube
            
            # get the regridded lat/lons
            stdNm, fcstTm, refTm, lat1, lon1 = getDataAttr(regdCube)

            # save the cube in append mode as a grib2 file       
            
            if fcstTm.bounds:
                # get the last hour bound ## need this for pf files.                
                hr = str(int(fcstTm.bounds[-1][-1]))            
            else:
                # get the fcst time point 
                hr = str(int(fcstTm.points))
            # end of if fcstTm.bounds:
            outFn = outfile +'_'+ hr.zfill(3) +'hr'+ '_' + current_date +'.grib2'
            outFn = os.path.join(opPath, outFn)
            print "Going to be save into ", outFn
            
            try:
                iris.save(regdCube, outFn, append=True)
            except iris.exceptions.TranslationError as e:
                if str(e) == "The vertical-axis coordinate(s) ('soil_model_level_number') are not recognised or handled.":  
                    regdCube.remove_coord('soil_model_level_number') 
                    print "Removed soil_model_level_number from cube, due to error %s" % str(e)
                    iris.save(regdCube, outFn, append=True)
                else:
                    print "Got error while saving, %s" % str(e)
                    print " So skipping this without saving data"
                    continue
            except Exception as e:
                print "Error while saving!! %s" % str(e)
                print " So skipping this without saving data"
                continue
            # end of try:
            print "saved"
            # make memory free 
            del regdCube
            
            ## edit location section in grib2 to point to the right RMC
            # gribapi.grib_set(outFn,'centre','28')
            # os.system('source /gpfs2/home/umtid/test/grb_local_section.sh')
        # end of for jj in timeIndx:
    # end of for ii in range(len(varIdx)):
    # make memory free
    del cubes
    
    print "  Time taken to convert the file: %8.5f seconds \n" %(time.time()-startT)
    print " Finished converting file: %s into grib2 format for fcst file: %s \n" %(fname,hr)
# end of def regridAnlFcstFiles(fname):


def doConvert(fname):
    fcst_times = ['000', '024','048','072','096','120','144','168','192','216']
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
    fnames1 = ['umglaa_pb','umglaa_pd', 'umglaa_pe']
    ## pf file is not working....
    ###'umglaa_pf',
    # get the current date in YYYYMMDD format
    current_date = time.strftime('%Y%m%d')

    print "\n current_date is %s" % current_date
    sys.stdout = myLog("log1.log")
    
    # start the timer now
    startT = time.time()

    # set-up base folders
    wrkngDir = '/gpfs2/home/umtid/test/'
    dataDir = '/gpfs3/home/umfcst/NCUM/fcst/' + current_date + '/00/'
    opPath = os.path.join('/gpfs2/home/umtid/test/GRIB-parallel/', current_date)
    if not os.path.exists(opPath):  
        os.makedirs(opPath)
        print "Created directory", opPath
    # end of if not os.path.exists(opPath):  
    
    # target grid as 0.25 deg resolution by setting up sample points based on coord
    targetGrid = [('longitude',numpy.linspace(0,360,1440)),
                    ('latitude',numpy.linspace(-90,90,721))]
    main(fnames1)
    

# -- End code
