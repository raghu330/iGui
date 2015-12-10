#!/usr/bin/env python

__author__ = 'raghav, arulalant'
__version__ = 'v5.0'
__release_version__ = 'v1.0a'
__release_name__ = 'alpha'

"""
What does this code piece do?
This code converts 6-hourly UM fields file data into grib2 format after
regridding the data to 0.25x0.25 degree spatial resolution by imbibing
analysis fields from the yesterday's 18Z time (based on Dr. SM).

Output:
This script produce output files as multiple 6 hourly forecasts data from
different input files such as pd, pd, pe, etc., So all 6 hourly forecasts data
of different input files will be append to same 6 hourly grib2 outfiles (These
conventions are according to NCUM only!)

Parallel:
As for now, we are using multiprocessing to make parallel run on different files
like pb, pd, pe and its creating child porcess with respect to no of forecast
hours. To make more parallel threads on variable, fcstHours level we may need to
use OpenMPI-Py.

Disclaimers (if any!)
This is just test code as of now and is meant for a specific purpose only!

Standards:
This code conforms to pep8 standards and KISS philosophy.

Contributors & their roles:
#1. Mr. Raghavendra S. Mupparthy (MNRS) - Integrator, TIAV Lead, I/C, overseer & code humor!
#2. Mr. Arulalan T (AAT) - Chief coder, optimiser, parelleliser and THE shebang!
#3. Dr. Devjyoti Dutta (DJ) - ECMWF-GRIB2 Metadata manipulator
#4. Dr. Saji Mohandas (SM) - TIFF lead/expertise and shell-template.

Testing & their roles:
#1. Mr. Kuldeep Sharma (KS) - Main tester for visual integrety vis-a-vis GrADS
#2. Mr. Raghavendra S. Mupparthy (MNRS) - Implementor
#3. Dr. Raghavendra Ashrit (RA) - Testing for RIMES and overall integrity testing
#4. Dr. Jayakumar A. (JA) - Comparison with the CAWCR convertor and specifictions needs
#5. Dr. Saji Mohandad (SM) - Control test (GrADS & subset.tcl) & Future Functional Description
#6. Mr. Gopal Raman Iyengar (GRI) - Overseer

Acknowledgments:
#1. Dr. Rakhi R, Dr. Jayakumar A, Dr. Saji Mohandas and Mr. Bangaru (Ex-IBM) for N768.
#2. IBM Team @ NCMRWF for installation support on Bhaskara - Ms. Shivali & Mr. Bangaru (Ex-IBM)

Code History:
1.  Jul 22nd, 2015: First version by MNRS
2.  Jul 24th, 2015: Grib section editor - version-0.1,
                  : Automation of filenames started
                  : Extraction of required variables
                  : Interpolation scheme to 0.25 degree (MNRS & DJ)
3.  Sep 11th, 2015: Recasted for 6-hourly ouputs (MNRS)
4.  Nov 05th, 2015: Changed fname to a string in getVarInOutFilesDetails() (MNRS)
5.  Nov 07th, 2015: Added to iGui project on github from fcm project (MNRS & AAT)
6.  Nov 09th, 2015: parallelization!!! (AAT)
7.  Nov 10th, 2015: Spawned multiple versions for input (AAT & MNRS)
8.  Nov 12th, 2015: Appending same 6 hourly forecast data of different input
                    files into same 6 hourly grib2 files. (AAT)
9.  Nov 16th, 2015: Added new module/functionality "cubeAverager" to account
                    for two kinds of fields: accumulated or instantaneous (AAT)
10. Dec 02nd, 2015: Added module to create analysis fields from crtAnal.py (MNRS)
                    Corrected for typos (MNRS)
11. Dec 07th, 2015: Freshly added functions/facilities to create analysis fields 
                    by using short forecast files by chossing either instantaneous
                    and average/sum by using past 6 hour's short forecast (AAT)                     
                    Version - 5.0. Ready for alpha release v1.0a (AAT)

References:
1. Iris. v1.8.1 03-Jun-2015. Met Office. UK. https://github.com/SciTools/iris/archive/v1.8.1.tar.gz
2. myLog() based on http://mail.python.org/pipermail/python-list/2007-May/438106.html
3. Data understanding: /gpfs2/home/umfcst/ShortJobs/Subset-WRF/ncum_subset_24h.sh
4. Saji M. (2014), "Utility to convert UM fieldsfile output to NCEP GRIB1 format:
                    A User Guide", NMRF/TR/01/2014, April 2014, pp. 51, available at
                    http://www.ncmrwf.gov.in/umfld2grib.pdf

Copyright: ESSO-NCMRWF,MoES, 2015-2016.
"""

# -- Start importing necessary modules
import os, sys, time
import numpy, scipy
import iris
import gribapi
import netCDF4
import iris.unit as unit
import multiprocessing as mp
import multiprocessing.pool as mppool       # We must import this explicitly, it is not imported by the top-level multiprocessing                                                 module.
import types

import datetime
# End of importing business

# -- Start coding

_current_date_ = None
_startT_ = None
_tmpDir_ = None
_inDataPath_ = None
_opPath_ = None
_targetGrid_ = None

# create a class #1 for capturing stdin, stdout and stderr
class myLog():
    """
    A simple class with destructor and construtor for logging the standatd I/O
    """
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
# end of class #1

## Start definition files
# start definition #1
def getCubeData(umFname):
    """
    This definition module reads the input file name and its location as a
    string and it returns the data as an Iris Cube.
    An upgraded version uses a GUI to read the file.

    :param umFname: UM fieldsfile filename passed as a string
    :return: Data for corresponding data file in Iris cube format
    """

    cubes = iris.load(umFname)
    
    return cubes
# end of definition #1

def getYdayStr(today):
    """
    This module returns yesterday's date-time string 
        :today: today date string must follow pattern of yyyymmdd.
    
        :return: yesterday's date in string format of yyyymmdd.
    """
    tDay = datetime.datetime.strptime(today, "%Y%m%d")
    lag = datetime.timedelta(days=1)
    yDay = (tDay - lag).strftime('%Y%m%d')

    return yDay
# end of def getYdayStr(today):

# start definition #2
def getVarInOutFilesDetails(inDataPath, fname, hr):
    """
    This definition module gets the required variables from the passed
    cube as per the WRF-Variables.txt file.
    (matches the contents of pgp06prepDDMMYY)
    - Improvements & Edits by AAT & MNRS
    :param inDataPath: data path which contains data and hour.
    :param fname: filename of the fieldsfile that has been passed as a string.

    :return: varIndx: Cube index indicating the variable as an array
    :return: varLvls: No. of vertical levels in the cube as an array/scalar - integer (number)
    :return: fcstHours: Time slices of the cube as an array/scalar - integer (number)
    :return: do6HourlyMean: Logical expression as either True or False, indicating
                            whether the field is instantaneous or accumulated
    :return: infile: It returns absolute path of infile by inDataPath and fname.
                     Also it updates inDataPath yesterday, hour for analysis pf files
    :return: outfile: It returns outfile absolute path with ana or fcst type 
                      along with date and hour.
    Started by MNRS and improved by AAT!
    
    Updated : 07-12-2015
    """
    
    hr = int(hr)
    
    infile = os.path.join(inDataPath, fname)    
    
    inDataPathHour = inDataPath.split('/')[-1]      
    if fname.startswith('umglaa'):
        outfile = 'um_prg' 
    elif fname.startswith(('umglca', 'qwqg00')):
        outfile = 'um_ana'
    else:
        raise ValueError("Got unknown fname, so couldn't set outfile!")
    # end of if fname.startswith('umglaa'):
    
    ##### ANALYSIS FILE BEGIN     
    if fname.startswith('qwqg00'):                   # qwqg00
        varIndx = [0, 1, 2, 3, 4, 5, 7, 8]
        ### need to add 6 in varIdx, but its not working in wgrib2
        varLvls = 0        
        # the cube contains Instantaneous data at every 3-hours.        
        # but we need to extract every 6th hours instantaneous.
        fcstHours = numpy.array([0,])     
        do6HourlyMean = False
            
    elif fname.startswith('umglca_pb'):              # umglca_pb
        # varIndx = [19, 24, 26, 30, 31, 32, 33, 34] # needed
        varIndx = [ 23, 25, 29 ] # available for use
        varLvls = 0        
        # the cube contains Instantaneous data at every 3-hours.        
        # but we need to extract every 6th hours instantaneous.
        fcstHours = numpy.array([0,])     
        do6HourlyMean = False
        
    elif fname.startswith('umglca_pd'):            # umglca_pd
        # consider variable
        if inDataPathHour == '00':
            varIndx = [4] 
            # rest of them (i.e 1,2,3,5,6,7) from taken already from qwqg00 file.
        else:
            varIndx = [1,2,3,4,6,7] # 5 is not working
            
        # qwqg00 file variables are more correct than this short forecast vars.
        varLvls = 18
        # the cube contains Instantaneous data at every 3-hours.
        # but we need to extract only every 6th hours instantaneous.
        fcstHours = numpy.array([0,])     
        do6HourlyMean = False
        
    elif fname.startswith('umglca_pe'):            # umglca_pe
        if inDataPathHour == '00':
            varIndx = [5,6,7,9,11,14,16]
            # rest of them (i.e 4, 12) from taken already from qwqg00 file.
        else:
            varIndx = [4,5,6,7,9,11,12,14,16]

        ### varIdx 10 is omited, since it has two zero. i think we need to take previous file average or current hour aver.
        varLvls = 0        
        # the cube contains Instantaneous data at every 1-hours.
        # but we need to extract only every 6th hours instantaneous.
        fcstHours = numpy.array([0,])     
        do6HourlyMean = False

    elif fname.startswith('umglca_pf'):             # umglca_pf
        # other vars (these vars will be created as 6-hourly averaged)
        # varIndx = [4, 23, 24, 25, 26, 28, 31, 32, 33, 34, 35, 36]
        # rain and snow vars (these vars will be created as 6-hourly accumutated)
        varIndx2 = [12, 13, 17, 18, 20, 21]         # all vars
        varIndx = varIndx2 #+ varIndx2
        varLvls = 0        
        # the cube contains data of every 3-hourly average or accumutated.
        # but we need to make only every 6th hourly average or accumutated.
        fcstHours = numpy.array([(1, 5)])   
        do6HourlyMean = True
        
        ipath = inDataPath.split('/')
        hr = ipath[-1]
        today_date = ipath[-2]
        
        if hr in ['06', '12', '18']:
            hr = str(int(hr) - 6).zfill(2)
            print "Taken analysis past 6 hour data", hr
        elif hr == '00':           
            # actually it returns yesterday's date.
            today_date = getYdayStr(today_date)
            # set yesterday's 18z hour.
            hr = '18'
            print "Taken analysis yesterday's date and 18z hour", today_date
        else:
            raise ValueError("hour %s method not implemented" % hr)
        # end of if hr in ['06', '12', '18']:            
            
        ## update the hour, date 
        ipath[-1] = hr
        ipath[-2] = today_date
        ipath = os.path.join('/', *ipath)
        # infile path (it could be current date and past 6 hour for 06,12,18 hours.  
        # but it set yesterday date and past 6 hour for 00 hour)
        infile = os.path.join(ipath, fname)    
    
    ##### ANALYSIS FILE END
    
    ##### FORECAST FILE BEGIN
    elif fname.startswith('umglaa_pb'):              # umglaa_pb
        # varIndx = [19, 24, 26, 30, 31, 32, 33, 34] # needed
        varIndx = [ 24, 26, 30 ] # available for use
        varLvls = 0        
        # the cube contains Instantaneous data at every 3-hours.        
        # but we need to extract every 6th hours instantaneous.
        fcstHours = numpy.array([6, 12, 18, 24]) + hr
        do6HourlyMean = False
        
    elif fname.startswith('umglaa_pd'):            # umglaa_pd
        # consider variable
        # varIndx = 5 # needed
        varIndx = [1,2,3,4, 6,7] # available for use
        varLvls = 18
        # the cube contains Instantaneous data at every 3-hours.
        # but we need to extract only every 6th hours instantaneous.
        fcstHours = numpy.array([6, 12, 18, 24]) + hr
        do6HourlyMean = False
        
    elif fname.startswith('umglaa_pe'):            # umglaa_pe
        varIndx = [1,4,5,7,8,10,12,13,14,16]
        varLvls = 0        
        # the cube contains Instantaneous data at every 1-hours.
        # but we need to extract only every 6th hours instantaneous.
        fcstHours = numpy.array([6, 12, 18, 24]) + hr
        do6HourlyMean = False

    elif fname.startswith('umglaa_pf'):             # umglaa_pf
        # other vars (these vars will be created as 6-hourly averaged)
        # varIndx = [4, 23, 24, 25, 26, 28, 31, 32, 33, 34, 35, 36]
        # rain and snow vars (these vars will be created as 6-hourly accumutated)
        varIndx2 = [12, 13, 17, 18, 20, 21]         # all vars
        varIndx = varIndx2 #+ varIndx2
        varLvls = 0        
        # the cube contains data of every 3-hourly average or accumutated.
        # but we need to make only every 6th hourly average or accumutated.
        fcstHours = numpy.array([(1, 5), (7, 11), (13, 17), (19, 23)]) + hr    
        do6HourlyMean = True    
    
    ##### FORECAST FILE END
    else:
        raise ValueError("Filename not implemented yet!")
    # end if-loop

    return varIndx, varLvls, fcstHours, do6HourlyMean, infile, outfile
# end of definition #2

# start definition #3
def getCubeAttr(tmpCube):
    """
    This module returns basic coordinate & attribute info about any Iris data cube.
    :param tmpCube: a temporary Iris cube containing a single geophysical field/parameter
    :return: stdNm: CF-compliant Standard name of the field/parameter
    :return: fcstTm: forecast time period for eg: 00, 06, 12 etc -- units as in hours
    :return: refTm: reference time -- units as date  in Gregorian
    :return: lat as scalar array (1D) units as degree (from 90S to 90N)
    :return: lon as scalar array (1D) units as degree (from 0E to 360E)
    Original by MNRS
    """
    stdNm = tmpCube.standard_name
    fcstTm = tmpCube.coord('forecast_period')
    refTm = tmpCube.coord('forecast_reference_time')
    lat = tmpCube.coord('latitude')
    lon = tmpCube.coord('longitude')

    return stdNm, fcstTm, refTm, lat, lon
# end of definition #3

# start definition #4
def cubeAverager(tmpCube, action='mean', intervals='hourly'):
    """
    This module was added by AAT to return a data variable depending on the nature of the field.
    :param tmpCube:     The temporary cube data (in Iris format) with non-singleton time dimension
    :param action:      mean| sum (accumulated fields are summed and instantaneous are averaged).
    :param intervals:   A simple string representing represting the time & binning aspect.
    :return: meanCube:  An Iris formatted cube date containing the resultant data either as
                        averaged or summed.
    ACK:
    Started and initiated by AAT on 11/16/2015 and minor correction & standardization by MNRS on
    11/29/15.
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

# create a class #2 to initiate mp daemon processes
class _NoDaemonProcess(mp.Process):
    # make 'daemon' attribute always return False
    # A class created by AAT
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)
# end of class #2

# create a class #3 to set-up worker-pools
class _MyPool(mppool.Pool):
    # We sub-class multiprocessing.pool. Pool instead of multiprocessing.Pool
    # because the latter is only a wrapper function, not a proper class.
    ### http://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
    ### refer the above link to invoke child processes
    # A class created by AAT
    Process = _NoDaemonProcess
# end of class #3

# start definition #5
def regridAnlFcstFiles(arg):
    """
    New Module by AAT:
    This module has been rewritten entirely by AAT for optimization as an embarassingly-
    parallel problem! It also checks the std names from Iris cube format with the
    CF-convention and it regrids the data to 0.25x0.25 regular grid using linear
    interpolation methods.
    :param arg: tuple(fname, hr)
            fname: common filename
            hr: forecast hour
    :return: regridded cube saved as GRIB2 file! TANGIBLE!
    ACK:
    This module has been entirely revamped & improved by AAT based on an older and
    serial version by MNRS on 11/16/2015.
    """
    global _targetGrid_, _current_date_, _startT_, _inDataPath_, _opPath_
    
    fpname, hr = arg 
    
    fext = fpname.split('_')[-1]
    
    ### if fileName has some extension, then do not add hr to it.
    fileName = fpname + hr if not '.' in fpname else fpname
    
    fname = os.path.join(_inDataPath_, fileName)        
    
    # call definition to get variable indices
    varIndices, varLvls, fcstHours, do6HourlyMean, infile, outfile = getVarInOutFilesDetails(_inDataPath_,
                                                                                             fileName, hr)
    
    if not os.path.isfile(fname): 
        print "The file doesn't exists: %s.. \n" %fname
        return  
    # end of if not os.path.isfile(fname): 
    print "Started Processing the file: %s.. \n" %fname
    
    # call definition to get cube data
    cubes = getCubeData(infile)
    nVars = len(cubes)
    
    accumutationType = ['rain', 'precip', 'snow']
    
    # open for-loop-1 -- for all the variables in the cube
    for varIdx in varIndices:
        stdNm, _, _, _, _ = getCubeAttr(cubes[varIdx])
        print "stdNm", stdNm, fileName
        if stdNm is None:
            print "Unknown variable standard_name for varIdx[%d] of %s. So skipping it" % (varIdx, fileName)
            continue
        # end of if 'unknown' in stdNm: 
        print "  Working on variable: %s \n" %stdNm
        for fhr in fcstHours:
            # loop-2 -- runs through the selected time slices - synop hours                        
            print "   Working on forecast time: ", fhr            
            # grab the variable which is f(t,z,y,x)
            # tmpCube corresponds to each variable for the SYNOP hours
            print "extract start", infile, fhr, varIdx
            tmpCube = cubes[varIdx].extract(iris.Constraint(forecast_period=fhr))
            print "extrad end", infile, fhr, varIdx
            if do6HourlyMean and (tmpCube.coords('forecast_period')[0].shape[0] > 1):              
                # grab the variable which is f(t,z,y,x)
                # tmpCube corresponds to each variable for the SYNOP hours from
                # start to end of short time period mean (say 3-hourly)                                
                action = 'mean'
                cubeName = tmpCube.standard_name    
                # to check either do we have to do accumutation or not.
                for acc in accumutationType:
                    if cubeName and acc in cubeName:
                        action = 'sum'
                        break 
                # end of for acc in accumutationType:

                # convert 3-hourly mean data into 6-hourly mean or accumutation
                tmpCube = cubeAverager(tmpCube, action, intervals='6-hourly')            
            # end ofif do6HourlyMean and tmpCube.coords('forecast_period')[0].shape[0] > 1:     

#            _, _, _, lat0, lon0 = getCubeAttr(tmpCube)
            # interpolate it 0,25 deg resolution by setting up sample points based on coord
            print "\n    Regridding data to 0.25x0.25 deg spatial resolution \n"
            print "From shape", tmpCube.shape
            try:            
                regdCube = tmpCube.interpolate(_targetGrid_, iris.analysis.Linear())
            except Exception as e:
                print "ALERT !!! Error while regridding!! %s" % str(e)
                print " So skipping this without saving data"
                continue
            # end of try:   
            print "regrid done"
            print "To shape", regdCube.shape        
            # make memory free 
            del tmpCube
            
            # get the regridded lat/lons
            stdNm, fcstTm, refTm, lat1, lon1 = getCubeAttr(regdCube)

            # save the cube in append mode as a grib2 file       
            if _inDataPath_.endswith('00'):
                if fcstTm.bounds is not None:
                    # get the last hour bound ## need this for pf files.                
                    hr = str(int(fcstTm.bounds[-1][-1]))     
                    print "Bounds comes in ", hr, fcstTm.bounds, fileName       
                else:
                    # get the fcst time point 
                    hr = str(int(fcstTm.points))
                    print "points comes in ", hr, fileName 
                # end of if fcstTm.bounds:
            else:
                # get the hour from infile path as 'least dirname'
                hr = _inDataPath_.split('/')[-1]
            # end of if _inDataPath_.endswith('00'):
            
            outFn = outfile +'_'+ hr.zfill(3) +'hr'+ '_' + _current_date_ + '_' + fext + '.grib2'
            outFn = os.path.join(_opPath_, outFn)
            print "Going to be save into ", outFn
            
            try:
                iris.save(regdCube, outFn, append=True)
            except iris.exceptions.TranslationError as e:
                if str(e) == "The vertical-axis coordinate(s) ('soil_model_level_number') are not recognised or handled.":  
                    regdCube.remove_coord('soil_model_level_number') 
                    print "Removed soil_model_level_number from cube, due to error %s" % str(e)
                    iris.save(regdCube, outFn, append=True)
                else:
                    print "ALERT !!! Got error while saving, %s" % str(e)
                    print " So skipping this without saving data"
                    continue
            except Exception as e:
                print "ALERT !!! Error while saving!! %s" % str(e)
                print " So skipping this without saving data"
                continue
            # end of try:
            print "saved"
            # make memory free 
            del regdCube
            
            ## edit location section in grib2 to point to the right RMC
            # gribapi.grib_set(outFn,'centre','28')
#            gribapi.grib_set_long(gribid, "centre", 28)  # RMC of India
#            gribapi.grib_set_long(gribid, "subCentre", 0)  # exeter is not in the spec
            # os.system('source /gpfs2/home/umtid/test/grb_local_section.sh')
        # end of for fhr in fcstHours:
    # end of for ii in range(len(varIdx)):
    # make memory free
    del cubes
    
    print "  Time taken to convert the file: %8.5f seconds \n" %(time.time()-_startT_)
    print " Finished converting file: %s into grib2 format for fcst file: %s \n" %(fileName,hr)
# end of def regridAnlFcstFiles(fname):

def doMergeInOrder(ftype, simulated_hr):
    
    def mergeFiles(arg):
        ftype, fcst_hr = arg 
        global _current_date_
        
        order = ('pb', 'pd', 'pe')
        if ftype in ['fcst', 'forecast']:            
            outfile = 'um_prg' 
        elif ftype in ['anl', 'analysis']:
            if fcst_hr == '00': order = ('qwqg00.pp0', 'pb', 'pd', 'pe')            
            outfile = 'um_ana'
        # end of if ftype in ['fcst', 'forecast']:    
        
        infiles = ''
        for fext in order:
            infiles += outfile +'_'+ fcst_hr.zfill(3) +'hr'+ '_' + _current_date_ + '_' 
            infiles += fext + '.grib2' + '  '*4
        # end of for fext in order:
        merged_file = outfile +'_'+ fcst_hr.zfill(3) +'hr'+ '_' + _current_date_ + '.grib2'
        # merge in order
        mergecmd = "cdo merge " + infiles + "   " + merged_file 
        print "merge command : ", mergecmd
        os.system(mergecmd)
        print "merged into ", merged_file
        
        time.sleep(2)
        # remove older files
        rmcmd = "rm -rf " + infiles
        os.system(rmcmd)
        print "removed older files ", infiles
    # end of def mergeFiles(arg):
    
    print "Lets re-order and merge all the files!!!"
    #####
    ## 6-hourly Files have been created with extension.
    ## Now lets do merge of those individual files in order, in parallel mode. 
    current_dir = os.getcwd()
    os.chdir(_opPath_)
    
    if ftype in ['fcst', 'forecast']:
        ftype_hr = [(ftype, str(hr).zfill(3)) for hr in range(6,241,6)]        
        ## get the no of created anl/fcst 6hourly files  
        nprocesses = len(ftype_hr)        
        # parallel begin - 3
        pool = _MyPool(nprocesses)
        print "Creating %d (non-daemon) workers and jobs in doMergeInOrder process." % nprocesses
        results = pool.map(mergeFiles, ftype_hr)    
        # closing and joining master pools
        pool.close()     
        pool.join()
        # parallel end - 3    
    elif ftype in ['anl', 'analysis']:
        ftype_hr = (ftype, simulated_hr)
        mergeFiles(ftype_hr)
    # end of if ftype in ['fcst', 'forecast']: 
    os.chdir(current_dir)
    print "Total time taken to convert and re-order %d files was: %8.5f seconds \n" % (time.time()-_startT_)
    
    return 
# end of def doMergeInOrder(arg):
    
# Start definition #6
def doFcstConvert(fname):
    """
    New Module by AAT:
    This module has been rewritten entirely by AAT for optimization as an embarassingly-
    parallel problem! This module acts as the main program to feed the filenames as a
    child process to a multiprocessing thread as a daemon process.
    :param fname: Name of the FF filename in question as a "string"
    :return: Nothing! TANGIBLE!
    """
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
# end def doFcstConvert(fname):


def doAnlConvert(fname):
    """
    New Module by AAT:
    This module has been rewritten entirely by AAT for optimization as an embarassingly-
    parallel problem! This module acts as the main program to feed the filenames as a
    child process to a multiprocessing thread as a daemon process.
    :param fname: Name of the FF filename in question as a "string"
    :return: Nothing! TANGIBLE!
    """
    
    regridAnlFcstFiles((fname, '000'))  
# end def doAnlConvert(fname):


# Start the convertFilesInParallel function
def convertFilesInParallel(fnames, ftype):
    """
    convertFilesInParallel function calling all the sub-functions
    :param fnames: a simple filename as argument in a string format
    :return: THE SheBang!
    """
    
    global _startT_, _tmpDir_, _opPath_
    
    ## get the no of files and 
    nprocesses = len(fnames)
    # lets create no of parallel process w.r.t no of files.
    
    # parallel begin - 1 
    pool = _MyPool(nprocesses)
    print "Creating %d (non-daemon) workers and jobs in convertFilesInParallel process." % nprocesses
    
    if ftype in ['anl', 'analysis']:
        print "fnames ++++++++", fnames
        results = pool.map(doAnlConvert, fnames)
    elif ftype in ['fcst', 'forecast']:
        results = pool.map(doFcstConvert, fnames)
    else:
        raise ValueError("Unknown file type !")
    # end of if ftype in ['anl', 'analysis']:    

    # closing and joining master pools
    pool.close()     
    pool.join()
    # parallel end - 1 
    
    print "Total time taken to convert %d files was: %8.5f seconds \n" %(len(fnames),(time.time()-_startT_))
    
    return
# end of def convertFilesInParallel(fnames):


def convertFcstFiles(inPath, outPath, tmpPath, date=time.strftime('%Y%m%d'), hr='00'):
       
    global _targetGrid_, _current_date_, _startT_, _tmpDir_, _inDataPath_, _opPath_
    
    # forecast filenames partial name
    fcst_fnames = ['umglaa_pb','umglaa_pd', 'umglaa_pe'] 
    
    ## pf file is not working....
    ###'umglaa_pf',
    # get the current date in YYYYMMDD format
    _tmpDir_ = tmpPath
    _current_date_ = date
    print "\n _current_date_ is %s" % _current_date_
    sys.stdout = myLog(os.path.join(_tmpDir_, "log2.log"))
    
    # start the timer now
    _startT_ = time.time()

    # set-up base folders    
    _inDataPath_ = os.path.join(inPath, _current_date_, hr)
    if not os.path.exists(_inDataPath_):
        raise ValueError("In datapath does not exists %s" % _inDataPath_)
    # end of if not os.path.exists(_inDataPath_):
    
    _opPath_ = os.path.join(outPath, _current_date_)
    if not os.path.exists(_opPath_):  
        os.makedirs(_opPath_)
        print "Created directory", _opPath_
    # end of if not os.path.exists(_opPath_):  
    
    # target grid as 0.25 deg resolution by setting up sample points based on coord
    _targetGrid_ = [('longitude',numpy.linspace(0,360,1440)),
                    ('latitude',numpy.linspace(-90,90,721))]
                    
    # do convert for forecast files 
    convertFilesInParallel(fcst_fnames, ftype='fcst')   
    
    # do re-order and merge files in parallel
    doMergeInOrder('fcst', hr)
    
    cmdStr = 'mv '+_tmpDir_+'log2.log  '+_tmpDir_+ 'um2grib2_fcst_stdout_'+ _current_date_ +'_00hr.log'
    os.system(cmdStr)     
# end of def convertFcstFiles(...):


def convertAnlFiles(inPath, outPath, tmpPath, date=time.strftime('%Y%m%d'), hr='00'):
       
    global _targetGrid_, _current_date_, _startT_, _tmpDir_, _inDataPath_, _opPath_
    
    # analysis filenames partial name
    anl_fnames = ['umglca_pb', 'umglca_pd', 'umglca_pe']
    
    if hr == '00': anl_fnames.insert(0, 'qwqg00.pp0')
    
    ## pf file is not working....
    ###'umglca_pf',
    # get the current date in YYYYMMDD format
    _tmpDir_ = tmpPath
    _current_date_ = date
    print "\n _current_date_ is %s" % _current_date_
    sys.stdout = myLog(os.path.join(_tmpDir_, "log1.log"))
    
    # start the timer now
    _startT_ = time.time()

    # set-up base folders
    _inDataPath_ = os.path.join(inPath, _current_date_, hr)
    if not os.path.exists(_inDataPath_):
        raise ValueError("In datapath does not exists %s" % _inDataPath_)
    # end of if not os.path.exists(_inDataPath_):
    
    _opPath_ = os.path.join(outPath, _current_date_)
    if not os.path.exists(_opPath_):  
        os.makedirs(_opPath_)
        print "Created directory", _opPath_
    # end of if not os.path.exists(_opPath_):  
    
    # target grid as 0.25 deg resolution by setting up sample points based on coord
    _targetGrid_ = [('longitude',numpy.linspace(0,360,1440)),
                    ('latitude',numpy.linspace(-90,90,721))]
                    
    # do convert for analysis files
    convertFilesInParallel(anl_fnames, ftype='anl')   
    
    # do re-order and merge files in parallel
    doMergeInOrder('anl', hr)
    
    cmdStr = 'mv '+_tmpDir_+'log1.log  '+_tmpDir_+ 'um2grib2_anl_stdout_'+ _current_date_ +'_' +hr+'hr.log'
    os.system(cmdStr)  
# end of def convertAnlFiles(...):


## feeder!
#if __name__ == '__main__':
#    
#    
#    # call analysis conversion function w.r.t data assimilated during short forecast hour.
#    convertAnlFiles(hr='00')
#    #########################################################
#    ## Can be called the above function as below also.      #
#    ### for hour in ['00', '06', '12', '18']:               #
#    ###     convertAnlFiles(hr=hour)                        #
#    ### end of for hour in ['00', '06', '12', '18']:        #
#    ##                                                      #
#    #########################################################
#    
#    # call forecast conversion function w.r.t data assimilated at 00z long forecast hour.
#    convertFcstFiles(hr='00')
#    
# -- End code
