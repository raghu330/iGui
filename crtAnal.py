#!/usr/bin/env python

__author__ = 'raghav, arunalant'

"""
What does this code piece do?
This code creates the analysis files from previous days 18Z files
This code conforms to pep8 standards.

Contributors:
#1. Mr. Raghavendra S. Mupparthy (MNRS)
#2. Mr. Arulalan T (AAT)
#3. Dr. Saji Mohandas (SM)

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
1. Nov 09th, 2015: First version by MNRS

References:
1. Iris. v1.8.1 03-Jun-2015. Met Office. UK. https://github.com/SciTools/iris/archive/v1.8.1.tar.gz
2. myLog() based on http://mail.python.org/pipermail/python-list/2007-May/438106.html
3. Data understanding: /gpfs2/home/umfcst/ShortJobs/Subset-WRF/ncum_subset_24h.sh

Copyright: ESSO-NCMRWF,MoES, 2015.
"""

# -- Start importing necessary modules
import os, sys, time
import numpy, scipy
import gribapi, netCDF4, pygrib
import iris
import multiprocessing as mp
import multiprocessing.pool as mppool
from datetime import datetime
# End of importing business

# -- Start coding
# start  def-1
def getYdayDtStr():
    """
    This module returns yesterday's date-time string for 18Z hours
    :return: yDay18z
    """
    tDay = datetime.date.today()
    lag = datetime.timedelta(days=1)
    yDay =  tDay - lag
    yDay18z = yDay.strftime('%Y%m%d')

    return yDay18z
# end of def-1

# start def-2
def getYdayData(umFname,yDay18z):
    """
    This module returns yesterday's data for the corresponding filename at 18Z cycle
    :param umFname: The fieldsfile name that's being called and run.
    :param yDay18z: Yesterday's date-time string.
    :return: yDay18zCube: Yesterday's data as an Iris cube
    """
    yDataDir = '/gpfs3/home/umfcst/NCUM/fcst/' + yDay18z +'/'+'18'
    aCube = iris.load(yDataDir+umFname)

    return aCube
# end of def-2

def main():
    # get the current date in YYYYMMDD format
    current_date = time.strftime('%Y%m%d')

    print "\n current_date is %s" % current_date
    sys.stdout = myLog("log1.log")
    # set-up base folders
    wrkngDir = '/gpfs2/home/umtid/test/'
    dataDir = '/gpfs3/home/umfcst/NCUM/fcst/' + current_date + '/00/'
    opPath = os.path.join('/gpfs2/home/umtid/test/GRIB-parallel/', current_date)
    if not os.path.exists(opPath):
        os.makedirs(opPath)
        print "Created directory", opPath
    # end of if not os.path.exists(opPath):
