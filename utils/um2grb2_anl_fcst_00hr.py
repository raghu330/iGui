"""
This is simple script to invoke parallel conversion function from um2grb2
and pass the assimilated / forecasted hour as argument.

hour : 00
Output : It creates both analysis - 1 file (um_ana_00hr_date.grib2) and 
         forecast - 40 files (um_prg_00hr_date.grib2, ..., um_prg_240hr_date.grib2).
Written by : Arulalan.T
Date : 07.Dec.2015
"""

import os, sys 
from um2grb2 import convertAnlFiles, convertFcstFiles

# call analysis conversion function w.r.t data assimilated during short forecast hour.
convertAnlFiles(hr='00')
    
# call forecast conversion function w.r.t data assimilated at 00z long forecast hour.
convertFcstFiles(hr='00')
