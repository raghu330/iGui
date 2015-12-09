"""
This is simple script to invoke parallel conversion function from um2grb2
and pass the assimilated / forecasted hour as argument.

hour : 00
Output : It creates forecast - 40 files 
         (um_prg_00hr_date.grib2, ..., um_prg_240hr_date.grib2).
Written by : Arulalan.T
Date : 07.Dec.2015
"""

import os, sys
from g2utils.um2grb2 import convertFcstFiles
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from loadconfigure import inPath, outPath, tmpPath, date
    
### call forecast conversion function w.r.t data assimilated at 00z long forecast hour.
convertFcstFiles(inPath, outPath, tmpPath, date, hr='00')
