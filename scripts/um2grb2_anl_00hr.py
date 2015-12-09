"""
This is simple script to invoke parallel conversion function from um2grb2
and pass the assimilated / forecasted hour as argument.

hour : 00
Output : It creates analysis - 1 file (um_ana_00hr_date.grib2) 
Written by : Arulalan.T
Date : 07.Dec.2015
"""

from g2utils.um2grb2 import convertAnlFiles
from loadconfigure import inPath, outPath, tmpPath, date

### call analysis conversion function w.r.t data assimilated during short forecast hour.
convertAnlFiles(inPath, outPath, tmpPath, date, hr='00')

