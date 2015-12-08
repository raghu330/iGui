"""
This is simple script to invoke parallel conversion function from um2grb2
and pass the assimilated / forecasted hour as argument.

hour : 06
Output : It creates analysis - 1 file (um_ana_06hr_date.grib2).

Written by : Arulalan.T
Date : 07.Dec.2015
"""

import os, sys 
from um2grb2 import convertAnlFiles 

# call analysis conversion function w.r.t data assimilated during short forecast hour.
convertAnlFiles(hr='06')

