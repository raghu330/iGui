"""
This is simple script to invoke parallel main function from um2grb2_parallel_v1
and pass the umglaa_pb as argument.
This parallel uses 10 processors for forecast hours 

Written by : Arulalan.T
Date : 10.Nov.2015
"""

import os, sys 
from um2grb2_parallel_v1 import main 

main('umglaa_pb')
