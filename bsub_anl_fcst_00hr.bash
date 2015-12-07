#!/bin/bash
#
#BSUB -a poe                  # set parallel operating environment
#BSUB -J um2grib2_conv        # job name
#BSUB -W 00:30                # wall-clock time (hrs:mins)
#BSUB -n 32                   # number of tasks in job
#BSUB -q small              	# queue
#BSUB -e errors.%J.hybrid     # error file name in which %J is replaced by the job ID
#BSUB -o output.%J.hybrid     # output file name in which %J is replaced by the job ID
 
/gpfs2/home/umtid/Pythons/Python-2.7.9/bin/python /gpfs2/home/umtid/iGui/utils/um2grb2_anl_fcst_00hr.py

