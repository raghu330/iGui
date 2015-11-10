#!/bin/bash
#
#BSUB -a poe                  # set parallel operating environment
#BSUB -J hybrid_job_name      # job name
#BSUB -W 00:20                # wall-clock time (hrs:mins)
#BSUB -n 32                   # number of tasks in job
#BSUB -q small              	# queue
#BSUB -e errors.%J.hybrid     # error file name in which %J is replaced by the job ID
#BSUB -o output.%J.hybrid     # output file name in which %J is replaced by the job ID
 
mpirun.lsf /gpfs2/home/raghav/Iris/datCnvrsn/iGui/um2grb2_parallel_v2.py
