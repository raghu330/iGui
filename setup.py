#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 
# (C) 2015-2016 Arulalan.T 
# iGui Project

from distutils.core import setup
from codecs import open

setup(name='iGui',
      version='1.0a',
      description='iris gui to convert models data from pp2grib2 as well as visualize it',
      authors='T. Arulalan, Raghavendra S. Mupparthy',
      authors_email=('arulalan@ncmrwf.gov.in, arulalant@gmail.com', 'raghav@ncmrwf.gov.in'),
      url='https://github.com/arulalant/iGui',
      packages=['g2utils'],
      license='GPLv3',
      classifiers=[
            'Development Status :: v1.0a - Alpha',
            ('License :: OSI Approved :: '
             'GNU General Public License v3 or later (GPLv3+)'),
            'Operating System :: POSIX :: Linux',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: POSIX',
            'Operating System :: POSIX :: AIX',            
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.7',            
            'Topic :: Scientific/Engineering',
            'Topic :: Scientific/Engineering :: pp2grib2, um2grib2, visualization',
            ],
      long_description=open('README.md','r','UTF-8').read(),
      download_url='https://github.com/arulalant/iGui/archive/master.zip',#pip
      )



