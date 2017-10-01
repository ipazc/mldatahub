#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# MLDataHub
# Copyright (C) 2017 Iván de Paz Centeno <ipazc@unileon.es>.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.


import sys
from setuptools import setup, setuptools

__author__ = 'Iván de Paz Centeno'


def version():
    with open("version.py") as f:
        _version = f.readline().split("version=")[1]
    return _version

def readme():
    with open('README.rst') as f:
        return f.read()

if sys.version_info < (3, 4, 1):
    sys.exit('Python < 3.4.1 is not supported!')

setup(name='mldatahub',
      version='0.0.1',
      description='REST API hub for storing ML datasets.',
      long_description=readme(),
      url='http://github.com/ipazc/mldatahub',
      author='Iván de Paz Centeno',
      author_email='ipazc@unileon.es',
      license='GNU GPLv3 or later',
      packages=setuptools.find_packages(),
      install_requires=[
          "pyzip",
          "flask",
          "flask_restful",
          "ming"
      ],
      test_suite='nose.collector',
      tests_require=['nose'],
      include_package_data=True,
      keywords="machine-learning data hub datasets dataset store RESTAPI restful",
      zip_safe=False)