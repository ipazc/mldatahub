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

from contextlib import contextmanager
import datetime

__author__ = 'Iván de Paz Centeno'


def time_left_as_str(seconds):
    result = []

    t = ["s", "m", "h"]
    d = [60, 60, 24]

    for m, n in zip(t, d):

        v = seconds % n
        seconds //= n

        result.append("{:02}{}".format(int(v), m))

    return " ".join(result[::-1])

def now():
    return datetime.datetime.now()

class TimeMeasurement():
    def __init__(self, delta):
        self.delta = delta

    @property
    def days(self):
        return self.delta.days + self.delta.seconds/60/60/24 + self.delta.microseconds/1000/1000/60/60/24

    @property
    def hours(self):
        return self.delta.days*24 + self.delta.seconds/60/60 + self.delta.microseconds/1000/1000/60/60

    @property
    def minutes(self):
        return self.delta.days*24*60 + self.delta.seconds/60 + self.delta.microseconds/1000/1000/60

    @property
    def seconds(self):
        return self.delta.days*24*60*60 + self.delta.seconds + self.delta.microseconds/1000/1000

    @property
    def milliseconds(self):
        return self.delta.days*24*60*60*1000 + self.delta.seconds*1000 + self.delta.microseconds/1000


class FutureTime(object):
    def __init__(self, time):
        self.time = time

    def elapsed(self):
        return TimeMeasurement(now() - self.time)

    def reset(self):
        self.time = now()

@contextmanager
def Measure():
    time = now()
    yield FutureTime(time)
