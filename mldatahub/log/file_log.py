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

from multiprocessing import Lock
from threading import Thread
from time import sleep
from mldatahub.helper.timing_helper import Measure

__author__ = 'Iván de Paz Centeno'

LOG_UPDATE_TIME_FREQUENCY = 10  # seconds


class FileLog(object):
    """
    Singleton class for storing the log into a file in batches.
    """
    __file_uri=None
    __finish_requested = False
    __lock = Lock()
    __buffer = []
    __thread_storer = None

    def __init__(self):
        self.__thread_storer = Thread(target=self.__thread_func__, daemon=True)
        self.__thread_storer.start()

    def set_file_uri(self, new_uri):
        self.__file_uri = new_uri

    def get_file_uri(self):
        return self.__file_uri

    def queue_line(self, string, same_line=False):
        with self.__lock:
            if same_line:
                if len(self.__buffer) == 0:
                    self.__buffer.append(string)
                else:
                    self.__buffer[-1] = string
            else:
                self.__buffer.append(string)

    def finish(self, wait_for_finish=False):
        """
        Tells the log file to stop immediately logging content into the file.
        :param wait_for_finish: Flag that, if set, causes the thread to wait until the file log is completely written.
        """
        with self.__lock:
            self.__finish_requested = True

        buffer_length = 1

        while wait_for_finish and buffer_length > 0:
            with self.__lock:
                buffer_length = len(self.__buffer)
            sleep(1)

    def __is_finish_requested(self):
        with self.__lock:
            return self.__finish_requested

    def __thread_func__(self):
        with Measure() as timing:
            while not self.__is_finish_requested():

                if timing.elapsed().seconds > LOG_UPDATE_TIME_FREQUENCY:
                    self.__store_buffer__()
                    timing.reset()

                sleep(1)

        self.__store_buffer__()

    def __store_buffer__(self):
        if self.__file_uri is None:
            return

        with self.__lock:
            if len(self.__buffer) > 0:
                with open(self.__file_uri, "a+") as f:
                    chars_written = f.write("\n".join(self.__buffer))
                    self.__buffer = []

file_logger = FileLog()
