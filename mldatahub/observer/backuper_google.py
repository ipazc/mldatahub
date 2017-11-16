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

import json
from queue import Queue
from threading import Thread, Lock
from time import sleep
from pyzip import PyZip
from mldatahub.config.config import global_config
from mldatahub.helper.timing_helper import now, Measure

from pygfolder import PyGFolder
from mldatahub.odm.file_dao import FileDAO

__author__ = 'Iván de Paz Centeno'


UPLOAD_TIME = 10 # seconds


def as_bytes(c):

    if type(c) is dict or type(c) is list:
        result = json.dumps(c).encode()
    elif type(c) is str:
        result = c.encode()
    else:
        result = str(c).encode()

    return result

class BackuperGoogle(object):

    tasks_num = 0
    lock = Lock()
    finish = False
    backup_data_queue = Queue()

    def __init__(self):
        drive_folder = global_config.get_google_drive_folder()
        self.pygfolder = PyGFolder("")

        if drive_folder.endswith("/"): drive_folder = drive_folder[:-1]

        drive_folder += "/init"
        self.pygfolder[drive_folder] = as_bytes(now())
        self.pygfolder = self.pygfolder[global_config.get_google_drive_folder()]
        self.timer = Thread(target=self.__uploader__, daemon=True)
        self.timer.start()

    @property
    def tasks_count(self):
        with self.lock:
            return self.tasks_num

    @tasks_count.setter
    def tasks_count(self, value):
        with self.lock:
            self.tasks_num = value

    def increase_tasks_count(self, value=1):
        with self.lock:
            self.tasks_num += value

    def decrease_tasks_count(self, value=1):
        with self.lock:
            self.tasks_num -= value

    @property
    def exit(self):
        with self.lock:
            return self.finish

    def __uploader__(self):
        name = None
        packet = {}

        with Measure() as timing:
            while not self.exit:

                if timing.elapsed().seconds > UPLOAD_TIME:
                    timing.reset()
                    self.__store_packet(packet, name)
                    name = None
                    packet = {}

                sleep(0.01)

                element = self.backup_data_queue.get(block=False)

                if element is not None and len(element) > 1:
                    packet[element[0]] = element[1]

                if name is None:
                    name = element[0]

    def __store_packet(self, packet, name):
        content = PyZip(packet).to_bytes(True)
        self.pygfolder[name] = content

        # We must write the index reference.
        # How to do that?
        # We pick the [0:-3] elements' id as the hash of the index file that contains it.
        # We retrieve all the different indexes required by the stored elements
        # We save back into the indexes each of the element's ids and the link to the file that is hosting it.

        # Let us see an example:

        # Imagine that we have a dataset with elements whose file refs points to the ids XXXXX01e, XXXXX01f, XXXXX01g
        # and XXXXD000
        # If we want to know how to locate these files, we only need to take the [: -3] of each of them.
        # We get 2 different values doing that: "XXXXX" and "XXXXD". Thus, we only need to download those two indexes
        # to know where to locate the elements.

        # Each index file will contain the whole ID of each of the elements, pointing to the ZIP file that contains them
        # Next step is to download this ZIP file and to get the elements' content from there.

        # Summarizing:

        # 1. Get the id of the hash for elements holders
        hashes = {id[:-3] for id in packet}

        # 2. Update the indexes for the hashes:
        for hash in hashes:
            try:
                index_table = json.loads(self.pygfolder[hash].decode())
            except:
                # Let's create the file with the indexes
                index_table = {id:name for id in packet if id.startswith(hash)}

            self.pygfolder[hash] = json.dumps(index_table).encode()

    def backup_file(self, file: FileDAO):
        """
        Saves the specified file into Google Drive
        :param file:
        :return:
        """
        self.backup_data_queue.put([file._id, file.content])

