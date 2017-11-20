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

from concurrent.futures import ThreadPoolExecutor

import json
from queue import Queue, Empty
from threading import Thread, Lock
from time import sleep
from bson import ObjectId
from pyzip import PyZip
from mldatahub.config.config import global_config
from mldatahub.helper.timing_helper import now, Measure
from pygfolder import PyGFolder

__author__ = 'Iván de Paz Centeno'


UPLOAD_TIME = DOWNLOAD_TIME = 1  # seconds
VERBOSE=1

def v(string, same_line=False):
    if 0 < VERBOSE <= 1:
        if same_line: end = ""
        else:   end = "\n"

        print("\r[BAK-GOOGLE] {}     ".format(string), end=end, flush=True)

def i(string):
    if 0 < VERBOSE <= 2:
        print("[BAK-GOOGLE] {}".format(string))

def as_bytes(c):

    if type(c) is dict or type(c) is list:
        result = json.dumps(c).encode()
    elif type(c) is str:
        result = c.encode()
    else:
        result = str(c).encode()

    return result


class BackuperGoogle(object):

    __tasks_num = 0
    __lock = Lock()
    __finish = False
    __wait_finish = True
    __backup_data_queue = Queue()
    __restore_data_queue = Queue()
    __files_lock = Lock()
    __temp_storage = {}
    __download_pool = ThreadPoolExecutor(2)

    def __init__(self, storage=None):
        if storage is None:
            storage = global_config.get_storage()

        self.storage = storage

        drive_folder = global_config.get_google_drive_folder()
        i("Initializing PYGFolder...")
        self.pygfolder = PyGFolder()
        i("Done")

        if drive_folder.endswith("/"): drive_folder = drive_folder[:-1]

        init_file = drive_folder + "/init"
        i("Creating init file with current timestamp...")
        self.pygfolder[init_file] = as_bytes(now())
        i("Done")
        i("Accessing folder...")
        self.pygfolder = self.pygfolder[drive_folder]
        i("Done")
        self.uploader = Thread(target=self.__uploader__, daemon=True)
        self.downloader = Thread(target=self.__downloader__, daemon=True)
        self.uploader.start()
        self.downloader.start()

    @property
    def tasks_count(self):
        with self.__lock:
            return self.__tasks_num

    @tasks_count.setter
    def tasks_count(self, value):
        with self.__lock:
            self.__tasks_num = value

    def increase_tasks_count(self, value=1):
        with self.__lock:
            self.__tasks_num += value

    def decrease_tasks_count(self, value=1):
        with self.__lock:
            self.__tasks_num -= value

    @property
    def exit(self):
        with self.__lock:
            return self.__finish

    @property
    def wait_for_finish(self):
        with self.__lock:
            return self.__wait_finish

    @wait_for_finish.setter
    def wait_for_finish(self, value=True):
        with self.__lock:
            self.__wait_finish = value

    def __downloader__(self):

        def download_packet(packet_files):

            filtered_files = []

            with self.__files_lock:
                for p in packet_files:
                    if p in self.__temp_storage:
                        self.__temp_storage[p]['count'] += 1
                    else:
                        filtered_files.append(p)

            packet = self.__retrieve_packet__(filtered_files)

            i("Packet for {} elements retrieved successfully".format(len(packet_files)))
            with self.__files_lock:
                self.__temp_storage.update(packet)
            i("Temporal storage updated with packet data")

        packet_files = []

        with Measure() as timing:
            while not self.exit:

                if timing.elapsed().seconds > DOWNLOAD_TIME and len(packet_files) > 0:
                    i("Downloading packet of size {} (number of elements, not bytes)".format(len(packet_files)))
                    download_packet(packet_files)
                    timing.reset()
                    packet_files = []

                sleep(0.01)

                try:
                    file_id = self.__restore_data_queue.get(block=False)
                except Empty:
                    file_id = None

                if file_id is not None:
                    packet_files.append(file_id)

            if len(packet_files) > 0:
                download_packet(packet_files)

    def __retrieve_packet__(self, file_ids_list):
        hashes = {str(id)[:-3] for id in file_ids_list}

        table_indexes = {}

        i("Retrieving {} table indexes...".format(len(hashes)))
        for hash_id in hashes:
            table_indexes.update(json.loads(self.pygfolder[hash_id].decode()))
        i("Done")

        table_content = {}
        for file_id in file_ids_list:
            if file_id in table_content: continue

            i("Downloading Zip file for element {}...".format(str(file_id)))
            zip_uri = table_indexes[str(file_id)]
            zip_content = PyZip().from_bytes(self.pygfolder[zip_uri])
            i("Done.")
            table_content.update(zip_content)

        packet = {str(file_id): {'content':table_content[str(file_id)], 'count': 1} for file_id in file_ids_list}

        self.decrease_tasks_count(len(packet))

        return packet

    def __uploader__(self):
        def build_packet(files_list):
            return {str(file_repr.id): file_repr.content for file_repr in self.storage.get_files(files_list)}

        def store_packet(packet):
            self.__store_packet__(packet, str(ObjectId()))

        packet_files = []

        with Measure() as timing:
            while not self.exit:
                if timing.elapsed().seconds > UPLOAD_TIME and len(packet_files) > 0:
                    i("Writting packet of size {} (number of elements, not bytes)".format(len(packet_files)))
                    store_packet(build_packet(packet_files))
                    timing.reset()
                    packet_files = []

                sleep(0.01)

                try:
                    file_id = self.__backup_data_queue.get(block=False)
                except Empty:
                    file_id = None

                if file_id is not None:
                    packet_files.append(file_id)

            # There is a chance of having a packet partially filled when the exit is requested.
            if len(packet_files) > 0:
                store_packet(build_packet(packet_files))

    def __store_packet__(self, packet, name):
        content = PyZip(packet).to_bytes(True)
        i("Compressed into {} bytes".format(len(content)))
        self.pygfolder[name] = content
        i("Saved into GDrive: {}".format(name))

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

        i("Creating {} hash tables".format(len(hashes)))
        # 2. Update the indexes for the hashes:
        for hash in hashes:
            try:
                index_table = json.loads(self.pygfolder[hash].decode())
            except:
                # Let's create the file with the indexes
                index_table = {}

            if len(index_table) > 0:
                i("Appending to an existing index... ({} previous elements)".format(len(index_table)))

            index_table.update({id: name for id in packet if id.startswith(hash)})

            self.pygfolder[hash] = json.dumps(index_table).encode()
            i("Saved index table into {}".format(hash))

        self.decrease_tasks_count(len(packet))

    def backup_file(self, file_id:ObjectId):
        """
        Saves the specified file into Google Drive
        :param file:
        :return:
        """
        self.increase_tasks_count()
        self.__backup_data_queue.put(file_id)

    def sync(self):

        tasks_remaining = self.tasks_count

        while tasks_remaining > 0:

            v("Tasks remaining: {}".format(tasks_remaining), same_line=True)
            tasks_remaining = self.tasks_count
            sleep(0.5)
        v("Tasks remaining: {}".format(tasks_remaining), same_line=False)


    def restore_file(self, file_id:ObjectId):
        """
        Restores the specified file from the backup (if available)
        :param file_id: file id to restore from backup
        :return: the content of the file
        """

        def check_restore_file(file_id):
            file_id = str(file_id)
            file_content = None

            while file_content is None:
                with self.__files_lock:
                    try:
                        file_content = self.__temp_storage[file_id]['content']
                        self.__temp_storage[file_id]['count'] -= 1
                    except KeyError:
                        sleep(0.5)

            return file_content

        # 1. We queue the file_id to be restored
        self.__restore_data_queue.put(file_id)

        # 2. We submit a task to the pool to check for the restore
        future = self.__download_pool.submit(check_restore_file, file_id)

        return future
