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
from multiprocessing import Queue, Lock
from queue import Empty
from threading import Thread
from time import sleep
from bson import ObjectId
from mldatahub.helper.timing_helper import Measure
from mldatahub.config.config import global_config
from mldatahub.log.logger import Logger

__author__ = 'Iván de Paz Centeno'


UPLOAD_TIME = DOWNLOAD_TIME = 2  # seconds

logger = Logger(module_name="BAK",
                verbosity_level=global_config.get_log_verbosity(),
                log_file=global_config.get_log_file())

# Some short-names for the log functions.
i = logger.info
d = logger.debug
e = logger.error
w = logger.warning


class Backuper(object):
    """
    Base class for backupers.
    """

    # Tasks being done by the object
    __tasks_num = 0

    # Multithreading Lock
    __lock = Lock()

    # Finish flag for stopping the the internal threads
    __finish = False

    # Flag for waiting for finish when __finish flag is True
    __wait_finish = True

    # Queue for storing backups
    __backup_data_queue = Queue()

    # Queue for restoring backups
    __restore_data_queue = Queue()

    # Multithreading Lock for files
    __files_lock = Lock()

    # Temporal storage cache
    __temp_storage = {}

    # Thread pool for downloads.
    __download_pool = ThreadPoolExecutor(2)

    """
    GETTERS
    """

    @property
    def tasks_count(self):
        """
        Getter for the tasks_count
        :return: number of tasks remaining for this object.
        """
        with self.__lock:
            return self.__tasks_num

    @property
    def is_exit_requested(self):
        """
        Getter for the exit flag
        :return: True if exit was requested, false otherwise.
        """
        with self.__lock:
            return self.__finish

    @property
    def should_I_wait_for_finish(self):
        """
        Getter for the wait for finish flag.
        :return: True if should wait for finish, False otherwise.
        """
        with self.__lock:
            return self.__wait_finish

    """
    METHODS
    """

    def __init__(self, storage=None):
        """
        Constructor of the backuper.
        It is going to initialize 2 threads: one for uploads and other for downloads.
        :param storage: storage to use. If None, it will fall back to the one from global_config.
        """
        if storage is None:
            storage = global_config.get_storage()

        self.storage = storage

        self.uploader = Thread(target=self.__uploader__, daemon=True)
        self.downloader = Thread(target=self.__downloader__, daemon=True)
        self.uploader.start()
        self.downloader.start()

    def _increase_tasks_count(self, value=1):
        """
        Increases the tasks count by the specified value
        :param value: number of tasks to increase the tasks count.
        """
        with self.__lock:
            self.__tasks_num += value

    def _decrease_tasks_count(self, value=1):
        """
        Decreases the tasks count by the specified value
        :param value: number of tasks to decrease the tasks count.
        """
        with self.__lock:
            self.__tasks_num -= value


    """
    THREAD FUNCS
    """

    def __downloader__(self):
        """
        Downloader thread function.
        """

        def download_packet(packet_files):
            """
            Downloads a packet with the given file IDs
            :param packet_files: list of File IDs to download
            :return: Packet
            """
            filtered_files = []

            # Don't fetch files that already have been fetched and are still in the cache (__temp_storage).
            with self.__files_lock:
                for p in packet_files:
                    if p in self.__temp_storage:
                        self.__temp_storage[p]['count'] += 1
                    else:
                        filtered_files.append(p)

            # We retrieve only those files that haven't been retrieved yet
            packet = self.__retrieve_packet__(filtered_files)
            d("Packet for {} elements retrieved successfully".format(len(packet_files)))

            # Then we update the temporal cache with the packet. This cache will get released whenever it is read
            with self.__files_lock:
                self.__temp_storage.update(packet)
            d("Temporal storage updated with packet data")

        # Meanwhile, outside of the download_packet function...
        packet_files = []

        # We wrap the donwload requests in batches, so it is more optimus.
        with Measure() as timing:
            while not self.is_exit_requested:

                if timing.elapsed().seconds > DOWNLOAD_TIME :
                    if len(packet_files) > 0:

                        # Each DOWNLOAD_TIME seconds we download a packet, only if there are requests done.
                        d("Downloading packet of size {} (number of elements, not bytes)".format(len(packet_files)))

                        download_packet(packet_files)
                        # The downloaded packet is stored in a temporal buffer called __temp_storage. It is a Dictionary
                        # associating file ID with its content.
                        packet_files = []
                    timing.reset()

                sleep(0.01)

                try:
                    file_id = self.__restore_data_queue.get(block=False)
                    d("Picked file from queue.")

                except Empty:
                    file_id = None

                if file_id is not None:
                    packet_files.append(file_id)

            # There is a chance of having a packet partially filled when the exit is requested.

            if len(packet_files) > 0:
                download_packet(packet_files)

    def __uploader__(self):
        """
        Uploader thread function
        """

        def build_packet(files_list):
            """
            Builds a packet for the given file ID list.
            :param files_list: list of file IDs
            :return: files packet. It is a dict with format ID -> Content
            """
            return {str(file_repr.id): file_repr.content for file_repr in self.storage.get_files(files_list)}

        def store_packet(packet):
            """
            Stores this packet into the backend
            :param packet:
            :return:
            """
            self.__store_packet__(packet, str(ObjectId()))

        packet_files = []

        """
        Like before in the downloader, we upload in batches. It is the optimized way to handle this.
        """
        with Measure() as timing:

            while not self.is_exit_requested:
                """
                We group the requests by batches here
                """
                if timing.elapsed().seconds > UPLOAD_TIME:
                    if len(packet_files) > 0:
                        d("Writting packet of size {} (number of elements, not bytes)".format(len(packet_files)))
                        store_packet(build_packet(packet_files))
                        packet_files = []

                    timing.reset()

                sleep(0.01)

                try:
                    file_id = self.__backup_data_queue.get(block=False)
                    d("Picked file from queue.")
                except Empty:
                    file_id = None

                if file_id is not None:
                    packet_files.append(file_id)

            # There is a chance of having a packet partially filled when the exit is requested.
            if len(packet_files) > 0:
                store_packet(build_packet(packet_files))

    def backup_file(self, file_id: ObjectId):
        """
        Saves the specified file into the Backend
        :param file_id: file ID to store into backend
        :return:
        """
        self._increase_tasks_count()
        self.__backup_data_queue.put(file_id)

    def restore_file(self, file_id:ObjectId):
        """
        Restores the specified file from the backup (if available)
        :param file_id: file id to restore from backup
        :return: the content of the file wrapped in a Future class.
                 You can get this content calling the method result().
        """

        def check_restore_file(file_id):
            """
            Threaded func that is running in background, and waiting for the file to be accessible.
            """
            file_id = str(file_id)
            file_content = None

            while file_content is None:
                with self.__files_lock:
                    try:
                        file_content = self.__temp_storage[file_id]['content']
                        self.__temp_storage[file_id]['count'] -= 1

                        if self.__temp_storage[file_id]['count'] == 0:
                            del self.__temp_storage[file_id]

                    except KeyError:
                        sleep(0.5)

            return file_content

        self._increase_tasks_count()

        # 1. We queue the file_id to be restored
        self.__restore_data_queue.put(file_id)

        # 2. We submit a task to the pool to check for the restore
        future = self.__download_pool.submit(check_restore_file, file_id)

        return future

    def sync(self):
        """
        Synchronizes the backuper until no more tasks are queued.
        :return:
        """
        tasks_remaining = self.tasks_count

        while tasks_remaining > 0:

            i("Tasks remaining: {}".format(tasks_remaining), same_line=True)
            tasks_remaining = self.tasks_count
            sleep(0.5)
        i("Tasks remaining: {}".format(tasks_remaining), same_line=False)

    def __store_packet__(self, packet, name):
        """
        Stores the packet in the backend, with the specified name.
        :param packet: packet to store in the backend. Must be a dict with format FileID -> Content
        :param name: Name of the packet.
        :return:
        """
        pass

    def __retrieve_packet__(self, file_ids_list):
        """
        Retrieves a packet filled with the content of the specified file ids, from the backend
        :param file_ids_list: list of file IDs to retrieve
        :return: files packet. It is a dict with format FileID -> Content
        """
        return {}