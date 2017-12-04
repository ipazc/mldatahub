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

from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Queue, Lock
from queue import Empty
from threading import Thread
from time import sleep
from bson import ObjectId, BSON
from mldatahub.odm.token_dao import TokenDAO
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetElementDAO
from mldatahub.helper.timing_helper import Measure, now
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


class MultiprocessingTaskedObject(object):
    """
    Base class for counting tasks
    """
    __tasks_num = 0
    __tasks_lock = Lock()

    @property
    def tasks_count(self):
        """
        Getter for the tasks_count
        :return: number of tasks remaining for this object.
        """
        with self.__tasks_lock:
            return self.__tasks_num

    def _increase_tasks_count(self, value=1):
        """
        Increases the tasks count by the specified value
        :param value: number of tasks to increase the tasks count.
        """
        with self.__tasks_lock:
            self.__tasks_num += value

    def _decrease_tasks_count(self, value=1):
        """
        Decreases the tasks count by the specified value
        :param value: number of tasks to decrease the tasks count.
        """
        with self.__tasks_lock:
            self.__tasks_num -= value

    def sync(self):
        """
        Synchronizes the object until no more tasks are queued.
        :return:
        """
        tasks_remaining = self.tasks_count

        while tasks_remaining > 0:

            i("Tasks remaining: {}".format(tasks_remaining), same_line=True)
            tasks_remaining = self.tasks_count
            sleep(0.5)
        i("Tasks remaining: {}".format(tasks_remaining), same_line=False)


class MultiprocessingStoppableObject(object):
    """
    Base class for allowing stop in multiprocessing classes.
    """
    # Finish flag for stopping the the internal threads
    __finish = False

    # Flag for waiting for finish when __finish flag is True
    __wait_finish = True

    # Global Lock for flags.
    __lock = Lock()

    """
    GETTERS
    """

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


class FileBackuper(MultiprocessingTaskedObject, MultiprocessingStoppableObject):
    """
    Base class for backupers.
    """

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
                try:
                    with self.__files_lock:
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


class DatasetBackuper():
    """
    Backups a whole dataset
    """
    __backuper_pool = ThreadPoolExecutor(1)
    _backup_start = now()

    def __init__(self, file_backuper: FileBackuper):
        """
        Constructor of the class.
        The dataset backuper require a file backuper to store the files.
        :param file_backuper: FileBackuper to store the files.
        """
        self.file_backuper = file_backuper

    def __full_serialization(self, dataset: DatasetDAO):
        """
        Serializes the whole dataset into BSON format, including the elements (but not their content).
        The serial returned by this method represents the full dataset, and can be reverted back to the original
        database format.

        Warning!!: The content of the Dataset Elements is not included!

        :param dataset: DatasetDAO to fully serialize.

        :return: A tuple of 3 elements:\n
                \b1) BSON Serial of the dataset,\n
                \b2) List of elements' file-refs IDs. Useful to know which files should be stored along with this serial.\n
                \b3) header of the dataset (id, url prefix, title, description, dates, tags, ...)\n
        """
        header_serial = {
            '_id': str(dataset._id),
            'url_prefix': dataset.url_prefix,
            'title': dataset.title,
            'description': dataset.description,
            'reference': dataset.reference,
            'creation_date': dataset.creation_date,
            'modification_date': dataset.modification_date,
            'size': dataset.size,
            'tags': dataset.tags,
            'fork_count': dataset.fork_count,
            'forked_from_id': str(dataset.forked_from_id)
        }

        elements = [
            {
                '_id': element._id,
                '_previous_id': element._previous_id,
                'title': element.title,
                'description': element.description,
                'file_ref_id': element.file_ref_id,
                'http_ref': element.http_ref,
                'tags': element.tags,
                'addition_date': element.addition_date,
                'modification_date': element.modification_date,
                'dataset_id': element.dataset_id
            } for element in dataset.elements
        ]

        dataset_serial = {
            'header': header_serial,
            'elements': elements
        }

        return BSON.encode(dataset_serial), [element['file_ref_id'] for element in elements], header_serial

    def __full_deserialization(self, dataset_serial):
        """
        Fully deserializes a dataset serial, previously serialized with __full_serialization().
        Warning!! if the dataset ID already exists, it should be deleted first!

        Warning!!: The content of the Dataset Elements is not included!

        :param dataset_serial: BSON serial of a dataset

        :return: Tuple -> (DatasetDAO, list of elements)
        """
        decoded_dataset = BSON.decode(dataset_serial)
        dataset_header = decoded_dataset['header']

        dataset = DatasetDAO(
            url_prefix=dataset_header['url_prefix'],
            title=dataset_header['title'],
            description=dataset_header['description'],
            reference=dataset_header['reference'],
            tags=dataset_header['tags'],
            creation_date=dataset_header['creation_date'],
            modification_date=dataset_header['modification_date'],
            fork_count=dataset_header['fork_count']
        )

        if dataset_header['forked_from_id'] not in [None, "None"]:
            dataset.forked_from_id = dataset_header['forked_from_id']

        dataset._id = dataset_header['_id']

        elements = []
        for element_data in decoded_dataset['elements']:
            element = DatasetElementDAO(
                title=element_data['title'],
                description=element_data['description'],
                file_ref_id=element_data['file_ref_id'],
                http_ref=element_data['http_ref'],
                tags=element_data['tags'],
                addition_date=element_data['addition_date'],
                modification_date=element_data['modification_date'],
                dataset_id=dataset._id
            )
            element._id = element_data['_id']
            element._previous_id = element_data['_previous_id']
            elements.append(element)

        return dataset, elements

    def backup_dataset(self, dataset:DatasetDAO):
        """
        Backups a dataset into the backend.
        :param dataset:
        :return:
        """
        i("Backuping dataset {}.".format(dataset.url_prefix))
        dataset_serial, file_ids, header_serial = self.__full_serialization(dataset)
        d("Serialized dataset {}.".format(dataset.url_prefix))

        dataset_url_prefix = header_serial['url_prefix']
        body = dataset_serial

        d("Queued dataset {} to store.".format(dataset.url_prefix))
        self._store_dataset_(dataset_url_prefix, body)

        d("Backuping its files...")
        for file in file_ids:
            self.file_backuper.backup_file(file)

    def restore_dataset(self, dataset_url_prefix, date=None) -> DatasetDAO:
        """
        Restores the specified dataset to the specified date.
        :param dataset_url_prefix:
        :param date:
        :return:
        """
        date_msg = "latest" if date is None else str(date)

        i("Restoring dataset of url prefix {} from date '{}'".format(dataset_url_prefix, date_msg))
        previous_dataset = DatasetDAO.query.get(url_prefix=dataset_url_prefix)

        if previous_dataset is not None:
            d("Deleting previous dataset ({})".format(previous_dataset.url_prefix))
            previous_dataset.delete()
        else:
            d("No previous dataset found.")

        dataset_serial = self._retrieve_dataset_(dataset_url_prefix, date)

        d("Generating new dataset...")
        dataset, elements = self.__full_deserialization(dataset_serial)
        d("Generated dataset from serial.")
        files_ref_ids = [e.file_ref_id for e in elements]

        d("Queueing... restore of files")
        new_file_refs = self.__push_files_to_storage(files_ref_ids)

        d("Finished restoring {} files. Found {} ids to require update in the dataset".format(len(files_ref_ids), len(new_file_refs)))
        d("Updating ids...")

        for element in elements:
            if element.file_ref_id in new_file_refs:
                element.file_ref_id = new_file_refs[element.file_ref_id]

        d("Ids successfully updated.")

        # TODO: Store the tokens associated to this one.

        global_config.get_session().flush()
        d("Flushing...")

        return dataset

    def __push_files_to_storage(self, files_ref_ids: list):
        """
        Restores the specified files from the backup to the storage.
        Files are indexed by their SHA256 hash. Whenever a file is already found, they won't be updated.

        :param files_ref_ids: list of file ref ids to restore
        :return:
        """
        d("Queued {} files to restore".format(len(files_ref_ids)))
        futures = {self.file_backuper.restore_file(file_ref_id): file_ref_id for file_ref_id in files_ref_ids}
        # Let's do it in batches
        results = []
        ps = global_config.get_page_size()
        storage = global_config.get_storage() # type: MongoStorage
        d("Restoring in batches of {} files into storage".format(ps))

        # This dict holds all the file_ref_IDs that should be overriden
        override_ids = {}

        for future in as_completed(futures):
            results.append({futures[future]: future.result()})

            if len(results) > 0 and len(results) % ps == 0:
                ids = [k for k in results]
                values = [results[k] for k in ids]

                list_of_ids = storage.put_files_contents(ids, force_ids=values)

                # We need to check which file ref ids have not been replaced.
                override_ids.update({old_id: new_id for old_id, new_id in zip(ids, list_of_ids)})

        return override_ids

    def get_available_dates(self, dataset_url_prefix) -> list:
        """
        Retrieves the available backup dates for a given dataset url prefix
        :param dataset_url_prefix: the full dataset url prefix,
        :return: list of dates in string format (YYYYMMDD)
        """
        pass

    def _store_dataset_(self, url_prefix, body):
        pass

    def _retrieve_dataset_(self, url_prefix, date):
        pass