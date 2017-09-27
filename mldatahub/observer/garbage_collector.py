#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import threading
from threading import Thread
from time import sleep
from mldatahub.config.config import now, global_config
from mldatahub.odm.dataset_dao import DatasetElementDAO

TIMER_TICK = global_config.get_garbage_collector_timer_interval()  # seconds

__author__ = 'Iván de Paz Centeno'

class GarbageCollector(object):
    """
    Class whose purpose is to clean the filesystem from files that lost references.
    This is a GarbageCollector.

    In order to do this optimally, fortunately the storage class keeps a set with all the filenames he worked with,
    it is guaranteed that it is up-to-date and persistent across server reboots.
    It is better to iterate over this structure than the filesystem itself.
    """
    lock = threading.Lock()
    do_stop = False
    local_storage = global_config.get_local_storage()
    last_tick = now()

    def __init__(self):
        self.thread = Thread(target=self.__thread_func)
        self.thread.start()

    def __stop_requested(self):
        with self.lock:
            value = self.do_stop
        return value

    def __thread_func(self):
        while not self.__stop_requested():
            if (now() - self.last_tick).total_seconds() > TIMER_TICK:
                with self.lock:
                    self.last_tick = now()

                self.do_garbage_collect()
            sleep(1)

    def do_garbage_collect(self):
        refs_in_use = {element.file_ref_id for element in DatasetElementDAO.query.find({})}
        garbage_files = {ref for ref in self.local_storage if ref not in refs_in_use}

        for file_id in garbage_files:
            try:
                self.local_storage.delete_file_content(file_id)
            except FileNotFoundError:
                print("skipped {} as it wasn't found on filesystem".format(file_id))

        self.local_storage._save_file_list()
        print("[GC] Cleaned {} elements".format(len(garbage_files)))

    def stop(self, wait_for_finish=True):
        with self.lock:
            self.do_stop = True

        if wait_for_finish:
            self.thread.join()

    def __del__(self):
        self.stop()
