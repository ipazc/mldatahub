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

import threading
from threading import Thread
from time import sleep

from mldatahub.storage.generic_storage import GenericStorage
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
    It is better to iterate over this structure rather than the filesystem itself.
    """
    lock = threading.Lock()
    do_stop = False
    storage = global_config.get_storage() # type: GenericStorage
    last_tick = now()

    def __init__(self):
        self.thread = Thread(target=self.__thread_func, daemon=True)
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
        print("[GC] Exited.")

    def do_garbage_collect(self):
        global_config.session.clear()
        refs_in_use = {element.file_ref_id for element in DatasetElementDAO.query.find({})}
        garbage_files = {file._id for file in self.storage if file._id not in refs_in_use}

        if len(garbage_files) > 0:
            self.storage.delete_files(list(garbage_files))

        print("[GC] Cleaned {} elements".format(len(garbage_files)))
        return len(garbage_files)

    def stop(self, wait_for_finish=True):
        with self.lock:
            self.do_stop = True

        if wait_for_finish:
            self.thread.join()

    def __del__(self):
        self.stop()
