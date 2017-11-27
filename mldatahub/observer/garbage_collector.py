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
import datetime
from mldatahub.helper.timing_helper import now, Measure, time_left_as_str
from mldatahub.config.config import global_config
from mldatahub.odm.dataset_dao import DatasetElementDAO

TIMER_TICK = global_config.get_garbage_collector_timer_interval()  # seconds

__author__ = 'Iván de Paz Centeno'


def segment(l, count):
    b = 0
    blocks = len(l) // count
    for b in range(blocks):
        yield l[b*count:(b+1)*count]

    if len(l) % count > 0:
        yield l[(blocks)*count:]


def progress(current, max, string):
    print("\r[{:05.2f}%] {}/{} - {}                  "
          "".format(round(current/max * 100, 2), current, max, string), end="", flush=True)


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
    last_tick = now() - datetime.timedelta(minutes=60)

    def __init__(self):
        self.thread = Thread(target=self.__thread_func, daemon=True)
        self.thread.start()
        self.previous_unused_files = {}

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

    def collect_unused_files(self):
        """
        Searchs for unused files in the DB and returns a list of ids.
        :return: list with IDs of the unused files.
        """
        unused_files = []

        files_count = len(self.storage)

        print("[GC] {} files to be checked.".format(files_count))

        sleep_batch=50
        files_per_second = [0]
        files_per_second_avg = 0

        with Measure() as timing:
            for index, file in enumerate(self.storage):
                if DatasetElementDAO.query.get(file_ref_id=file._id) is None:
                    unused_files.append(file._id)

                if index % sleep_batch == 0:
                    sleep(0.1)

                if len(files_per_second) < 5:
                    files_per_second[-1] += 1
                    if timing.elapsed().seconds >= 1:
                        files_per_second.append(0)
                        timing.reset()

                    files_per_second_avg = sum(files_per_second) / len(files_per_second)
                    time_remaining = ""
                else:
                    time_remaining = " {} remaining".format(time_left_as_str((len(self.storage) - index) // files_per_second_avg))

                if self.__stop_requested():
                    break

                progress(index+1, files_count, "{} files are orphan.{}".format(len(unused_files), time_remaining))

        print("")
        return unused_files

    def do_garbage_collect(self):
        print("[GC] Collecting garbage...")

        global_config.get_session().clear()

        # 1. We retrieve the unused files.
        unused_files = self.collect_unused_files()

        # 2. We check how many unused files are in common with the previous unused files.
        new_unused_files = []
        remove_files = []
        print("[GC] Comparing {} unused files to previous {} unused files.".format(len(unused_files), len(self.previous_unused_files)))

        print("[GC] Cleaning {} elements...".format(len(remove_files)))

        for index, file in enumerate(unused_files):
            if file in self.previous_unused_files:
                remove_files.append(file)
            else:
                new_unused_files.append(file)

        # 3. We delete by batches
        files_count = 0
        for list_ids in segment(remove_files, 50):
            files_count += len(list_ids)
            self.storage.delete_files(list_ids)
            progress(files_count, len(remove_files), "{} files garbage collected.".format(files_count))
            sleep(0.1)

        self.previous_unused_files = set(new_unused_files)

        print("[GC] Cleaned {} elements...".format(len(remove_files)))

        return files_count

    def stop(self, wait_for_finish=True):
        with self.lock:
            self.do_stop = True

        if wait_for_finish:
            self.thread.join()

    def __del__(self):
        self.stop()
