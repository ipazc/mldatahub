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
import os
from dateutil.relativedelta import relativedelta
from ming import create_datastore
from ming.odm import ThreadLocalODMSession
import datetime

__author__ = 'Iván de Paz Centeno'

HOME = os.path.expanduser("~")


def now():
    return datetime.datetime.now()

def token_future_end():
    return now() + relativedelta(months=+1)


class GlobalConfig(object):

    def __init__(self, config_values=None):
        if config_values is None:
            config_values = {}

        self.config_values = config_values
        self.session = None
        self.local_storage = None

        self.load_from_file()

    def load_from_file(self):
        try:
            with open("/etc/mldatahub/config.json") as f:
                self.config_values = json.load(f)

                self.config_values['local_storage_uri'] = self.config_values['local_storage_uri'].replace("$HOME", HOME)
                self.config_values['storage_cache_file'] = self.config_values['storage_cache_file'].replace("$HOME", HOME)

            print("Config read:")
            for k, v in self.config_values.items():
                print("{}: {}".format(k, v))
        except FileNotFoundError as ex:
            print("Config file not found. Running on default values.")

    def set_session_uri(self, new_uri):
        self.config_values['session_uri'] = new_uri

    def set_local_storage_uri(self, new_uri):
        self.config_values['local_storage_uri'] = new_uri

    def set_storage_cache_file(self, new_storage_filename):
        self.config_values['storage_cache_file'] = new_storage_filename

    def set_page_size(self, new_page_size):
        self.config_values['page_size'] = new_page_size

    def set_save_interval(self, new_save_interval):
        self.config_values['save_interval'] = new_save_interval

    def set_garbage_collector_timer_interval(self, new_interval):
        self.config_values['garbage_collector_time_interval'] = new_interval

    def set_max_access_times(self, new_max_access_times):
        self.config_values['new_max_access_times'] = new_max_access_times

    def set_access_reset_time(self, new_access_reset_time):
        self.config_values['access_reset_time'] = new_access_reset_time

    def get_session(self):
        if 'session_uri' not in self.config_values:
            global_config.set_session_uri("mongodb://localhost:27017/mlhubdata")

        if self.session is None:
            self.session = ThreadLocalODMSession(bind=create_datastore(self.config_values['session_uri']))
        return self.session

    def get_local_storage(self):

        if 'local_storage_uri' not in self.config_values:
            self.config_values['local_storage_uri'] = os.path.join(HOME, 'storage')

        if self.local_storage is None:
            from mldatahub.storage.local.local_storage import LocalStorage
            self.local_storage = LocalStorage(self.config_values['local_storage_uri'])
        return self.local_storage

    def get_storage_cache_file(self):
        if 'storage_cache_file' not in self.config_values:
            self.config_values['storage_cache_file'] = os.path.join(HOME, 'file_list.json')
        return self.config_values['storage_cache_file']

    def get_max_access_times(self):
        if 'max_access_times' not in self.config_values:
            self.config_values['max_access_times'] = 250
        return self.config_values['max_access_times']

    def get_access_reset_time(self):
        if 'access_reset_time' not in self.config_values:
            self.config_values['access_reset_time'] = 1  # seconds
        return self.config_values['access_reset_time']

    def get_page_size(self):
        if 'page_size' not in self.config_values:
            self.config_values['page_size'] = 100  # 100 elements per page
        return self.config_values['page_size']

    def get_save_interval(self):
        if 'save_interval' not in self.config_values:
            self.config_values['save_interval'] = 180  # 3 minutes
        return self.config_values['save_interval']

    def get_garbage_collector_timer_interval(self):
        if 'garbage_collector_time_interval' not in self.config_values:
            self.config_values['garbage_collector_time_interval'] = 600  # 10 minutes
        return self.config_values['garbage_collector_time_interval']

global_config = GlobalConfig()
