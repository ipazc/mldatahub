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
        self.storage = None

        self.load_from_file()

    def load_from_file(self):
        try:
            with open("/etc/mldatahub/config.json") as f:
                self.config_values = json.load(f)

        except FileNotFoundError as ex:
            print("Config file not found. Running on default values.")

    def print_config(self):
        for k, v in self.config_values.items():
            if not k.startswith("#"):
                print("{}: {}".format(k, v))

    def set_host(self, new_host):
        self.config_values['host'] = new_host

    def set_port(self, new_port):
        self.config_values['port'] = new_port

    def set_session_uri(self, new_uri):
        self.config_values['session_uri'] = new_uri

    def set_page_size(self, new_page_size):
        self.config_values['page_size'] = new_page_size

    def set_garbage_collector_timer_interval(self, new_interval):
        self.config_values['garbage_collector_time_interval'] = new_interval

    def set_max_access_times(self, new_max_access_times):
        self.config_values['new_max_access_times'] = new_max_access_times

    def set_access_reset_time(self, new_access_reset_time):
        self.config_values['access_reset_time'] = new_access_reset_time

    def set_file_size_limit(self, new_file_size_limit):
        self.config_values['file_size_limit'] = new_file_size_limit

    def get_host(self):
        if 'host' not in self.config_values:
            self.set_host("localhost")

        return self.config_values['host']

    def get_port(self):
        if 'port' not in self.config_values:
            self.set_port("5555")

        return int(self.config_values['port'])

    def get_session(self):
        if 'session_uri' not in self.config_values:
            self.set_session_uri("mongodb://localhost:27017/mldatahub")

        if self.session is None:
            self.session = ThreadLocalODMSession(bind=create_datastore(self.config_values['session_uri']))
        return self.session

    def get_storage(self):
        if self.storage is None:
            from mldatahub.storage.remote.mongo_storage import MongoStorage
            self.storage = MongoStorage()

        return self.storage

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

    def get_garbage_collector_timer_interval(self):
        if 'garbage_collector_time_interval' not in self.config_values:
            self.config_values['garbage_collector_time_interval'] = 600  # 10 minutes
        return self.config_values['garbage_collector_time_interval']

    def get_file_size_limit(self):
        if 'file_size_limit' not in self.config_values:
            self.config_values['file_size_limit'] = 16*1024*1024  # 16 MB
        return self.config_values['file_size_limit']

global_config = GlobalConfig()
