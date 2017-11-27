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
from functools import partial
from ming import create_datastore
from ming.odm import ThreadLocalODMSession

__author__ = 'Iván de Paz Centeno'

HOME = os.path.expanduser("~")
DEFAULT_CONFIG_ROUTE = "/etc/mldatahub/config.json"


class GlobalConfig(object):
    # Special keys that cannot be set in the config """
    __forbidden_keys = {"log_file", "session", "storage"}

    # Current config
    __config = {}
    __storage = None
    __session = None

    def __init__(self):
        # Default configuration
        self.__default_config = {
            "host": "localhost",
            "port": 5555,
            "log_file_uri": "$HOME/mldatahub.log",
            "session_uri": "mongodb://localhost:27017/mldatahub",
            "max_access_times": 250,
            "access_reset_time": 1,  # seconds
            "page_size": 100,  # elements
            "garbage_collector_timer_interval": 600,  # seconds
            "file_size_limit": 16*1024*1024,   #  16 MB
            "google_drive_folder": "mldatahub/",
            "session": self.__get_session__,
            "storage": self.__get_storage__,
            "log_file": self.__get_log_file,
        }
        self.load_from_file()

    def __get_log_file(self):
        return self.get_log_file_uri().replace("$HOME", HOME)

    def __get_session__(self):
        if self.__session is None:
            self.__session = ThreadLocalODMSession(bind=create_datastore(self.get_session_uri()))
        return self.__session

    def __get_storage__(self):
        if self.__storage is None:
            from mldatahub.storage.remote.mongo_storage import MongoStorage
            self.__storage = MongoStorage()

        return self.__storage

    def __read_config__(self, key):
        """
        Reads the key from the current config.
        :param key: key to read.
        :return: config result
        """
        if key in self.__forbidden_keys:
            result = self.__default_config[key]()
        else:
            if key in self.__config:
                result = self.__config[key]
            else:
                result = self.__default_config[key]

        return result

    def __put_config__(self, key, value):
        """
        Sets a value for the current config
        :param key:
        :param value:
        :return:
        """
        self.__config[key] = value

    def __getattr__(self, item):

        if item.startswith("set"):
            result = partial(self.__put_config__, item.replace("set_", ""))
        elif item.startswith("get"):
            result = partial(self.__read_config__, item.replace("get_", ""))
        else:
            raise Exception("'{}' not valid. Only getters and setters available. Try with get_[config_option] or set_[config_option](value)".format(item))

        return result

    def load_from_file(self):
        try:
            with open(DEFAULT_CONFIG_ROUTE) as f:
                self.config_values = json.load(f)

        except FileNotFoundError as ex:
            print("Config file not found. Running on default values.")

    def print_config(self):
        for k, v in self.config_values.items():
            if not k.startswith("#"):
                print("{}: {}".format(k, v))

global_config = GlobalConfig()
