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

import os
from functools import partial
from ming import create_datastore
from ming.odm import ThreadLocalODMSession
from pyfolder import PyFolder
from mldatahub.log.logger import Logger

__author__ = 'Iván de Paz Centeno'


logger = Logger("Config")

e = logger.error
w = logger.warning
d = logger.debug
i = logger.info

HOME = os.path.expanduser("~")
DEFAULT_CONFIG_ROUTE = "/etc/mldatahub/"


class GlobalConfig(object):
    """
    Represents a configuration object, as a Singleton for several options.
    """
    # Special keys that cannot be set in the config
    __forbidden_keys = {"log_file", "session", "storage"}

    # Current config
    __config = {}
    __storage = None
    __session = None

    def __init__(self):
        """
        Initializer of the class.
        """
        # Default configuration

        config_routes = [".", DEFAULT_CONFIG_ROUTE]
        pyfolder = None
        example_config_route = []

        for config_route in config_routes:
            try:
                pyfolder = PyFolder(config_route)
                example_config_route = pyfolder.index("config_example.json", 3)

            except PermissionError:
                w("Not permission to access the folder {}".format(config_route))
                pass

            if len(example_config_route) > 0:
                break

        if len(example_config_route) == 0 or pyfolder is None:
            e("Example config (config_example.json) could not be found. Aborting execution.")
            exit(-1)

        # We take the default config from the example file itself.
        self.__default_config = pyfolder[example_config_route[0]]
        self.__default_config["session"] = self.__get_session__
        self.__default_config["storage"] = self.__get_storage__
        self.__default_config["log_file"] = self.__get_log_file

        self.load_from_file()

    def __get_log_file(self):
        """
        :return: Log file used to store the logs.
        """
        return self.get_log_file_uri().replace("$HOME", HOME)

    def __get_session__(self):
        """
        :return: ODM Session used by ODM classes.
        """
        if self.__session is None:
            self.__session = ThreadLocalODMSession(bind=create_datastore(self.get_session_uri()))
        return self.__session

    def __get_storage__(self):
        """
        :return: storage used to save/retrieve files' contents.
        """
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
        """
        Proxy for getters and setters.
        :param item: get_[configuration_option] or set_[configuration_option]. Example:

            get_host
            set_port

        it maps the item to the contents of the config dict, but also adds some functionality on certain getters.
        :return:
        """
        if item.startswith("set"):
            result = partial(self.__put_config__, item.replace("set_", ""))
        elif item.startswith("get"):
            result = partial(self.__read_config__, item.replace("get_", ""))
        else:
            raise Exception("'{}' not valid. Only getters and setters available. Try with get_[config_option] or set_[config_option](value)".format(item))

        return result

    def load_from_file(self):
        """
        Loads the config from the config.json file.
        Usually, this file is located at /etc/mldatahub/
        """
        try:
            pyfolder = PyFolder(DEFAULT_CONFIG_ROUTE)
            self.config_values = pyfolder['config.json']
        except PermissionError as ex:
            w("There is no permission to access the config file. Running on default values.")
        except KeyError as ex:
            w("Config file not found. Running on default values.")

    def __str__(self):
        """
        Makes a string representation of the config. Each key is printed with its corresponding value.
        """
        result = ""
        for k, v in self.config_values.items():
            if not k.startswith("#"):
                result += "{}: {}\n".format(k, v)

        return result

    def print_config(self):
        """
        Prints this config to the stdout.
        """
        print(self)

# Singleton object.
global_config = GlobalConfig()
