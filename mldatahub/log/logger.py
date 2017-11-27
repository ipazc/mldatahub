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

from mldatahub.helper.timing_helper import now
from mldatahub.log.file_log import file_logger

__author__ = 'Iván de Paz Centeno'



class verbose_levels():
    ERROR   = 1
    INFO    = 2
    WARNING = 3
    DEBUG   = 4


def verbose_level(v_level: int):
    """
    Decorator for setting the verbosity level for a given log function.
    :param v_level: verbose level from 1 to N.
    :return:
    """
    def wrapper(func):
        def wrapper2(*args, **kwargs):
            instance = args[0]
            if 0 < v_level <= instance.verbosity_level:
                result = func(*args, **kwargs)
            else:
                result = None

            return result
        return wrapper2
    return wrapper


class Logger(object):
    """
    Logger class for MLDataHub.
    """

    def __init__(self, module_name="MAIN", show_timestamp=True, verbosity_level=verbose_levels.DEBUG, log_file=None):
        """
        Constructor of the logger class.
        :param module_name: prefix for each of the log messages.
        :param show_timestamp: flag to show the time stamp.
        :param verbosity_level: verbose level to output (Check the verbose_levels meta class).
        :param log_file: file to output the logs.
        :return:
        """
        if type(verbosity_level) is str:
            self.verbosity_level = getattr(verbose_levels, verbosity_level)
        else:
            self.verbosity_level = verbosity_level

        self.module_name = module_name
        self.show_timestamp = show_timestamp
        self.log_file = log_file
        self.file_logger = file_logger
        self.file_logger.set_file_uri(self.log_file)

    def __generate_main_message__(self, level: str="INFO"):
        """
        Generates the main message.

        Example:
            >>> __generate_main_message__("debug").format("this is the message")
            "[DEBUG][MODULE - DD/MM/YYYY HH:MM:SS:MS] this is the message"

        :param level: string representing the level of the message. Examples: "debug", "info", "warning", "error", ...
        :return: string message ready to be formatted with the message.
        """
        main_message = "\r[{level}] [{module}"

        if self.show_timestamp:
            main_message += " - {timestamp}"

        main_message += "] {message}"

        return main_message.format(level=level.upper(), module="{}", timestamp=now(), message="{}")

    @verbose_level(verbose_levels.DEBUG)
    def debug(self, string, same_line=False):
        self.__output__("DEBUG", same_line, string)

    @verbose_level(verbose_levels.WARNING)
    def warning(self, string, same_line=False):
        self.__output__("WARNING", same_line, string)

    @verbose_level(verbose_levels.INFO)
    def info(self, string, same_line=False):
        self.__output__("INFO", same_line, string)

    @verbose_level(verbose_levels.ERROR)
    def error(self, string, same_line=False):
        self.__output__("ERROR", same_line, string)

    def __output__(self, level, same_line, string):
        end = "" if same_line else "\n"
        line = self.__generate_main_message__(level).format(self.module_name, string)
        self.file_logger.queue_line(line+end, same_line)
        print(line, end=end, flush=True)
