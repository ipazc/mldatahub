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

from time import sleep
from pyfolder import PyFolder

__author__ = 'Iván de Paz Centeno'

import unittest
from io import StringIO
from unittest.mock import patch
from mldatahub.log.logger import Logger, verbose_levels


class TestLogger(unittest.TestCase):

    def test_logger_generates_output_for_verbosity_two(self):
        """
        Logger generates the desired output for each verbosity level
        """

        for verbosity_level in range(0, verbose_levels.DEBUG+1):
            logger = Logger(show_timestamp=False, verbosity_level=verbosity_level)
            methods = [("ERROR", logger.error), ("INFO", logger.info), ("WARNING", logger.warning), ("DEBUG", logger.debug)]

            for index, (level, method) in enumerate(methods):
                with patch('sys.stdout', new=StringIO()) as fakeOutput:
                    method("hello")
                    output_value = fakeOutput.getvalue().strip()
                    expected = '[{}] [MAIN] hello'.format(level) if index+1 <= verbosity_level else ''
                    self.assertEqual(output_value, expected)


class TestFileLogger(unittest.TestCase):

    def setUp(self):
        self.pf = PyFolder(".", allow_override=True)

    def test_file_logger_generates_output_file(self):
        """
        Logger successfully generates log files.
        :return:
        """
        logger = Logger(show_timestamp=False, verbosity_level=verbose_levels.DEBUG, log_file="file.log")
        logger.info("HELLO1")
        logger.debug("HELLO2")
        logger.error("HELLO3")
        logger.warning("HELLO4")

        sleep(15)

        self.assertEqual(self.pf['file.log'].decode(), "\r[INFO] [MAIN] HELLO1\n\r[DEBUG] [MAIN] HELLO2\n\r[ERROR] [MAIN] HELLO3\n\r[WARNING] [MAIN] HELLO4\n")

    def test_file_logger_generates_output_file_finish_wait(self):
        """
        Logger successfully generates log files and wait for finish.
        :return:
        """
        logger = Logger(show_timestamp=False, verbosity_level=verbose_levels.DEBUG, log_file="file.log")
        logger.info("HELLO1")
        logger.debug("HELLO2")
        logger.error("HELLO3")
        logger.warning("HELLO4")

        logger.file_logger.finish(True)

        self.assertEqual(self.pf['file.log'].decode(), "\r[INFO] [MAIN] HELLO1\n\r[DEBUG] [MAIN] HELLO2\n\r[ERROR] [MAIN] HELLO3\n\r[WARNING] [MAIN] HELLO4\n")

    def tearDown(self):
        del self.pf['file.log']


if __name__ == '__main__':
    unittest.main()
