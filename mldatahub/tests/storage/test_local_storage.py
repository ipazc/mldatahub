#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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
from mldatahub.odm.file_dao import FileDAO

__author__ = 'Iván de Paz Centeno'


import unittest
import os
import shutil
from mldatahub.storage.local.local_storage import LocalStorage


class TestLocalStorage(unittest.TestCase):

    def setUp(self):
        self.temp_path = "examples/tmp_folder"

    def test_storage_creates_folder(self):
        """
        Tests whether the storage is able to create a folder in the specified directory.
        """
        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

        storage = LocalStorage(self.temp_path)

        self.assertTrue(os.path.exists(self.temp_path))

    def test_storage_creates_read_file(self):
        """
        Tests whether the storage is able to create a file and read it after.
        """

        storage = LocalStorage(self.temp_path)
        file_id = storage.put_file_content(b"content")

        self.assertTrue(os.path.exists(os.path.join(self.temp_path, str(file_id))))
        self.assertGreater(len(file_id), 0)

        content = storage.get_file_content(file_id)

        self.assertEqual(content, b"content")

    def test_storage_get_invalid_id(self):
        """
        Tests that the storage raises exception on invalid id.
        """
        storage = LocalStorage(self.temp_path)

        with self.assertRaises(Exception) as ex:
            storage.get_file_content("aaaa")
            self.assertEqual(
                "File ID is not valid for local storage, must be an integer.",
                str(ex.exception)
            )

        with self.assertRaises(Exception) as ex:
            storage = LocalStorage(self.temp_path, "aaa")
            self.assertEqual(
                "File ID is not valid for local storage, must be an integer.",
                str(ex.exception)
            )

        with self.assertRaises(Exception) as ex:
            storage.put_file_content(b"content", "aaa")
            self.assertEqual(
                "File ID is not valid for local storage, must be an integer.",
                str(ex.exception)
            )

    def test_storage_list_files(self):
        """
        Tests that the storage successfully stores the files refs list rather than iterating over filesystem.
        """
        storage = LocalStorage(self.temp_path)
        storage.put_file_content(b"asd", "none")

        self.assertEqual(len(storage), 1)
        for file in storage:
            self.assertEqual(file, "none")

        storage.delete_file_content("none")
        self.assertEqual(len(storage), 0)

    def tearDown(self):
        shutil.rmtree(self.temp_path)
        FileDAO.query.remove()

if __name__ == '__main__':
    unittest.main()
