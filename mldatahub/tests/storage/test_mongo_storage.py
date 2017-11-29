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

__author__ = 'Iván de Paz Centeno'

import unittest
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.file_dao import FileDAO, FileContentDAO
from mldatahub.storage.remote.mongo_storage import MongoStorage
from bson import ObjectId


class TestMongoStorage(unittest.TestCase):

    def test_storage_creates_read_file(self):
        """
        Tests whether the storage is able to create a file and read it after.
        """

        storage = MongoStorage()
        file_id = storage.put_file_content(b"content")

        self.assertIsInstance(file_id, ObjectId)

        content = storage.get_file(file_id).content

        self.assertEqual(content, b"content")

    def test_storage_list_files(self):
        """
        Tests that the storage successfully stores the files refs list.
        """
        storage = MongoStorage()
        storage.put_file_content(b"asd")

        self.assertEqual(len(storage), 1)

        for file_id in storage:
            self.assertIsInstance(file_id, FileDAO)

    def test_storage_removes_files(self):
        """
        Tests that the storage is able to remove files.
        :return:
        """
        storage = MongoStorage()

        content = b"hi"
        file = FileContentDAO(content=content, size=len(content))
        global_config.get_session().flush()
        self.assertEqual(len(storage), 1)
        storage.delete_file(file._id)
        self.assertEqual(len(storage), 0)

    def test_storage_multiple_files(self):
        """
        Storage can store and retrieve multiple files at once.
        :return:
        """

        contents = ["content{}".format(i).encode() for i in range(1000)]

        storage = MongoStorage()
        files_ids = storage.put_files_contents(contents)

        self.assertTrue(all([type(f) is ObjectId for f in files_ids]))

        # The order of the stored IDs must be the same as the order of the input
        contents2 = [f.content for f in storage.get_files(files_ids)]

        self.assertTrue(all([content == content2 for content, content2 in zip(contents, contents2)]))

    def test_storage_delete_files(self):
        """
        Storage can delete multiple files at once.
        :return:
        """
        contents = [b"content1", b"content2", b"content3"]

        storage = MongoStorage()
        files_ids = storage.put_files_contents(contents)

        self.assertEqual(len(storage), 3)
        storage.delete_files(files_ids[:-1])
        self.assertEqual(len(storage), 1)

    def test_size(self):
        """
        Storage can calculate its size.
        :return:
        """
        contents = [b"content1", b"content2", b"content3"]

        storage = MongoStorage()
        files_ids = storage.put_files_contents(contents)

        self.assertEqual(storage.size(), sum([len(c) for c in contents]))
        self.assertEqual(storage.get_files_size(files_ids[:-1]), sum([len(c) for c in contents[:-1]]))

    def test_contains(self):
        """
        Storage can check if a file is inside.
        :return:
        """
        contents = [b"content1", b"content2", b"content3"]

        storage = MongoStorage()
        files_ids = storage.put_files_contents(contents)

        self.assertEqual(storage.size(), sum([len(c) for c in contents]))

    def test_storage_do_hash(self):
        """
        Storage is hashing content to optimize space.
        :return:
        """
        content1 = b"content1"
        content2 = b"content2"

        storage = MongoStorage()
        file_id = storage.put_file_content(content1)
        file_id2 = storage.put_file_content(content1)
        file_id3 = storage.put_file_content(content2)

        self.assertEqual(file_id, file_id2)
        self.assertNotEqual(file_id, file_id3)

    def test_storage_force_ids(self):
        """
        Storage can be forced to set custom ids to files.
        :return:
        """
        content1 = b"content1"
        content2 = b"content2"
        content3 = b"content3"
        content4 = b"content4"

        storage = MongoStorage()
        file_id = storage.put_file_content(content1, force_id=ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))

        self.assertEqual(file_id, ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))

        file_id = storage.put_file_content(content1, force_id=ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1"))

        self.assertEqual(file_id, ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))

        file_id = storage.put_file_content(content2, force_id=ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))

        self.assertEqual(file_id, ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))

        file_id = storage.put_file_content(content1)

        self.assertNotEqual(file_id, ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))
        self.assertNotEqual(file_id, ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1"))

        storage.delete_file(file_id)
        storage.delete_file(ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"))

        file_ids = storage.put_files_contents([content1, content2], force_ids=[ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        self.assertEqual(file_ids, [ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        file_ids = storage.put_files_contents([content1, content3], force_ids=[ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        self.assertEqual(file_ids, [ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        file_ids = storage.put_files_contents([content1, content3])

        self.assertEqual(file_ids, [ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        file_ids = storage.put_files_contents([content2])
        self.assertNotIn(file_ids[0], [ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        storage.delete_files(file_ids)
        storage.delete_files([ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1")])

        file_ids = storage.put_files_contents([content3, content2, content1], force_ids=[ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb2")])
        self.assertEqual(file_ids, [ObjectId("bbbbbbbbbbbbbbbbbbbbbbbb"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb1"), ObjectId("bbbbbbbbbbbbbbbbbbbbbbb2")])

    def tearDown(self):
        FileDAO.query.remove()

if __name__ == '__main__':
    unittest.main()
