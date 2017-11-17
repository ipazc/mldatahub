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

from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.file_dao import FileDAO, FileContentDAO
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetElementDAO
from mldatahub.observer.garbage_collector import GarbageCollector

__author__ = 'Iván de Paz Centeno'

import unittest


class TestGarbageCollector(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()
        DatasetDAO.query.remove()
        DatasetElementDAO.query.remove()
        FileDAO.query.remove()

    def test_gc_works_as_expected(self):
        """
        Garbage Collector effectively collects all the garbage when it gets dereferenced.
        :return:
        """
        gc = GarbageCollector()
        self.assertEqual(gc.do_garbage_collect(), 0)

        contents = ["hello{}".format(i).encode() for i in range(1000)]

        files = [FileContentDAO(content=content, size=len(content)) for content in contents]

        self.session.flush()

        self.assertEqual(FileDAO.query.find().count(), len(contents))

        # First time is always 0. It is a security check to ensure that a content is not being used.
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), len(contents))

        self.assertEqual(FileDAO.query.find().count(), 0)

        contents = ["hello{}".format(i).encode() for i in range(10)]
        files = [FileContentDAO(content=content, size=len(content)) for content in contents]

        elements = [
            DatasetElementDAO("title1", "none", files[0]._id, ),
            DatasetElementDAO("title2", "none", files[0]._id, ),
            DatasetElementDAO("title3", "none", files[1]._id, ),
            DatasetElementDAO("title4", "none", files[2]._id, )]

        self.session.flush()

        self.assertEqual(FileDAO.query.find().count(), len(contents))
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 7)
        self.assertEqual(FileDAO.query.find().count(), 3)
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 0)

        elements[2].delete()
        self.session.flush()
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 1)
        self.assertEqual(FileDAO.query.find().count(), 2)
        elements[0].delete()
        self.session.flush()
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(FileDAO.query.find().count(), 2)
        elements[1].delete()
        self.session.flush()
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 1)
        self.assertEqual(FileDAO.query.find().count(), 1)
        elements[3].delete()
        self.session.flush()
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 1)
        self.assertEqual(FileDAO.query.find().count(), 0)
        self.assertEqual(gc.do_garbage_collect(), 0)
        self.assertEqual(gc.do_garbage_collect(), 0)

    def tearDown(self):
        DatasetDAO.query.remove()
        DatasetElementDAO.query.remove()
        FileDAO.query.remove()

if __name__ == '__main__':
    unittest.main()
