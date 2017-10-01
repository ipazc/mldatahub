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

__author__ = 'Iván de Paz Centeno'

import unittest
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO


class TestTokenODM(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()

    def test_token_can_be_created_and_destroyed(self):
        """
        Tests that tokens are successfully created and removed.
        :return:
        """
        token = TokenDAO("example", 2, 3, "dalap")
        token_gui = token.token_gui
        self.assertTrue(len(token.token_gui) > 10)
        self.session.flush()
        token = TokenDAO.query.get(token_gui=token_gui)
        self.assertEqual(token.token_gui, token_gui)
        token.delete()
        token = TokenDAO.query.get(token_gui)
        self.assertIsNone(token)

    def test_token_can_link_datasets(self):
        """
        Tests that tokens can be associated to multiple datasets and disassociated.
        :return:
        """
        token1 = TokenDAO("example_token", 2, 5, "dalap")
        token2 = TokenDAO("example_token2", 2, 5, "dalap")

        dataset1 = DatasetDAO("ex/ivan", "example1", "lalala", "none")
        dataset2 = DatasetDAO("ex/ivan2", "example2", "lalala", "none")

        token1 = token1.link_datasets([dataset1, dataset2])
        token2 = token2.link_dataset(dataset2)

        self.assertEqual(len(token1.datasets), 2)
        self.assertEqual(len(token2.datasets), 1)

        token1 = token1.unlink_dataset(dataset2)
        self.assertEqual(len(token1.datasets), 1)

    def tearDown(self):
        DatasetDAO.query.remove()
        TokenDAO.query.remove()

if __name__ == '__main__':
    unittest.main()
