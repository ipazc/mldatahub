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

from functools import wraps
from time import sleep
import requests
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
global_config.set_page_size(2)
from mldatahub.entry_point import build_app, purge_database, __create_token__
import unittest
from multiprocessing import Process
from dhub.datasets import Datasets
from mldatahub.config.privileges import Privileges


__author__ = 'Iván de Paz Centeno'

def run_server():
    build_app().run("localhost", 17114)

storage = global_config.get_storage()
server = Process(target=run_server)


def wait_http_deployment(f):
    """
    Decorator function to wait for HTTP to be ready before calling the function body.
    :param f:
    :return:
    """

    @wraps(f)
    def test__wait_for_http_ready(params):
        status_code = None
        tries = 0
        while status_code is None and tries < 15:
            try:
                response = requests.get("http://localhost:17114")
                status_code = response.status_code
            except Exception:
                sleep(1)
                tries += 1

        if tries == 15:
            raise Exception("Server can't be contacted.")

    return test__wait_for_http_ready


class TestDatasetAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        server.start()

    def setUp(self):
        self.session = global_config.get_session()
        purge_database()
        privileges = Privileges.CREATE_DATASET + Privileges.EDIT_DATASET + Privileges.DESTROY_DATASET + \
                 Privileges.ADD_ELEMENTS + Privileges.EDIT_ELEMENTS + Privileges.DESTROY_ELEMENTS + \
                 Privileges.RO_WATCH_DATASET + Privileges.USER_EDIT_TOKEN

        self.token_guid = __create_token__("example", "", 100, 100000, privileges, 1000).token_gui

    @wait_http_deployment
    def test_datasets_retrieval(self):
        """
        Datasets can be retrieved from API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        self.assertEqual(str(datasets), "[]")
        self.assertEqual(len(datasets), 0)

    @wait_http_deployment
    def test_dataset_creation(self):
        """
        Dataset can be created from API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(str(datasets), "['example/dataset1']")
        self.assertEqual(len(datasets), 1)
        self.assertEqual(dataset.get_url_prefix(), "example/dataset1")
        self.assertEqual(len(dataset), 0)

        dataset2 = datasets.add_dataset("dataset2", title="This is dataset2", description="Now with description",
                                        reference="No reference", tags=["one", "two"])

        self.assertEqual(dataset2.get_url_prefix(), "example/dataset2")
        self.assertEqual(dataset2.get_title(), "This is dataset2")
        self.assertEqual(dataset2.get_description(), "Now with description")
        self.assertEqual(dataset2.get_tags(), ["one", "two"])

        dataset2.set_tags(["three", "four"])
        self.assertEqual(dataset2.get_tags(), ["three", "four"])

        dataset2 = datasets['dataset2']
        dataset2_1 = datasets['example/dataset2']

        self.assertEqual(dataset2, dataset2_1)

        dataset2.set_title("asd123")
        dataset2.set_description("testing_description")

        self.assertEqual(dataset2.get_title(), "asd123")
        self.assertEqual(dataset2.get_description(), "testing_description")
        dataset2.update()
        datasets2 = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset2 = datasets2['example/dataset2']

        self.assertEqual(dataset2.get_title(), "asd123")
        self.assertEqual(dataset2.get_description(), "testing_description")

    @wait_http_deployment
    def test_dataset_removal(self):
        """
        Dataset can be removed from API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(str(datasets), "['example/dataset1']")
        self.assertEqual(len(datasets), 1)
        del dataset
        self.assertEqual(str(datasets), "[]")
        self.assertEqual(len(datasets), 0)

    def tearDown(self):
        purge_database()

    @classmethod
    def tearDownClass(cls):
        server.terminate()
        server.join()
        storage.delete()

if __name__ == '__main__':
    unittest.main()
