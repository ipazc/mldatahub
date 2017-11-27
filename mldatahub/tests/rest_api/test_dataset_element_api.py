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
    global_config.set_page_size(100)
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


class TestDatasetElementAPI(unittest.TestCase):

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
    def test_dataset_elements_add(self):
        """
        Dataset elements can be added through API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(len(dataset), 0)

        e1 = dataset.add_element(title="element1", content=b"content1", description="No description1", tags=[{"tag": "hi"}])
        e2 = dataset.add_element(title="element2", content=b"content2", description="No description2", tags=[{"tag": "ho"}])
        e3 = dataset.add_element(title="element3", content=b"content3", description="No description3", tags=[{"tag": "hi"}])

        dataset.sync(False)

        self.assertEqual(len(dataset), 3)

        e1 = dataset[0]  # Type: Element
        e2 = dataset[1]  # Type: Element
        e3 = dataset[2]  # Type: Element

        self.assertEqual(e1.get_content(), b"content1")
        self.assertEqual(e2.get_content(), b"content2")
        self.assertEqual(e3.get_content(), b"content3")

        self.assertEqual(e1.get_title(), "element1")
        self.assertEqual(e2.get_title(), "element2")
        self.assertEqual(e3.get_title(), "element3")

        self.assertEqual(e1.get_description(), "No description1")
        self.assertEqual(e2.get_description(), "No description2")
        self.assertEqual(e3.get_description(), "No description3")

        self.assertEqual(e1.get_tags(), [{"tag": "hi"}])
        self.assertEqual(e2.get_tags(), [{"tag": "ho"}])
        self.assertEqual(e3.get_tags(), [{"tag": "hi"}])

    @wait_http_deployment
    def test_dataset_elements_iterate(self):
        """
        Dataset elements can be iterated through API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(len(dataset), 0)

        e1 = dataset.add_element(title="element1", content=b"content1", description="No description1", tags=[{"tag": "hi"}])
        e2 = dataset.add_element(title="element2", content=b"content2", description="No description2", tags=[{"tag": "ho"}])
        e3 = dataset.add_element(title="element3", content=b"content3", description="No description3", tags=[{"tag": "hi"}])

        dataset.sync(False)

        self.assertEqual(len(dataset), 3)
        order = [e1, e2, e3]

        for element, e in zip(dataset, order):
            self.assertEqual(e.get_title(), element.get_title())
            self.assertEqual(e.get_description(), element.get_description())
            self.assertEqual(e.get_tags(), element.get_tags())

        for element, e in zip(dataset.filter_iter(cache_content=True), order):
            self.assertEqual(e.get_title(), element.get_title())
            self.assertEqual(e.get_description(), element.get_description())
            self.assertEqual(e.get_content(), element.get_content())
            self.assertEqual(e.get_tags(), element.get_tags())

        for element in dataset.filter_iter(options={'tags': {'tag': 'hi'}}):
            self.assertEqual(element.get_tags(), [{'tag': 'hi'}])

    @wait_http_deployment
    def test_elements_indexes(self):
        """
        Dataset elements can be indexed in several ways through API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(len(dataset), 0)

        e1 = dataset.add_element(title="element1", content=b"content1", description="No description1", tags=[{"tag": "hi"}])
        e2 = dataset.add_element(title="element2", content=b"content2", description="No description2", tags=[{"tag": "ho"}])
        e3 = dataset.add_element(title="element3", content=b"content3", description="No description3", tags=[{"tag": "hi"}])

        dataset.sync(False)

        self.assertEqual(len(dataset), 3)

        self.assertEqual(dataset[0].get_title(), e1.get_title())
        self.assertEqual(dataset[-1].get_title(), e3.get_title())
        self.assertEqual(dataset[e2.get_id()].get_title(), e2.get_title())
        self.assertEqual(dataset[{'tags': {'tag': 'hi'}}].get_title(), e1.get_title())
        self.assertEqual(dataset[{'tags': {'tag': 'ho'}}].get_title(), e2.get_title())

    @wait_http_deployment
    def test_elements_removal(self):
        """
        Dataset elements can be removed through API
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(len(dataset), 0)

        e1 = dataset.add_element(title="element1", content=b"content1", description="No description1", tags=[{"tag": "hi"}])
        e2 = dataset.add_element(title="element2", content=b"content2", description="No description2", tags=[{"tag": "ho"}])
        e3 = dataset.add_element(title="element3", content=b"content3", description="No description3", tags=[{"tag": "hi"}])

        dataset.sync(False)

        self.assertEqual(len(dataset), 3)

        del dataset[e1.get_id()]
        self.assertEqual(len(dataset), 2)
        with self.assertRaises(KeyError):
            value = dataset[e1.get_id()]

        del dataset[0]
        self.assertEqual(len(dataset), 1)
        del dataset[{'tags':{"tag": "hi"}}]
        self.assertEqual(len(dataset), 0)

        e1 = dataset.add_element(title="element1", content=b"content1", description="No description1", tags=[{"tag": "hi"}])
        e2 = dataset.add_element(title="element2", content=b"content2", description="No description2", tags=[{"tag": "ho"}])
        e3 = dataset.add_element(title="element3", content=b"content3", description="No description3", tags=[{"tag": "hi"}])

        dataset.sync(False)

        self.assertEqual(len(dataset), 3)

        dataset.clear()
        self.assertEqual(len(dataset), 0)

    @wait_http_deployment
    def test_dataset_fork(self):
        """
        Dataset can be forked, with elements, through API.
        :return:
        """
        datasets = Datasets(token_id=self.token_guid, api_url="http://localhost:17114")
        dataset = datasets.add_dataset("dataset1")

        self.assertEqual(len(dataset), 0)

        e1 = dataset.add_element(title="element1", content=b"content1", description="No description1", tags=[{"tag": "hi"}])
        e2 = dataset.add_element(title="element2", content=b"content2", description="No description2", tags=[{"tag": "ho"}])
        e3 = dataset.add_element(title="element3", content=b"content3", description="No description3", tags=[{"tag": "hi"}])

        dataset.sync(False)

        self.assertEqual(len(dataset), 3)
        self.assertEqual(int(dataset.get_fork_count()), 0)

        fork = dataset.fork("dataset2")  # type: Dataset
        dataset.refresh()
        self.assertEqual(len(fork), 3)
        self.assertNotEqual(fork.get_url_prefix(), dataset.get_url_prefix())
        self.assertEqual(fork.get_fork_father(), dataset.get_url_prefix())
        self.assertEqual(int(dataset.get_fork_count()), 1)

        self.assertEqual(dataset[0].get_title(), fork[0].get_title())
        self.assertEqual(dataset[1].get_title(), fork[1].get_title())
        self.assertEqual(dataset[2].get_title(), fork[2].get_title())

        self.assertEqual(dataset[0].get_id(), fork[0].get_id())

        element = fork[0]
        element2 = fork[1]
        self.assertEqual(fork[0].get_content(), dataset[0].get_content())
        self.assertEqual(element.get_content(), dataset[0].get_content())
        element.set_title("hi")
        element2.set_title("hi2")
        self.assertEqual(element.get_title(), "hi")
        self.assertEqual(fork[0].get_title(), "hi")
        self.assertEqual(fork[0].get_content(), dataset[0].get_content())
        self.assertEqual(element.get_content(), dataset[0].get_content())
        self.assertNotEqual(fork[0].get_id(), dataset[0].get_id())
        self.assertNotEqual(dataset[0].get_title(), fork[0].get_title())

    def tearDown(self):
        purge_database()

    @classmethod
    def tearDownClass(cls):
        server.terminate()
        server.join()
        storage.delete()

if __name__ == '__main__':
    unittest.main()
