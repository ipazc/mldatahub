#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from werkzeug.exceptions import Unauthorized, BadRequest
from mldatahub.config.privileges import Privileges
import unittest
from mldatahub.config.config import global_config
from mldatahub.factory.dataset_factory import DatasetFactory

global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetCommentDAO, DatasetElementDAO, DatasetElementCommentDAO, \
    taken_url_prefixes
from mldatahub.odm.token_dao import TokenDAO


__author__ = 'Iván de Paz Centeno'


def join_prefixes(prefix1, prefix2):
    if prefix1.endswith("/"):
        prefix1 = prefix1[:-1]

    if prefix2.startswith("/"):
        prefix2 = prefix2[1:]

    return "{}/{}".format(prefix1, prefix2)


class TestDatasetFactory(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        TokenDAO.query.remove()
        taken_url_prefixes.clear()

    def test_dataset_creation(self):
        """
        Factory can create datasets
        :return:
        """
        anonymous = TokenDAO("Anonymous", 1, 1, "anonymous")
        watcher = TokenDAO("normal user", 1, 1, "user1")
        creator = TokenDAO("normal user privileged", 1, 1, "user1", privileges=Privileges.CREATE_DATASET)
        admin = TokenDAO("admin user", 1, 1, "admin", privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        # Anonymous or watcher can't create datasets
        with self.assertRaises(Unauthorized) as ex:
            dataset = DatasetFactory(anonymous).create_dataset(url_prefix="anonymous", title="Anonymous dataset", description="Dataset example anonymous", reference="Unknown")
        with self.assertRaises(Unauthorized) as ex:
            dataset = DatasetFactory(watcher).create_dataset(url_prefix="watcher", title="Watcher dataset", description="Dataset example watcher", reference="Unknown")

        # Creator can create a dataset
        dataset = DatasetFactory(creator).create_dataset(url_prefix="creator", title="Creator dataset", description="Dataset example creator", reference="Unknown")
        self.assertEqual(join_prefixes(creator.url_prefix, "creator"), dataset.url_prefix)
        self.assertEqual(dataset.description, "Dataset example creator")

        # Not all prefixes allowed (for example, "/" char is protected)
        illegal_chars = "/*;:,.ç´`+Ç¨^><¿?'¡¿!\"·$%&/()@~¬"

        for illegal_char in illegal_chars:
            with self.assertRaises(BadRequest) as ex:
                dataset = DatasetFactory(creator).create_dataset(url_prefix="creator{}da".format(illegal_char), title="Creator dataset", description="Dataset example creator", reference="Unknown")

        # Admin can create dataset
        dataset = DatasetFactory(admin).create_dataset(url_prefix="admin", title="Admin dataset", description="Dataset example admin", reference="Unknown")
        self.assertEqual(join_prefixes(admin.url_prefix, "admin"), dataset.url_prefix)
        self.assertEqual(dataset.description, "Dataset example admin")

        # Admin can create dataset on other's url prefixes
        dataset = DatasetFactory(admin).create_dataset(url_prefix="user1/admin", title="Admin dataset", description="Dataset example admin", reference="Unknown")
        self.assertEqual(join_prefixes(creator.url_prefix, "admin"), dataset.url_prefix)
        self.assertEqual(dataset.description, "Dataset example admin")

    def test_dataset_destruction(self):
        """
        Factory can destroy datasets
        :return:
        """
        anonymous = TokenDAO("Anonymous", 1, 1, "anonymous")
        watcher = TokenDAO("normal user", 1, 1, "user1")
        creator = TokenDAO("normal user privileged", 1, 1, "user1", privileges=Privileges.CREATE_DATASET)
        creator2 = TokenDAO("normal user privileged", 1, 1, "user2", privileges=Privileges.CREATE_DATASET)
        destructor = TokenDAO("normal user privileged", 1, 1, "user1", privileges=Privileges.DESTROY_DATASET)
        admin = TokenDAO("admin user", 1, 1, "admin", privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetFactory(creator).create_dataset(url_prefix="creator", title="Creator dataset", description="Dataset example creator", reference="Unknown")
        dataset2 = DatasetFactory(creator2).create_dataset(url_prefix="creator", title="Creator dataset", description="Dataset example creator", reference="Unknown")
        self.session.flush()

        # Anonymous or watcher can't destroy datasets
        with self.assertRaises(Unauthorized) as ex:
            DatasetFactory(anonymous).destroy_dataset(url_prefix="user1/creator")
        with self.assertRaises(Unauthorized) as ex:
            DatasetFactory(watcher).destroy_dataset(url_prefix="user1/creator")

        # creator can't destroy a dataset
        with self.assertRaises(Unauthorized) as ex:
            DatasetFactory(creator).destroy_dataset(url_prefix="user1/creator")

        # destructor can't destroy other's datasets
        with self.assertRaises(Unauthorized) as ex:
            DatasetFactory(destructor).destroy_dataset(url_prefix="user2/creator")

        # destructor can destroy within his url-prefix datasets
        DatasetFactory(destructor).destroy_dataset(url_prefix="user1/creator")

        self.session.flush()
        self.session.clear()

        dataset = DatasetDAO.query.get(url_prefix="user1/creator")
        self.assertIsNone(dataset, None)

        # destructor can destroy within any url-prefix datasets
        DatasetFactory(admin).destroy_dataset(url_prefix="user2/creator")

        self.session.flush()
        self.session.clear()

        dataset = DatasetDAO.query.get(url_prefix="user2/creator")
        self.assertIsNone(dataset, None)

    def tearDown(self):
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        TokenDAO.query.remove()
        taken_url_prefixes.clear()

if __name__ == '__main__':
    unittest.main()
