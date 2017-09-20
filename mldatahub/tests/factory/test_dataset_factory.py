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

    def test_dataset_modification(self):
        """
        Factory can modify datasets
        :return:
        """
        dataset = DatasetDAO("foo/hello", "notitle", "desc", "ref")
        dataset2 = DatasetDAO("bar/hello", "notitle", "desc", "ref")

        token_unprivileged = TokenDAO("unprivileged", 0, 0, "bar")
        token_privileged = TokenDAO("privileged", 0, 0, "bar", privileges=Privileges.EDIT_DATASET)
        token_admin = TokenDAO("admin", 0, 0, "bar", privileges=Privileges.ADMIN_EDIT_TOKEN)

        self.session.flush()

        # Unprivileged token cannot modify dataset
        with self.assertRaises(Unauthorized) as ex:
            DatasetFactory(token_unprivileged).edit_dataset(dataset.url_prefix, title="hello")

        # Privileged token cannot modify dataset if not in same url prefix
        with self.assertRaises(Unauthorized) as ex:
            DatasetFactory(token_privileged).edit_dataset(dataset.url_prefix, title="hello")

        # Privileged token can modify dataset if in same url prefix
        # NOT: Because dataset is not linked with the token.
        with self.assertRaises(Unauthorized) as ex:
            dataset2 = DatasetFactory(token_privileged).edit_dataset(dataset2.url_prefix, title="hello")

        # If we link it, then it does:
        token_privileged = token_privileged.link_dataset(dataset2)
        dataset2 = DatasetFactory(token_privileged).edit_dataset(dataset2.url_prefix, title="hello")

        self.assertEqual(dataset2.title, "hello")

        # Admin token can modify any dataset
        dataset = DatasetFactory(token_admin).edit_dataset(dataset.url_prefix, title="hello2")
        self.assertEqual(dataset.title, "hello2")

        # Privileged can partially modify url prefix
        dataset2 = DatasetFactory(token_privileged).edit_dataset(dataset2.url_prefix, url_prefix="new_prefix")
        self.assertEqual(dataset2.url_prefix, "bar/new_prefix")

        with self.assertRaises(BadRequest) as ex:
            dataset2 = DatasetFactory(token_privileged).edit_dataset(dataset2.url_prefix, url_prefix="bar2/new_prefix")

        # Admin can modify url prefix without problems
        dataset2 = DatasetFactory(token_admin).edit_dataset(dataset2.url_prefix, url_prefix="bar2/new_prefix")

        self.assertEqual(dataset2.url_prefix, "bar2/new_prefix")

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

    def test_dataset_retrieval(self):
        """
        Factory can retrieve datasets.
        :return:
        """
        anonymous = TokenDAO("Anonymous", 1, 1, "anonymous")
        watcher = TokenDAO("normal user", 1, 1, "user1", privileges=0)
        creator = TokenDAO("normal user privileged", 1, 1, "user1", privileges=Privileges.CREATE_DATASET)
        creator2 = TokenDAO("normal user privileged", 1, 1, "user2", privileges=Privileges.CREATE_DATASET)
        admin = TokenDAO("admin user", 1, 1, "admin", privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetFactory(creator).create_dataset(url_prefix="creator", title="Creator dataset", description="Dataset example creator", reference="Unknown")
        dataset2 = DatasetFactory(creator2).create_dataset(url_prefix="creator", title="Creator dataset", description="Dataset example creator", reference="Unknown")
        self.session.flush()

        # anonymous should not be able to get info from the dataset
        with self.assertRaises(Unauthorized) as ex:
            dataset3 =DatasetFactory(anonymous).get_dataset(dataset.url_prefix)

        # watcher should not be able to get info from the dataset
        with self.assertRaises(Unauthorized) as ex:
            dataset3 =DatasetFactory(watcher).get_dataset(dataset.url_prefix)

        # creator should not be able to get info from the dataset
        with self.assertRaises(Unauthorized) as ex:
            dataset3 =DatasetFactory(creator).get_dataset(dataset.url_prefix)

        # admin should be able to get info from the dataset
        dataset3 =DatasetFactory(admin).get_dataset(dataset.url_prefix)
        self.assertEqual(dataset3.url_prefix, dataset.url_prefix)

        anonymous = anonymous.link_dataset(dataset)

        # anonymous should now be able to get info from the dataset
        dataset3 =DatasetFactory(anonymous).get_dataset(dataset.url_prefix)
        self.assertEqual(dataset3.url_prefix, dataset.url_prefix)

        # The privilege RO_WATCH_DATASET is always required except for admin

        watcher = watcher.link_dataset(dataset)
        with self.assertRaises(Unauthorized) as ex:
            dataset3 =DatasetFactory(watcher).get_dataset(dataset.url_prefix)


    def tearDown(self):
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        TokenDAO.query.remove()
        taken_url_prefixes.clear()

if __name__ == '__main__':
    unittest.main()