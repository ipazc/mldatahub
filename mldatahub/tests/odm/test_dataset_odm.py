#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from time import sleep

__author__ = 'Iván de Paz Centeno'

import unittest
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetCommentDAO, DatasetElementDAO, DatasetElementCommentDAO, taken_url_prefixes


class TestDatasetODM(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        taken_url_prefixes.clear()

    def test_create_remove_dataset(self):
        """
        Dataset creation and removal works successfully.
        :return:
        """
        dataset = DatasetDAO("ip/asd", "example1", "desc", "none")

        self.assertTrue(dataset.title, "example")
        self.assertTrue(dataset.description, "desc")
        self.assertTrue(dataset.reference, "none")

        self.session.flush()

        dataset2 = DatasetDAO.query.get(title="example1")

        self.assertEqual(dataset.title, dataset2.title)
        self.assertEqual(dataset.description, dataset2.description)
        self.assertEqual(dataset.reference, dataset2.reference)

        dataset.delete()
        self.session.flush()

        dataset3 = DatasetDAO.query.get(title='example1')
        self.assertIsNone(dataset3)

    def test_create_dataset_add_remove_comment(self):
        """
        Dataset creation and removal of comments works successfully.
        :return:
        """
        dataset = DatasetDAO("ip/asd2", "example2", "desc", "none")

        c1 = dataset.add_comment("ivan", "1", "11")
        c2 = dataset.add_comment("ivan", "1", "21")
        c3 = dataset.add_comment("ivan", "1", "11")

        self.session.flush()

        dataset2 = DatasetDAO.query.get(title="example2")

        self.session.refresh(dataset2)
        self.assertEqual(len(dataset2.comments), 3)
        self.assertEqual(dataset2.comments[0].author_name, "ivan")
        self.assertEqual(dataset2.comments[1].author_name, "ivan")
        self.assertEqual(dataset2.comments[2].author_name, "ivan")

        dataset.comments[0].delete()

        self.session.flush()

        dataset3 = DatasetDAO.query.get(title="example2")

        self.assertEqual(len(dataset3.comments), 2)

        comment = DatasetCommentDAO.query.get(author_name="ivan")

        self.assertEqual(comment.dataset_id, dataset._id)

        dataset.delete()
        comment = DatasetCommentDAO.query.get(author_name="ivan")
        self.assertIsNone(comment)

    def test_create_dataset_add_remove_element(self):
        """
        Dataset creation and removal of elements works successfully.
        :return:
        """
        dataset = DatasetDAO("ip/asd3", "example3", "for content", "unknown")

        dataset.add_element("ele1", "description of the element.", "randomID", tags=["tag1", "tag2"])
        dataset.add_element("ele2", "description of the element.", "randomID2", tags=["tag1"])
        self.session.flush()
        self.assertEqual(len(dataset.elements), 2)

        element = DatasetElementDAO.query.get(tags="tag2")
        self.assertEqual(element.title, "ele1")

        element.delete()
        self.session.flush()

        element = DatasetElementDAO.query.get(tags="tag2")
        self.assertIsNone(element)
        element = DatasetElementDAO.query.get(tags="tag1")
        self.assertEqual(element.title, "ele2")

        dataset.delete()

        element = DatasetElementDAO.query.get(tags="tag1")
        self.assertIsNone(element)

    def test_create_dataset_element_add_remove_comment(self):
        """
        Dataset creation and removal of comments from elements works successfully.
        :return:
        """
        dataset = DatasetDAO("ip/asd4", "example4", "desc", "none")

        element = dataset.add_element("ele1", "description of the element.", "randomID", tags=["tag1", "tag2"])
        element2 = dataset.add_element("ele2", "description of the element2.", "randomID2", tags=["tag1", "tag2"])

        element.add_comment("ivan", "1", "11")
        element.add_comment("ivan", "1", "21")
        element.add_comment("ivan", "1", "11")
        self.session.flush()

        self.assertEqual(len(element.comments), 3)
        self.assertEqual(len(element2.comments), 0)
        self.assertEqual(element.comments[0].author_name, "ivan")
        self.assertEqual(element.comments[1].author_name, "ivan")
        self.assertEqual(element.comments[2].author_name, "ivan")

        comment = element.comments[0]

        #sleep(10)
        comment.delete()
        #sleep(10)
        self.session.flush()
        self.session.clear()

        element = DatasetElementDAO.query.get(title="ele1")
        self.session.refresh(element)

        self.assertEqual(len(element.comments), 2)

        comment = DatasetElementCommentDAO.query.get(author_name="ivan")

        self.assertEqual(comment.element_id, element._id)

        element.delete()

        self.session.flush()

        comment = DatasetElementCommentDAO.query.get(author_name="ivan")
        self.assertIsNone(comment)
        dataset.delete()

    def test_url_prefix_duplication_error(self):
        """
        Tests that a duplicated url prefix cannot be retrieved.
        :return:
        """
        dataset = DatasetDAO("ip/asd5", "example5", "desc", "none")

        with self.assertRaises(Exception) as ex:
            dataset2 = DatasetDAO("ip/asd5", "example5", "desc", "none")

    def test_url_prefix_can_be_reutilized_on_delete(self):
        """
        Tests that a url prefix can be reutilized.
        :return:
        """
        dataset = DatasetDAO("ip/asd5", "example6", "desc", "none")

        dataset.delete()

        dataset2 = DatasetDAO("ip/asd5", "example6", "desc", "none")
        self.assertEqual(dataset2.url_prefix, "ip/asd5")

    def tearDown(self):
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        taken_url_prefixes.clear()

if __name__ == '__main__':
    unittest.main()
