#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from werkzeug.exceptions import Unauthorized, Conflict, BadRequest
from mldatahub.config.privileges import Privileges

__author__ = 'Iván de Paz Centeno'

import unittest
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetCommentDAO, DatasetElementDAO, DatasetElementCommentDAO, \
    taken_url_prefixes
from mldatahub.factory.token_factory import TokenFactory
from mldatahub.odm.token_dao import TokenDAO



class TestTokenFactory(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        TokenDAO.query.remove()
        taken_url_prefixes.clear()

    def test_factory_creation_token(self):
        """
        Factory can create tokens.
        :return:
        """

        # Unprivileged token should not be able to create new tokens out of his prefix.
        token = TokenDAO("example1", 5, 5, "example1")

        with self.assertRaises(Unauthorized) as ex:
            new_token = TokenFactory(token).create_token(description="example2",
                                                         max_dataset_count=4,
                                                         max_dataset_size=4,
                                                         url_prefix="exampl21")

        new_token = TokenFactory(token).create_token(description="example2",
                                                     max_dataset_count=4,
                                                     max_dataset_size=4,
                                                     url_prefix="example1")

        self.assertEqual(new_token.url_prefix, token.url_prefix)
        self.assertEqual(new_token.description, "example2")

        # Unprivileged token should not be able to create new tokens out of his privilege set.
        with self.assertRaises(Conflict) as ex:
            new_token = TokenFactory(token).create_token(description="example2",
                                                         max_dataset_count=4,
                                                         max_dataset_size=4,
                                                         url_prefix="example1",
                                                         privileges=Privileges.ADMIN_CREATE_TOKEN)

        # Privileged token can create tokens anywhere.
        token = TokenDAO("admin1", 5, 5, "admin1", privileges=Privileges.ADMIN_CREATE_TOKEN)

        new_token = TokenFactory(token).create_token(description="example2",
                                                     max_dataset_count=4,
                                                     max_dataset_size=4,
                                                     url_prefix="exampl21")

        self.assertEqual(new_token.url_prefix, "exampl21")

        # Privileged token can create tokens with any privilege set.
        new_token = TokenFactory(token).create_token(description="example2",
                                                     max_dataset_count=4,
                                                     max_dataset_size=4,
                                                     url_prefix="examplsasa21",
                                                     privileges=Privileges.ADD_ELEMENTS + Privileges.DESTROY_DATASET)

        self.assertEqual(new_token.url_prefix, "examplsasa21")
        self.assertEqual(new_token.privileges, Privileges.ADD_ELEMENTS + Privileges.DESTROY_DATASET)

    def test_factory_can_get_tokens(self):
        """
        Factory can retrieve tokens.
        """
        token = TokenDAO("example1", 5, 5, "uri1")
        token2 = TokenDAO("example2", 5, 5, "uri2")
        token3 = TokenDAO("example3", 5, 5, "uri2", privileges=Privileges.USER_EDIT_TOKEN)
        token4 = TokenDAO("example3", 5, 5, "uri2", privileges=Privileges.ADMIN_EDIT_TOKEN)

        self.session.flush()
        new_token = TokenFactory(token).get_token(token.token_gui)

        self.assertEqual(token.token_gui, new_token.token_gui)
        self.assertEqual(token.description, new_token.description)
        self.assertEqual(token.url_prefix, new_token.url_prefix)

        # Cannot request info from a different token
        with self.assertRaises(Unauthorized):
            new_token = TokenFactory(token).get_token(token2.token_gui)

        # Cannot request all the tokens if not privileged.
        with self.assertRaises(Unauthorized):
            new_token = TokenFactory(token).get_tokens()

        # Can request all the tokens if has privilege for user edit tokens.
        token_list = TokenFactory(token3).get_tokens()

        self.assertEqual(len(token_list), 3)

        # Can not request all the tokens from a different url prefix
        with self.assertRaises(Unauthorized):
            token_list = TokenFactory(token3).get_tokens(url_prefix="uri1")

        # Admin can request tokens from all the url prefixes
        token_list = TokenFactory(token4).get_tokens()
        self.assertEqual(len(token_list), 4)

        token_list = TokenFactory(token4).get_tokens(url_prefix="uri1")
        self.assertEqual(len(token_list), 1)

        token_list = TokenFactory(token4).get_tokens(url_prefix="uri2")
        self.assertEqual(len(token_list), 3)

        token_list = TokenFactory(token4).get_tokens(url_prefix="UNKNOWN")
        self.assertEqual(len(token_list), 0)

        # Invalid token-GUI raises exception
        with self.assertRaises(BadRequest):
            token_list = TokenFactory(token4).get_token(token_gui="INVALID_GUI")


    def test_factory_can_modify_tokens(self):
        """
        Factory can modify tokens depending on privileges of main token.
        :return:
        """
        token = TokenDAO("example1", 5, 5, "uri1")
        token2 = TokenDAO("example2", 5, 5, "uri2")
        token3 = TokenDAO("example3", 5, 5, "uri2", privileges=Privileges.USER_EDIT_TOKEN)
        token4 = TokenDAO("example3", 5, 5, "uri2", privileges=Privileges.ADMIN_EDIT_TOKEN)

        self.session.flush()

        # token is not allowed to change token2
        with self.assertRaises(Unauthorized):
            new_token = TokenFactory(token).edit_token(token_gui=token2.token_gui, description="New description")

        # token3 should be allowed to change token2
        new_token = TokenFactory(token3).edit_token(token_gui=token2.token_gui, description="New description")

        self.assertEqual(token2.token_gui, new_token.token_gui)
        self.assertEqual("New description", new_token.description)
        self.assertEqual("uri2", new_token.url_prefix)

        # token4 should be allowed to change token2
        new_token = TokenFactory(token4).edit_token(token_gui=token2.token_gui, description="New description2")

        self.assertEqual(token2.token_gui, new_token.token_gui)
        self.assertEqual("New description2", new_token.description)
        self.assertEqual("uri2", new_token.url_prefix)

        # token is not allowed to change itself
        with self.assertRaises(Unauthorized):
            new_token = TokenFactory(token).edit_token(token_gui=token.token_gui, description="New description")

        # Non valid token GUI should raise exception
        with self.assertRaises(BadRequest):
            new_token = TokenFactory(token).edit_token(token_gui="INVALID", description="New description")

        # Token3 should not be able to change url prefix
        with self.assertRaises(Unauthorized):
            new_token = TokenFactory(token3).edit_token(token_gui=token2.token_gui, url_prefix="new_prefix")

        # However, Token4 should be able to change url prefix
        new_token = TokenFactory(token4).edit_token(token_gui=token2.token_gui, url_prefix="new_prefix")
        self.assertEqual("new_prefix", new_token.url_prefix)


    def tearDown(self):
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        TokenDAO.query.remove()
        taken_url_prefixes.clear()

if __name__ == '__main__':
    unittest.main()
