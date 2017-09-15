#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Iván de Paz Centeno'

import unittest
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.dataset import Dataset
from mldatahub.odm.token import Token


class TestTokenODM(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()

    def test_token_can_be_created_and_destroyed(self):
        """
        Tests that tokens are successfully created and removed.
        :return:
        """
        token = Token("example", 2, 3)
        token_gui = token.token_gui
        self.assertTrue(len(token.token_gui) > 10)
        self.session.flush()
        token = Token.query.get(token_gui=token_gui)
        self.assertEqual(token.token_gui, token_gui)
        token.delete()
        token = Token.query.get(token_gui)
        self.assertIsNone(token)


    def tearDown(self):
        Dataset.query.remove()
        Token.query.remove()

if __name__ == '__main__':
    unittest.main()
