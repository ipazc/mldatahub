#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges

global_config.set_session_uri("mongodb://localhost:27017/unittests")

from mldatahub.factory.token_factory import TokenFactory
from mldatahub.odm.token_dao import TokenDAO

__author__ = 'Iván de Paz Centeno'

session = global_config.get_session()

token = TokenDAO("example1", 5, 5, "example1")
new_token = TokenFactory(token).create_token(description="example2",
                                             max_dataset_count=4,
                                             max_dataset_size=4,
                                             url_prefix="example1",
                                             privileges=Privileges.ADMIN_CREATE_TOKEN)

other_token = TokenFactory(token).edit_token(new_token.token_gui)

print(new_token)