#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
from mldatahub.odm.dataset_dao import DatasetDAO

__author__ = 'Iván de Paz Centeno'
session = global_config.get_session()

#d = DatasetDAO("lala/dededa", "aa", "de", "as")
#session.flush()

print(DatasetDAO.query.get(url_prefix="lala/dede"))

#print(d)

