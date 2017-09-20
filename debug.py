#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
from mldatahub.odm.dataset_dao import DatasetDAO

__author__ = 'Iván de Paz Centeno'

#dataset1 = DatasetDAO("lala", "a", "b", "c")
#dataset2 = DatasetDAO("lala2", "a", "b", "c")
#dataset3 = DatasetDAO("lala3", "a", "b", "c")
#dataset4 = DatasetDAO("lala4", "a", "b", "c")

session = global_config.get_session()

#session.flush()

for d in DatasetDAO.query.find({"title": "a"}).skip(4).limit(2):
    print(d.url_prefix)

