#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bson import ObjectId
from mldatahub.config.config import global_config
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = 'Iván de Paz Centeno'


token = TokenDAO("example", 1, 1, "none")
dataset = DatasetDAO("none2", "none", "none", "none")

session = global_config.get_session()
session.flush()

token = token.link_dataset(dataset)
o = ObjectId('59ccc539b9a7c02a2c4ad42b')
print(token._datasets)

print(o in token._datasets)
token.delete()
dataset.delete()
session.flush()
