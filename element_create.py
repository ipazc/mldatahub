#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetElementDAO

__author__ = 'Iván de Paz Centeno'


dataset = DatasetDAO("ivan/test4", "example", "example_man", "none")
element = DatasetElementDAO("test", "example", "example_man", dataset=dataset)

global_config.get_session().flush()
dataset = dataset.update()

print(dataset.elements)

element.delete()
global_config.get_session().flush()
d2 = global_config.get_session().refresh(dataset)
print(dataset.elements)
print(d2.elements)
