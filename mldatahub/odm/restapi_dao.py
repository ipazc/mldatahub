#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from ming import schema
from ming.odm import FieldProperty, MappedClass
from mldatahub.config.config import global_config


__author__ = "Iván de Paz Centeno"


session = global_config.get_session()


class RestAPIDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'restapi'

    _id = FieldProperty(schema.ObjectId)
    ip = FieldProperty(schema.String)
    last_access = FieldProperty(schema.DateTime)
    num_accesses = FieldProperty(schema.Int)


from ming.odm import Mapper
Mapper.compile_all()