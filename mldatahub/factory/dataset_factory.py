#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import abort
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO

__author__ = 'Iván de Paz Centeno'


class DatasetFactory(object):

    def __init__(self, token):
        self.token = token
        self.session = global_config.get_session()

    def create_dataset(self, *args, **kwargs):
        if not bool(self.token.privilege & Privileges.CREATE_DATASET):
            abort(401)

        if "url_prefix" in kwargs:
            url_prefix = kwargs["url_prefix"]
            del kwargs["url_prefix"]
        else:
            url_prefix = args[3]

        url_prefix="{}/{}".format(self.token.url_prefix, url_prefix)

        args = (args[0], args[1], url_prefix)

        dataset = DatasetDAO(*args, **kwargs)
        self.session.flush()

        return dataset