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
        self.illegal_chars = "/*;:,.ç´`+Ç¨^><¿?'¡¿!\"·$%&()@~¬"

    def create_dataset(self, *args, **kwargs):
        can_create_inner_dataset = bool(self.token.privileges & Privileges.CREATE_DATASET)
        can_create_others_dataset = bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN)
        illegal_chars = self.illegal_chars

        if not any([can_create_inner_dataset, can_create_others_dataset]):
            abort(401)

        try:
            url_prefix = kwargs["url_prefix"]
            del kwargs["url_prefix"]
        except KeyError as ex:
            url_prefix = ""
            abort(400)

        if can_create_others_dataset:
            illegal_chars = illegal_chars[1:] # "/" is allowed for admin

        if any([illegal_char in url_prefix for illegal_char in illegal_chars]):
            abort(400)

        if "/" not in url_prefix:
            url_prefix="{}/{}".format(self.token.url_prefix, url_prefix)

        kwargs['url_prefix'] = url_prefix

        dataset = DatasetDAO(*args, **kwargs)
        self.session.flush()

        return dataset

    def destroy_dataset(self, *args, **kwargs):
        can_destroy_inner_dataset = bool(self.token.privileges & Privileges.DESTROY_DATASET)
        can_destroy_others_dataset = bool(self.token.privileges & Privileges.ADMIN_DESTROY_TOKEN)

        if not any([can_destroy_inner_dataset, can_destroy_others_dataset]):
            abort(401)

        try:
            url_prefix = kwargs["url_prefix"]
        except KeyError as ex:
            url_prefix = ""
            abort(400)

        if not can_destroy_others_dataset and url_prefix.split("/")[0] != self.token.url_prefix:
            abort(401)

        dataset = DatasetDAO.query.get(url_prefix=url_prefix)
        dataset.delete()
        self.session.flush()

        return True
