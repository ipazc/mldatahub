#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import abort
from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO

__author__ = 'Iván de Paz Centeno'


class DatasetFactory(object):

    illegal_chars = "/*;:,.ç´`+Ç¨^><¿?'¡¿!\"·$%&()@~¬"

    def __init__(self, token):
        self.token = token
        self.session = global_config.get_session()

    def create_dataset(self, *args, **kwargs):
        can_create_inner_dataset = bool(self.token.privileges & Privileges.CREATE_DATASET)
        can_create_others_dataset = bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN)
        illegal_chars = self.illegal_chars

        if not any([can_create_inner_dataset, can_create_others_dataset]):
            abort(401)

        if not can_create_others_dataset and self._dataset_limit_reached():
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

        try:
            dataset = DatasetDAO(*args, **kwargs)
        except Exception as ex:
            dataset = None
            abort(500, message="Error while creating the dataset.")

        self.session.flush()

        return dataset

    def _dataset_limit_reached(self):
        return len(self.token.datasets) >= self.token.max_dataset_count

    def edit_dataset(self, edit_url_prefix, *args, **kwargs):
        can_edit_inner_dataset = bool(self.token.privileges & Privileges.EDIT_DATASET)
        can_edit_others_dataset = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)
        illegal_chars = self.illegal_chars

        if not any([can_edit_inner_dataset, can_edit_others_dataset]):
            abort(401)

        try:
            url_prefix = kwargs["url_prefix"]

            if can_edit_others_dataset:
                illegal_chars = illegal_chars[1:] # "/" is allowed for admin

            if any([illegal_char in url_prefix for illegal_char in illegal_chars]):
                abort(400)

            if "/" not in url_prefix:
                url_prefix="{}/{}".format(self.token.url_prefix, url_prefix)

            kwargs['url_prefix'] = url_prefix

        except KeyError as ex:
            pass

        if 'elements' in kwargs:
            abort(400)

        if 'comments' in kwargs:
            abort(400)

        edit_dataset = DatasetDAO.query.get(url_prefix=edit_url_prefix)

        if edit_dataset is None:
            abort(400)

        if not can_edit_others_dataset:
            if edit_dataset.url_prefix.split("/")[0] != self.token.url_prefix:
                abort(401)

            # Fix: this token can only edit a dataset if the dataset is linked to it.
            if edit_dataset not in self.token.datasets:
                abort(401)

        kwargs['modification_date'] = now()

        # Modification is performed here
        for k, v in kwargs.items():
            if v is None:
                continue

            edit_dataset[k] = v

        self.session.flush()

        return edit_dataset

    def get_dataset(self, url_prefix):
        can_view_inner_dataset = bool(self.token.privileges & Privileges.RO_WATCH_DATASET)
        can_view_others_dataset = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_view_inner_dataset, can_view_others_dataset]):
            abort(401)

        if url_prefix is None or url_prefix == "":
            abort(400)

        view_dataset = DatasetDAO.query.get(url_prefix=url_prefix)

        if view_dataset is None:
            abort(400)

        if not can_view_others_dataset and view_dataset not in self.token.datasets:
            abort(401)

        return view_dataset

    def destroy_dataset(self, url_prefix):
        can_destroy_inner_dataset = bool(self.token.privileges & Privileges.DESTROY_DATASET)
        can_destroy_others_dataset = bool(self.token.privileges & Privileges.ADMIN_DESTROY_TOKEN)

        if not any([can_destroy_inner_dataset, can_destroy_others_dataset]):
            abort(401)

        if url_prefix == "":
            abort(400)

        if not can_destroy_others_dataset and url_prefix.split("/")[0] != self.token.url_prefix:
            abort(401)

        dataset = DatasetDAO.query.get(url_prefix=url_prefix)
        if dataset is None:
            abort(400)

        dataset.delete()
        self.session.flush()

        return True
