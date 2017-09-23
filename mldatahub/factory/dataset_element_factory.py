#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import abort
from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.dataset_dao import DatasetElementDAO
from mldatahub.storage.local.local_storage import LocalStorage

__author__ = 'Iván de Paz Centeno'


class DatasetElementFactory(object):

    def __init__(self, token, dataset):
        self.token = token
        self.dataset = dataset
        self.session = global_config.get_session()
        self.local_storage = global_config.get_local_storage()

    def _dataset_limit_reached(self):
        return len(self.dataset.elements) > self.token.max_dataset_size

    def create_element(self, *args, **kwargs):
        can_create_inner_element = bool(self.token.privileges & Privileges.ADD_ELEMENTS)
        can_create_others_elements = bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN)

        if not any([can_create_inner_element, can_create_others_elements]):
            abort(401)

        if not can_create_others_elements and self._dataset_limit_reached():
            abort(401)

        try:
            element_content = kwargs["element_content"]
            del kwargs["element_content"]
        except KeyError as ex:
            element_content = None

        if element_content is None:
            if 'http_ref' not in kwargs:
                abort(400)

            if 'file_ref_id' in kwargs:  # Antiexploit: otherwise users might add resources from other tokens over here.
                abort(401)
        else:
            # We save the file into the storage
            file_id = self.local_storage.put_file_content(element_content)
            kwargs['file_ref_id'] = file_id

        # Ref file
        dataset_element = DatasetElementDAO(*args, **kwargs)
        self.session.flush()

        #self.token = TokenFactory(self.token).link_datasets(self.token.token_gui, [dataset.url_prefix])

        return dataset


    def edit_element(self, element_id):

    def get_element_info(self, element_id):

    def get_element_thumbnail(self, element_id):

    def get_element_content(self, element_id):

    def destroy_element(self, element_id):

