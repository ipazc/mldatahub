#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from io import BytesIO

from flask import make_response, send_file
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

        # Can token modify dataset? let's check.
        can_alter_datasets = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        # It is a must for token to have admin privileges or to have access to this dataset.
        if not can_alter_datasets and self.dataset not in self.token.datasets:
            abort(401)

    def _dataset_limit_reached(self):
        return len(self.dataset.elements) > self.token.max_dataset_size

    def create_element(self, **kwargs):
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

            if 'file_ref_id' in kwargs and not can_create_others_elements:
                # Antiexploit: otherwise users might add resources from other tokens over here.
                abort(401)
        else:
            # We save the file into the storage
            file_id = self.local_storage.put_file_content(element_content)
            kwargs['file_ref_id'] = file_id

        if ('dataset' in kwargs or 'dataset_id' in kwargs) and not can_create_others_elements:
            abort(401)

        kwargs['dataset'] = self.dataset

        dataset_element = DatasetElementDAO(**kwargs)
        self.session.flush()

        return dataset_element


    def edit_element(self, element_id, **kwargs):
        can_edit_inner_element = bool(self.token.privileges & Privileges.EDIT_ELEMENTS)
        can_edit_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_edit_inner_element, can_edit_others_elements]):
            abort(401)

        if 'file_ref_id' in kwargs and not can_edit_others_elements:
            abort(401)

        if 'element_content' in kwargs:
            # New content to append here...
            file_id = self.local_storage.put_file_content(kwargs['element_content'])
            kwargs['file_ref_id'] = file_id

        if ('dataset' in kwargs or 'dataset_id' in kwargs) and not can_edit_others_elements:
            abort(401)

        dataset_element = DatasetElementDAO.query.get(_id=element_id)

        if dataset_element is None:
            abort(401)

        for k, v in kwargs.items():
            if k is not None and v is not None:
                dataset_element[k] = v

        self.session.flush()

        return dataset_element

    def get_element_info(self, element_id):
        can_view_inner_element = bool(self.token.privileges & Privileges.RO_WATCH_DATASET)
        can_view_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_view_inner_element, can_view_others_elements]):
            abort(401)

        dataset_element = DatasetElementDAO.query.get(_id=element_id)

        if dataset_element is None:
            abort(401)

        return dataset_element

    def get_element_thumbnail(self, element_id):
        # The get_element_info() method is going to make all the required checks for the retrieval of the thumbnail.
        dataset_element = self.get_element_info(element_id)

        # TODO: build thumbnail from dataset_element
        # TODO: Cache layer goes here. Maybe a local CDN?
        if dataset_element.file_ref_id is not None:
            thumbnail = b''
        else:
            thumbnail = b''

        return thumbnail

    def get_element_content(self, element_id):
        # The get_element_info() method is going to make all the required checks for the retrieval of the thumbnail.
        dataset_element = self.get_element_info(element_id)

        if dataset_element.file_ref_id is None:
            abort(404)

        content = self.local_storage.get_file_content(dataset_element.file_ref_id)

        with BytesIO(content) as fp:
            result = send_file(fp)

        return result

    def destroy_element(self, element_id):
        can_destroy_inner_element = bool(self.token.privileges & Privileges.DESTROY_ELEMENTS)
        can_destroy_others_elements = bool(self.token.privileges & Privileges.DESTROY_ELEMENTS)

        if not any([can_destroy_inner_element, can_destroy_others_elements]):
            abort(401)

        # Destroy only removes the reference to the file, but not the file itself.
        # Files are automatically removed by a cron timer that checks for freed files.

        dataset_element = DatasetElementDAO.query.get(_id=element_id)

        if dataset_element is None:
            abort(401)

        dataset_element.delete()

        self.session.flush()
        return "Done"