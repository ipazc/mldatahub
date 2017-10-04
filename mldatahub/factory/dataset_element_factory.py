#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# MLDataHub
# Copyright (C) 2017 Iván de Paz Centeno <ipazc@unileon.es>.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.
from bson import ObjectId
from flask_restful import abort
from ming.odm.odmsession import ODMCursor
from mldatahub.storage.local.local_storage import LocalStorage

from mldatahub.factory.dataset_factory import DatasetFactory

from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.odm.token_dao import TokenDAO
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.dataset_dao import DatasetElementDAO

__author__ = 'Iván de Paz Centeno'


class DatasetElementFactory(object):

    def __init__(self, token: TokenDAO, dataset: DatasetDAO):
        self.token = token
        self.dataset = dataset
        self.session = global_config.get_session()
        self.local_storage = global_config.get_local_storage() # type: LocalStorage

        # Can token modify dataset? let's check.
        can_alter_datasets = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        # It is a must for token to have admin privileges or to have access to this dataset.
        if not can_alter_datasets and not self.token.has_dataset(self.dataset):
            abort(401)

    def _dataset_limit_reached(self, new_elements_count=1) -> bool:
        return len(self.dataset.elements) + new_elements_count > self.token.max_dataset_size

    def create_element(self, **kwargs) -> DatasetElementDAO:
        can_create_inner_element = bool(self.token.privileges & Privileges.ADD_ELEMENTS)
        can_create_others_elements = bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN)

        if not any([can_create_inner_element, can_create_others_elements]):
            abort(401)

        if not can_create_others_elements and self._dataset_limit_reached():
            abort(401, message="Dataset limit reached.")

        try:
            element_content = kwargs["content"]
            del kwargs["content"]
        except KeyError as ex:
            element_content = None

        if element_content is None:
            if 'http_ref' not in kwargs and 'file_ref_id' not in kwargs:
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
        if 'file_ref_id' not in kwargs:
            kwargs['file_ref_id'] = None

        dataset_element = DatasetElementDAO(**kwargs)
        self.session.flush()

        return dataset_element

    def create_elements(self, elements_kwargs) -> list:
        can_create_inner_element = bool(self.token.privileges & Privileges.ADD_ELEMENTS)
        can_create_others_elements = bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN)

        if not any([can_create_inner_element, can_create_others_elements]):
            abort(401)

        if not can_create_others_elements and self._dataset_limit_reached(len(elements_kwargs)):
            abort(401, message="Dataset limit reached. Can't add this set of elements. There are only {} slots free".format(len(self.dataset.elements) - self.token.max_dataset_size))

        dataset_elements = []
        for kwargs in elements_kwargs:

            try:
                element_content = kwargs["content"]
                del kwargs["content"]
            except KeyError as ex:
                element_content = None

            if element_content is None:
                if 'http_ref' not in kwargs and 'file_ref_id' not in kwargs:
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
            if 'file_ref_id' not in kwargs:
                kwargs['file_ref_id'] = None

            dataset_element = DatasetElementDAO(**kwargs)
            dataset_elements.append(dataset_element)

        self.session.flush()

        return dataset_elements

    def edit_element(self, element_id:ObjectId, **kwargs) -> DatasetElementDAO:
        can_edit_inner_element = bool(self.token.privileges & Privileges.EDIT_ELEMENTS)
        can_edit_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_edit_inner_element, can_edit_others_elements]):
            abort(401)

        if 'file_ref_id' in kwargs and not can_edit_others_elements:
            abort(401, message="File ref ID not allowed.")

        if 'content' in kwargs:
            # New content to append here...
            file_id = self.local_storage.put_file_content(kwargs['content'])
            kwargs['file_ref_id'] = file_id

        if ('dataset' in kwargs or 'dataset_id' in kwargs) and not can_edit_others_elements:
            abort(401, message="Dataset can't be modified.")

        dataset_element = DatasetElementDAO.query.get(_id=element_id)

        if dataset_element is None:
            abort(404, message="Dataset element wasn't found")

        if not self.dataset.has_element(dataset_element) and not can_edit_others_elements:
            abort(401, message="Operation not allowed, element is not contained by the dataset")

        for k, v in kwargs.items():
            if k is not None and v is not None:
                dataset_element[k] = v

        self.session.flush()

        return dataset_element

    def edit_elements(self, elements_kwargs) -> ODMCursor:
        can_edit_inner_element = bool(self.token.privileges & Privileges.EDIT_ELEMENTS)
        can_edit_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_edit_inner_element, can_edit_others_elements]):
            abort(401)

        elements_ids = list(elements_kwargs.keys())

        if len(elements_ids) > global_config.get_page_size():
            abort(416, message="Page size exceeded")

        dataset_elements = DatasetElementDAO.query.find({"dataset_id": self.dataset._id, "_id": { "$in" : elements_ids }})

        for dataset_element in dataset_elements:
            kwargs = elements_kwargs[dataset_element._id]

            if 'file_ref_id' in kwargs and not can_edit_others_elements:
                abort(401, message="File ref ID not allowed.")

            if 'content' in kwargs:
                # New content to append here...
                file_id = self.local_storage.put_file_content(kwargs['content'])
                kwargs['file_ref_id'] = file_id

            if ('dataset' in kwargs or 'dataset_id' in kwargs) and not can_edit_others_elements:
                abort(401, message="Dataset can't be modified.")

            for k,v in elements_kwargs[dataset_element._id].items():
                if k is not None and v is not None:
                    dataset_element[k] = v

        if dataset_elements.count() == 0:
            abort(404, message="Elements not found.")

        self.session.flush()

        return dataset_elements

    def clone_element(self, element_id:ObjectId, dest_dataset_url_prefix:str) -> DatasetElementDAO:
        can_edit_inner_element = bool(self.token.privileges & (Privileges.RO_WATCH_DATASET +
                                                               Privileges.EDIT_DATASET + Privileges.ADD_ELEMENTS))
        can_edit_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_edit_inner_element, can_edit_others_elements]):
            abort(401)


        dataset = DatasetFactory(self.token).get_dataset(dest_dataset_url_prefix)

        d_e_f = DatasetElementFactory(self.token, dataset)

        if not can_edit_others_elements and d_e_f._dataset_limit_reached():
            abort(401,message="Dataset limit reached.")

        element = self.get_element_info(element_id)

        new_element = dataset.add_element(element.title, element.description, element.file_ref_id, element.http_ref, list(element.tags))

        self.session.flush()

        return new_element

    def clone_elements(self, elements_ids: list, dest_dataset_url_prefix: str) -> list:
        can_edit_inner_element = bool(self.token.privileges & (Privileges.RO_WATCH_DATASET +
                                                               Privileges.EDIT_DATASET + Privileges.ADD_ELEMENTS))
        can_edit_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_edit_inner_element, can_edit_others_elements]):
            abort(401)

        dataset = DatasetFactory(self.token).get_dataset(dest_dataset_url_prefix)

        d_e_f = DatasetElementFactory(self.token, dataset)

        if not can_edit_others_elements and d_e_f._dataset_limit_reached(len(elements_ids)):
            abort(401, message="Dataset limit reached. Can't add this set of elements. There are only {} slots free".format(len(self.dataset.elements) - self.token.max_dataset_size))

        elements = self.get_specific_elements_info(elements_ids)

        new_elements = [dataset.add_element(element.title, element.description, element.file_ref_id, element.http_ref, list(element.tags)) for element in elements]

        self.session.flush()

        return new_elements

    def get_element_info(self, element_id:ObjectId) -> DatasetElementDAO:
        can_view_inner_element = bool(self.token.privileges & Privileges.RO_WATCH_DATASET)
        can_view_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_view_inner_element, can_view_others_elements]):
            abort(401)

        dataset_element = DatasetElementDAO.query.get(_id=element_id)

        if dataset_element is None:
            abort(401)

        if not self.dataset.has_element(dataset_element):
            abort(401)

        return dataset_element

    def get_elements_info(self, page=0, options=None) -> ODMCursor:
        can_view_inner_element = bool(self.token.privileges & Privileges.RO_WATCH_DATASET)
        can_view_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_view_inner_element, can_view_others_elements]):
            abort(401)

        if options is not None:
            query = options
        else:
            query = {}

        query["dataset_id"] = self.dataset._id

        return DatasetElementDAO.query.find(query).sort("addition_date", 1).skip(page*global_config.get_page_size()).limit(global_config.get_page_size())

    def get_specific_elements_info(self, elements_id:list) -> ODMCursor:
        can_view_inner_element = bool(self.token.privileges & Privileges.RO_WATCH_DATASET)
        can_view_others_elements = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_view_inner_element, can_view_others_elements]):
            abort(401)

        if len(elements_id) > global_config.get_page_size():
            abort(416, message="Page size exceeded")

        return DatasetElementDAO.query.find({"dataset_id": self.dataset._id, "_id": {"$in": elements_id }})

    def get_element_thumbnail(self, element_id:ObjectId) -> bytes:
        # The get_element_info() method is going to make all the required checks for the retrieval of the thumbnail.
        dataset_element = self.get_element_info(element_id)

        # TODO: build thumbnail from dataset_element
        # TODO: Cache layer goes here. Maybe a local CDN?
        if dataset_element.file_ref_id is not None:
            thumbnail = b''
        else:
            thumbnail = b''

        return thumbnail

    def get_element_content(self, element_id:ObjectId) -> bytes:
        # The get_element_info() method is going to make all the required checks for the retrieval of the thumbnail.
        dataset_element = self.get_element_info(element_id)

        if dataset_element.file_ref_id is None:
            abort(404, message="Element could not be found.")

        content = self.local_storage.get_file_content(dataset_element.file_ref_id)

        return content

    def get_elements_content(self, elements_id:list) -> dict:
        # The get_specific_elements_info() method is going to make all the required checks for the retrieval of the thumbnail.
        dataset_elements = self.get_specific_elements_info(elements_id)

        if dataset_elements.count() < len(elements_id):
            # Let's find which elements do not exist.
            retrieved_elements_ids = [element._id for element in dataset_elements]
            lost_elements = [element_id for element_id in elements_id if element_id not in retrieved_elements_ids]
            abort(404, message="The following elements couldn't be retrieved: {}".format(lost_elements))

        contents = {element._id: self.local_storage.get_file_content(element.file_ref_id) for element in dataset_elements}
        return contents

    def destroy_element(self, element_id:ObjectId) -> DatasetDAO:
        can_destroy_inner_element = bool(self.token.privileges & Privileges.DESTROY_ELEMENTS)
        can_destroy_others_elements = bool(self.token.privileges & Privileges.ADMIN_DESTROY_TOKEN)

        if not any([can_destroy_inner_element, can_destroy_others_elements]):
            abort(401)

        # Destroy only removes the reference to the file, but not the file itself.
        # Files are automatically removed by the Garbage Collector observer.

        dataset_element = DatasetElementDAO.query.get(_id=element_id)

        if dataset_element is None:
            abort(401)

        if not self.dataset.has_element(dataset_element) and not can_destroy_others_elements:
            abort(401)

        dataset_element.delete()

        self.session.flush()
        self.dataset = self.session.refresh(self.dataset)

        return self.dataset

    def destroy_elements(self, elements_ids:list) -> DatasetDAO:
        can_destroy_inner_element = bool(self.token.privileges & Privileges.DESTROY_ELEMENTS)
        can_destroy_others_elements = bool(self.token.privileges & Privileges.ADMIN_DESTROY_TOKEN)

        if not any([can_destroy_inner_element, can_destroy_others_elements]):
            abort(401)

        if len(elements_ids) > global_config.get_page_size():
            abort(416, message="Page size exceeded")

        # Destroy only removes the reference to the file, but not the file itself.
        # Files are automatically removed by the Garbage Collector observer.

        dataset_elements = DatasetElementDAO.query.find({'dataset_id': self.dataset._id, '_id': {'$in': elements_ids}})

        if dataset_elements.count() < len(elements_ids):
            # Let's find which elements do not exist.
            retrieved_elements_ids = [element._id for element in dataset_elements]
            lost_elements = [element_id for element_id in elements_ids if element_id not in retrieved_elements_ids]
            abort(404, message="The following elements couldn't be deleted (they don't exist?): {}".format(lost_elements))


        for d in dataset_elements:
            d.delete()

        self.session.flush()

        self.dataset = self.session.refresh(self.dataset)

        return self.dataset