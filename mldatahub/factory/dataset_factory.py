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

from flask_restful import abort
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges

__author__ = 'Iván de Paz Centeno'


class DatasetFactory(object):

    illegal_chars = "/*;:,.ç´`+Ç¨^><¿?'¡¿!\"·$%&()@~¬"

    def __init__(self, token:TokenDAO):
        self.token = token
        self.session = global_config.get_session()

    def create_dataset(self, *args, **kwargs) -> DatasetDAO:
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

        # Limit arguments.
        kwargs['fork_count'] = 0

        if 'forked_from' in kwargs:
            del kwargs['forked_from']

        if 'forked_from_id' in kwargs:
            del kwargs['forked_from_id']

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
            abort(400, message=str(ex))

        self.session.flush()

        return dataset

    def fork_dataset(self, dataset_url_prefix, token_src, *args, **kwargs) -> DatasetDAO:
        source_factory = DatasetFactory(token_src)

        target_dataset = source_factory.get_dataset(dataset_url_prefix)

        if target_dataset is None:
            abort(404, message="Dataset wasn't found.")

        if 'options' in kwargs:
            options = kwargs['options']
            del kwargs['options']
        else:
            options = None

        if 'title' not in kwargs or kwargs['title'] is None:
            kwargs['title'] = target_dataset.title

        if 'description' not in kwargs or kwargs['description'] is None:
            kwargs['description'] = target_dataset.description

        if 'reference' not in kwargs or kwargs['reference'] is None:
            kwargs['reference'] = target_dataset.reference

        if 'tags' not in kwargs or kwargs['tags'] is None:
            kwargs['tags'] = target_dataset.tags

        fork_dataset = self.create_dataset(*args, **kwargs)

        target_dataset.fork_count += 1
        fork_dataset.forked_from = target_dataset

        # Now we link all the elements to the forked dataset.
        # When a modification or removal of any of the elements is proposed,
        # the element should be cloned just an instant before.
        for element in target_dataset.get_elements(options):
            element.link_dataset(fork_dataset)
            #fork_dataset.add_element(element.title, element.description, element.file_ref_id, element.http_ref, list(element.tags))

        self.session.flush()

        return fork_dataset

    def _dataset_limit_reached(self) -> bool:
        return len(self.token.datasets) >= self.token.max_dataset_count

    def edit_dataset(self, edit_url_prefix:str, *args, **kwargs) -> DatasetDAO:
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
            abort(404, message="Dataset wasn't found.")

        if not can_edit_others_dataset:
            if edit_dataset.url_prefix.split("/")[0] != self.token.url_prefix:
                abort(401, message="Dataset can't be accessed.")

            # Fix: this token can only edit a dataset if the dataset is linked to it.
            if not self.token.has_dataset(edit_dataset):
                abort(401, message="Dataset can't be accessed.")

        kwargs['modification_date'] = now()

        # Modification is performed here
        for k, v in kwargs.items():
            if v is None:
                continue

            edit_dataset[k] = v

        self.session.flush()

        return edit_dataset

    def get_dataset(self, url_prefix:str) -> DatasetDAO:
        can_view_inner_dataset = bool(self.token.privileges & Privileges.RO_WATCH_DATASET)
        can_view_others_dataset = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if not any([can_view_inner_dataset, can_view_others_dataset]):
            abort(401)

        if url_prefix is None or url_prefix == "":
            abort(400, message="Url prefix of the dataset is required")

        view_dataset = DatasetDAO.query.get(url_prefix=url_prefix)

        if view_dataset is None:
            abort(404, message="Dataset wasn't found.")

        if not can_view_others_dataset and not self.token.has_dataset(view_dataset):
            abort(401, message="Dataset can't be accessed.")

        return view_dataset

    def destroy_dataset(self, url_prefix:str) -> bool:
        can_destroy_inner_dataset = bool(self.token.privileges & Privileges.DESTROY_DATASET)
        can_destroy_others_dataset = bool(self.token.privileges & Privileges.ADMIN_DESTROY_TOKEN)

        if not any([can_destroy_inner_dataset, can_destroy_others_dataset]):
            abort(401)

        if url_prefix is None or url_prefix == "":
            abort(400, message="Url prefix of the dataset is required")

        if not can_destroy_others_dataset and url_prefix.split("/")[0] != self.token.url_prefix:
            abort(401, message="Dataset can't be accessed.")

        dataset = DatasetDAO.query.get(url_prefix=url_prefix)
        if dataset is None:
            abort(404, message="Dataset wasn't found.")

        dataset.delete()
        self.session.flush()

        return True
