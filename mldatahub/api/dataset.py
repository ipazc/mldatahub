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

from flask import request
from flask_restful import reqparse, abort
from mldatahub.api.tokenized_resource import TokenizedResource, control_access
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.factory.dataset_factory import DatasetFactory
from mldatahub.factory.token_factory import TokenFactory
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = 'Iván de Paz Centeno'


class Datasets(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("url_prefix", type=str, required=False, help="URL prefix to get tokens from.")
        self.post_parser = reqparse.RequestParser()
        self.session = global_config.get_session()
        arguments = {
            "url_prefix":
                {
                    "type": str,
                    "required": True,
                    "help": "URL prefix for this dataset. Characters \"{}\" not allowed".format(DatasetFactory.illegal_chars),
                    "location": "json"
                },
            "title":
                {
                    "type": str,
                    "required": True,
                    "help": "Title for the dataset.",
                    "location": "json"
                },
            "description":
                {
                    "type": str,
                    "required": True,
                    "help": "Description for the dataset.",
                    "location": "json"
                },
            "reference":
                {
                    "type": str,
                    "required": True,
                    "help": "Reference data (perhaps a Bibtex in string format?)",
                    "location": "json"
                },
            "tags":
                {
                    "type": list,
                    "required": False,
                    "help": "Tags for the dataset (ease the searches for this dataset).",
                    "location": "json"
                },
        }

        for argument, kwargs in arguments.items():
            self.post_parser.add_argument(argument, **kwargs)

    @control_access()
    def get(self):
        """
        Retrieves all the datasets associated to the current token.
        :return:
        """
        self.session.clear()

        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        result = [dataset.serialize() for dataset in token.datasets]

        return result

    @control_access()
    def post(self):
        """
        Creates a dataset and links it to the token
        :return:
        """
        required_privileges = [
            Privileges.CREATE_DATASET,
            Privileges.EDIT_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        kwargs = self.post_parser.parse_args()
        if 'tags' in request.json:
            kwargs['tags'] = request.json['tags'] # fast fix for split-bug of the tags.

        dataset = DatasetFactory(token).create_dataset(**kwargs)

        self.session.flush()

        TokenFactory(token).link_datasets(token.token_gui, [dataset])

        self.session.flush()

        result = dataset.serialize()

        return result, 201


class Dataset(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("url_prefix", type=str, required=False, help="URL prefix to get tokens from.")
        self.patch_parser = reqparse.RequestParser()
        self.session = global_config.get_session()
        arguments = {
            "url_prefix":
                {
                    "type": str,
                    "required": False,
                    "help": "URL prefix for this dataset. Characters \"{}\" not allowed".format(
                        DatasetFactory.illegal_chars),
                    "location": "json"
                },
            "title":
                {
                    "type": str,
                    "required": False,
                    "help": "Title for the dataset.",
                    "location": "json"
                },
            "description":
                {
                    "type": str,
                    "required": False,
                    "help": "Description for the dataset.",
                    "location": "json"
                },
            "reference":
                {
                    "type": str,
                    "required": False,
                    "help": "Reference data (perhaps a Bibtex in string format?)",
                    "location": "json"
                },
            "tags":
                {
                    "type": list,
                    "required": False,
                    "help": "Tags for the dataset (ease the searches for this dataset).",
                    "location": "json"
                },
        }

        for argument, kwargs in arguments.items():
            self.patch_parser.add_argument(argument, **kwargs)

    @control_access()
    def get(self, token_prefix, dataset_prefix):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)
        result = dataset.serialize()

        return result, 200

    @control_access()
    def patch(self, token_prefix, dataset_prefix):
        required_privileges = [
            Privileges.EDIT_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        kwargs = self.patch_parser.parse_args()

        if "tags" in request.json:
            kwargs['tags'] = request.json['tags']  # fast fix for split-bug of the tags.

        kwargs = {k:v for k, v in kwargs.items() if v is not None}

        DatasetFactory(token).edit_dataset(full_dataset_url_prefix, **kwargs)

        self.session.flush()

        return "Done", 200

    @control_access()
    def delete(self, token_prefix, dataset_prefix):
        required_privileges = [
            Privileges.DESTROY_DATASET,
            Privileges.ADMIN_DESTROY_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        DatasetFactory(token).destroy_dataset(full_dataset_url_prefix)

        self.session.flush()

        return "Done", 200


class DatasetForker(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.post_parser = reqparse.RequestParser()
        self.session = global_config.get_session()
        arguments = {
            "url_prefix":
                {
                    "type": str,
                    "required": True,
                    "help": "URL prefix for this dataset. Characters \"{}\" not allowed".format(DatasetFactory.illegal_chars),
                    "location": "json"
                },
            "title":
                {
                    "type": str,
                    "required": True,
                    "help": "Title for the dataset.",
                    "location": "json"
                },
            "description":
                {
                    "type": str,
                    "required": True,
                    "help": "Description for the dataset.",
                    "location": "json"
                },
            "reference":
                {
                    "type": str,
                    "required": True,
                    "help": "Reference data (perhaps a Bibtex in string format?)",
                    "location": "json"
                },
            "tags":
                {
                    "type": list,
                    "required": False,
                    "help": "Tags for the dataset (ease the searches for this dataset).",
                    "location": "json"
                },
            "options":
                {
                    "type": dict,
                    "required": False,
                    "location": "json",
                    "help": "options string"
                }
        }

        for argument, kwargs in arguments.items():
            self.post_parser.add_argument(argument, **kwargs)

    @control_access()
    def post(self, token_prefix, dataset_prefix, dest_token_gui):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]
        _, token = self.token_parser.parse_args(request, required_any_token_privileges=required_privileges)

        kwargs = self.post_parser.parse_args()

        dest_token = TokenDAO.query.get(token_gui=dest_token_gui)

        dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        if dest_token is None:
            abort(404, message="Token not found.")

        if 'options' in request.json:
            options = request.json['options']
        else:
            options = None

        kwargs['options'] = options
        kwargs['tags'] = request.json['tags']

        dataset = DatasetFactory(dest_token).fork_dataset(dataset_url_prefix, token, **kwargs)

        TokenFactory(dest_token).link_datasets(dest_token.token_gui, [dataset])

        self.session.flush()

        result = dataset.serialize()

        return result


class DatasetSize(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.session = global_config.get_session()

    @control_access()
    def get(self, token_prefix, dataset_prefix):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        total_size = global_config.get_storage().get_files_size([l.file_ref_id for l in dataset.elements])
        dataset.size = total_size
        self.session.flush()

        return total_size, 200