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

from io import BytesIO
from bson import ObjectId
from flask import send_file, request
from flask_restful import reqparse, abort
from pyzip import PyZip

from mldatahub.api.tokenized_resource import TokenizedResource, control_access
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.factory.dataset_element_factory import DatasetElementFactory
from mldatahub.factory.dataset_factory import DatasetFactory

__author__ = "Iván de Paz Centeno"


class DatasetElements(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("page", type=int, required=False, help="Page number to retrieve.", default=0)
        self.get_parser.add_argument("elements", type=list, required=False, location="json", help="List of IDs to retrieve. Overrides the page attribute")
        self.get_parser.add_argument("options", type=dict, required=False, location="json", help="options string")

        self.post_parser = reqparse.RequestParser()
        self.session = global_config.get_session()

        arguments = {
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
            "http_ref":
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
            self.post_parser.add_argument(argument, **kwargs)

    @control_access()
    def get(self, token_prefix, dataset_prefix):
        """
        Retrieves dataset elements from a given dataset.
        Accepts parameters:
            page. It will strip the results to `global_config.get_page_size()` elements per page.
            elements. It will retrieve the info from the specified array of elements IDs rather than the page.
            options. result's find options.
        :return:
        """
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        args = self.get_parser.parse_args()

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        page = args['page']

        if 'options' in request.json:
            options = request.json['options']
        else:
            options = None

        elements_info = DatasetElementFactory(token, dataset).get_elements_info(page, options=options)

        result = [element.serialize() for element in elements_info]

        self.session.flush()
        self.session.clear()

        return result

    @control_access()
    def post(self, token_prefix, dataset_prefix):
        """
        Creates the element's header into the dataset.
        :return:
        """
        required_privileges = [
            Privileges.CREATE_DATASET,
            Privileges.EDIT_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        kwargs = self.post_parser.parse_args()

        if "content" not in kwargs and "http_ref" not in kwargs:
            kwargs["http_ref"] = "unknown"

        if 'tags' in request.json:
            kwargs['tags'] = request.json['tags']  # fast fix for split-bug of the tags.

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        element = DatasetElementFactory(token, dataset).create_element(**kwargs)

        self.session.flush()

        result=str(element._id)

        self.session.flush()
        self.session.clear()

        return result, 201


class DatasetElementsBundle(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("elements", type=list, required=True, location="json", help="List of IDs to retrieve.")
        self.post_parser = self.get_parser
        self.patch_parser = self.get_parser
        self.delete_parser = self.get_parser

        self.session = global_config.get_session()

    @control_access()
    def get(self, token_prefix, dataset_prefix):
        """
        Retrieves specific dataset elements from a given dataset.
        Accepts parameters:
            elements. It will retrieve the info from the specified array of elements IDs.
        :return:
        """
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        args = self.get_parser.parse_args()

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        # That json value is required, it is validated by the get_parser; so, it is ensured that key exists in the dict.
        elements_ids = request.json['elements']

        elements_info = DatasetElementFactory(token, dataset).get_specific_elements_info([ObjectId(x) for x in elements_ids])

        result = [element.serialize() for element in elements_info]

        self.session.flush()
        self.session.clear()

        return result

    @control_access()
    def delete(self, token_prefix, dataset_prefix):
        """
        """
        required_privileges = [
            Privileges.DESTROY_ELEMENTS,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        args = self.delete_parser.parse_args()

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        # That json value is required, it is validated by the get_parser; so, it is ensured that key exists in the dict.
        elements_ids = request.json['elements']

        DatasetElementFactory(token, dataset).destroy_elements([ObjectId(x) for x in elements_ids])

        self.session.flush()
        self.session.clear()

        return 200

    @control_access()
    def post(self, token_prefix, dataset_prefix):

        required_privileges = [
            Privileges.ADD_ELEMENTS,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        args = self.post_parser.parse_args()

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        # That json value is required, it is validated by the post_parser; so, it is ensured that key exists in the dict.
        elements_kwargs = request.json['elements']

        elements_created = DatasetElementFactory(token, dataset).create_elements(elements_kwargs)

        result = [element.serialize() for element in elements_created]

        self.session.flush()
        self.session.clear()

        return result

    @control_access()
    def patch(self, token_prefix, dataset_prefix):

        required_privileges = [
            Privileges.EDIT_ELEMENTS,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        args = self.post_parser.parse_args()

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        # That json value is required, it is validated by the delete_parser; so, it is ensured that key exists in the dict.
        elements_kwargs = request.json['elements']

        edited_elements = DatasetElementFactory(token, dataset).edit_elements({ObjectId(k): v for k, v in elements_kwargs.items()})
        result = [element.serialize() for element in edited_elements]

        self.session.flush()
        self.session.clear()

        return result


class DatasetElement(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.patch_parser = reqparse.RequestParser()
        self.session = global_config.get_session()
        arguments = {
            "title":
                {
                    "type": str,
                    "required": False,
                    "help": "Title for the element",
                    "location": "json"
                },
            "description":
                {
                    "type": str,
                    "required": False,
                    "help": "Description of the element.",
                    "location": "json"
                },
            "http_ref":
                {
                    "type": str,
                    "required": False,
                    "help": "HTTP link to the resource (if any).",
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
    def get(self, token_prefix, dataset_prefix, element_id):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        element = DatasetElementFactory(token, dataset).get_element_info(ObjectId(element_id))

        result = element.serialize()

        self.session.flush()
        self.session.clear()

        return result, 200

    @control_access()
    def patch(self, token_prefix, dataset_prefix, element_id):
        required_privileges = [
            Privileges.EDIT_ELEMENTS,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        kwargs = self.patch_parser.parse_args()

        if "tags" in request.json:
            kwargs['tags'] = request.json['tags']  # fast fix for split-bug of the tags.

        kwargs = {k:v for k, v in kwargs.items() if v is not None}

        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        DatasetElementFactory(token, dataset).edit_element(ObjectId(element_id), **kwargs)

        self.session.flush()
        self.session.clear()

        return "Done", 200

    @control_access()
    def delete(self, token_prefix, dataset_prefix, element_id):
        required_privileges = [
            Privileges.DESTROY_ELEMENTS,
            Privileges.ADMIN_DESTROY_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        DatasetElementFactory(token, dataset).destroy_element(ObjectId(element_id))

        self.session.flush()
        self.session.clear()

        return "Done", 200


class DatasetElementContent(TokenizedResource):
    def __init__(self):
        super().__init__()
        self.session = global_config.get_session()

    @control_access()
    def get(self, token_prefix, dataset_prefix, element_id):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        content = DatasetElementFactory(token, dataset).get_element_content(ObjectId(element_id))

        self.session.flush()
        self.session.clear()

        return send_file(BytesIO(content), mimetype="application/octet-stream")

    @control_access()
    def put(self, token_prefix, dataset_prefix, element_id):
        required_privileges = [
            Privileges.EDIT_ELEMENTS,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        content = request.stream.read()
        DatasetElementFactory(token, dataset).edit_element(ObjectId(element_id), content=content)

        self.session.flush()
        self.session.clear()

        return "Done", 200


class DatasetElementContentBundle(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.session = global_config.get_session()
        self.get_parser = reqparse.RequestParser()

        arguments = {
            "elements":
                {
                    "type": list,
                    "required": True,
                    "help": "List of element ids to retrieve content from (limited to {}).".format(global_config.get_page_size()),
                    "location": "json"
                },
        }

        for argument, kwargs in arguments.items():
            self.get_parser.add_argument(argument, **kwargs)

    @control_access()
    def get(self, token_prefix, dataset_prefix):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]
        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        kwargs = self.get_parser.parse_args()

        if 'elements' in request.json:
            kwargs['elements'] = request.json['elements']  # fast fix for split-bug of the tags.

        elements = kwargs['elements']

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        elements_content = DatasetElementFactory(token, dataset).get_elements_content([ObjectId(id) for id in elements])

        packet = PyZip(elements_content)

        self.session.flush()
        self.session.clear()

        return send_file(BytesIO(packet.to_bytes()), mimetype="application/octet-stream")

    @control_access()
    def put(self, token_prefix, dataset_prefix):
        required_privileges = [
            Privileges.EDIT_ELEMENTS,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        content = request.stream.read()
        packet = PyZip.from_bytes(content)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        crafted_request = {ObjectId(k): {'content': v} for k, v in packet.items()}
        DatasetElementFactory(token, dataset).edit_elements(crafted_request)

        self.session.flush()
        self.session.clear()

        return "Done", 200