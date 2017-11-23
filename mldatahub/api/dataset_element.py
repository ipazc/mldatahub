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
from threading import Lock

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


def _get_elements_real_id(elements_ids: list, dataset_element_factory: DatasetElementFactory):
    """
    Sometimes the client has in cache an element whose ID has already changed. This happens in the case of a forked
    element, for example when a value from the original element changed.

    _get_elements_real_id translates old IDs into new IDs if available, for each of the iDs in the list.

    Note that this method guarantees that all the IDs, if elements exist for the current dataset,
    will be available in the DB.

    :param elements_ids: List of elements ids (wrapped in ObjectId !!)
    :param dataset_element_factory: factory for managing dataset elements.
    :return: list of elements (with same order as original) with newest corresponding IDs.
    """

    # Sometimes the Id of the elements changes, but we can get a conversion dictionary to solve the problem.
    translation_dict = dataset_element_factory.discover_real_id(elements_ids)

    new_elements_ids = [element_id if element_id not in translation_dict else translation_dict[element_id] for element_id in elements_ids]

    return new_elements_ids


class DatasetElements(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("page", type=int, required=False, help="Page number to retrieve.", default=0)
        self.get_parser.add_argument("page-size", type=int, required=False, help="Size of the page to retrieve.", default=global_config.get_page_size())
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

        try:
            page_size = int(args['page-size'])
        except ValueError as ex:
            page_size = None
            abort(400, message="The page-size must be an integer.")

        page = args['page']

        if 'options' in request.json:
            options = request.json['options']
        else:
            options = None

        elements_info = DatasetElementFactory(token, dataset).get_elements_info(page, options=options, page_size=page_size)

        result = [element.serialize() for element in elements_info]

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

        result = str(element._id)

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        # That json value is required, it is validated by the get_parser; so, it is ensured that key exists in the dict.
        elements_ids = [ObjectId(element_id) for element_id in request.json['elements']]

        new_elements_ids = _get_elements_real_id(elements_ids, dataset_element_factory)

        try:
            elements_info = dataset_element_factory.get_specific_elements_info(new_elements_ids)
        except KeyError as e:
            elements_info = []
            abort(404, message=str(e)[1:-1])

        result = [element.serialize() for element in elements_info]

        # The client has to check for a "previous_id" field in the result.
        # In case he finds it, he must update his index table to change "previous_id" to "_id".

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        # That json value is required, it is validated by the get_parser; so, it is ensured that key exists in the dict.
        elements_ids = [ObjectId(element_id) for element_id in request.json['elements']]

        new_elements_ids = _get_elements_real_id(elements_ids, dataset_element_factory)

        # Note that if no elements are provided (length is 0) then we purge the whole dataset. It is the clear()
        # behavior.

        dataset_element_factory.destroy_elements(new_elements_ids)

        self.session.flush()

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

        self.session.flush()

        result = [element.serialize() for element in elements_created]

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        # That json value is required, it is validated by the delete_parser; so, it is ensured that key exists in the dict.
        elements_kwargs = request.json['elements']
        elements_kwargs_preprocessed = {ObjectId(k): v for k, v in elements_kwargs.items()}

        # There might be some elements with an old ID. Let's find their new ID
        elements_ids = list(elements_kwargs_preprocessed.keys())
        translation_dict = dataset_element_factory.discover_real_id(elements_ids)

        # Now rebuild the elements with the new ids
        elements_kwargs_postprocessed = {(k if k not in translation_dict else translation_dict[k]): v for k, v in elements_kwargs_preprocessed}

        edited_elements = dataset_element_factory.edit_elements(elements_kwargs_postprocessed)

        self.session.flush()

        result = [element.serialize() for element in edited_elements]

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        wrapped_element_id = ObjectId(element_id)

        # Let's translate from a potential old id (like element_id) to the newest ID of this element.
        # Most of the times, real_element_id is going to be the same as element_id; however there are
        # certain cases where not: after a modification of a forked element.
        real_element_id = _get_elements_real_id([wrapped_element_id], dataset_element_factory)[0]

        element = dataset_element_factory.get_element_info(real_element_id)

        result = element.serialize()

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        wrapped_element_id = ObjectId(element_id)

        # Let's translate from a potential old id (like element_id) to the newest ID of this element.
        # Most of the times, real_element_id is going to be the same as element_id; however there are
        # certain cases where not: after a modification of a forked element.
        real_element_id = _get_elements_real_id([wrapped_element_id], dataset_element_factory)[0]

        dataset_element_factory.edit_element(real_element_id, **kwargs)

        self.session.flush()

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        wrapped_element_id = ObjectId(element_id)

        # Let's translate from a potential old id (like element_id) to the newest ID of this element.
        # Most of the times, real_element_id is going to be the same as element_id; however there are
        # certain cases where not: after a modification of a forked element.
        real_element_id = _get_elements_real_id([wrapped_element_id], dataset_element_factory)[0]

        dataset_element_factory.destroy_element(real_element_id)

        self.session.flush()

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        wrapped_element_id = ObjectId(element_id)

        # Let's translate from a potential old id (like element_id) to the newest ID of this element.
        # Most of the times, real_element_id is going to be the same as element_id; however there are
        # certain cases where not: after a modification of a forked element.
        real_element_id = _get_elements_real_id([wrapped_element_id], dataset_element_factory)[0]

        content = dataset_element_factory.get_element_content(real_element_id)

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

        dataset_element_factory = DatasetElementFactory(token, dataset)

        wrapped_element_id = ObjectId(element_id)

        # Let's translate from a potential old id (like element_id) to the newest ID of this element.
        # Most of the times, real_element_id is going to be the same as element_id; however there are
        # certain cases where not: after a modification of a forked element.
        real_element_id = _get_elements_real_id([wrapped_element_id], dataset_element_factory)[0]

        content = request.stream.read()
        dataset_element_factory.edit_element(real_element_id, content=content)

        self.session.flush()

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

        elements_ids = [ObjectId(id) for id in kwargs['elements']]

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        dataset_element_factory = DatasetElementFactory(token, dataset)

        # Let's translate from a potential old id (like element_id) to the newest ID of this element.
        # Most of the times, real_element_id is going to be the same as element_id; however there are
        # certain cases where not: after a modification of a forked element.
        real_elements_ids = _get_elements_real_id(elements_ids, dataset_element_factory)

        elements_content = dataset_element_factory.get_elements_content(real_elements_ids)

        packet = PyZip(elements_content)

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
        try:
            packet = PyZip().from_bytes(content)
        except Exception:
            packet = None
            abort(422, message="The content pushed is not readable. Ensure that your version of DHUB is the latest one.")

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        dataset_element_factory = DatasetElementFactory(token, dataset)

        elements_ids = list(packet.keys())
        translation_dict = dataset_element_factory.discover_real_id(elements_ids)

        # Now rebuild the elements with the new ids
        elements_kwargs_postprocessed = {(k if k not in translation_dict else translation_dict[k]): {'content': v} for k, v in packet.items()}

        dataset_element_factory.edit_elements(elements_kwargs_postprocessed)

        self.session.flush()

        return "Done", 200
