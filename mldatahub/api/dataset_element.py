#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from io import BytesIO
from flask import send_file, request
from flask_restful import reqparse
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
        self.get_parser.add_argument("page", type=int, required=False, help="Page number to retrieve.")
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
        Accepts a parameter: page. It will strip the results to 20 elements per page.
        :return:
        """
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        elements_info = DatasetElementFactory(token, dataset).get_elements_info()

        return [element.serialize() for element in elements_info]

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

        full_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_prefix)

        element = DatasetElementFactory(token, dataset).create_element(**kwargs)

        self.session.flush()

        return element._id, 201


class DatasetElement(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("url_prefix", type=str, required=False, help="URL prefix to get tokens from.")
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
    def head(self, token_url_prefix, dataset_url_prefix, element_id):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_url_prefix, dataset_url_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        element = DatasetElementFactory(token, dataset).get_element_info(element_id)

        return element.serialize(), 200

    @control_access()
    def get(self, token_prefix, dataset_prefix, element_id):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        content = DatasetElementFactory(token, dataset).get_element_content(element_id)

        with BytesIO(content) as fp:
            result = send_file(fp)

        return result

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

        DatasetElementFactory(token, dataset).edit_element(element_id, **kwargs)

        return "Done", 200

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

        DatasetElementFactory(token, dataset).edit_element(element_id, content=content)

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

        DatasetElementFactory(token, dataset).destroy_element(element_id)

        return "Done", 200
