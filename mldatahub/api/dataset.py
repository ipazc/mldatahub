#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import reqparse
from mldatahub.api.tokenized_resource import TokenizedResource
from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges
from mldatahub.factory.dataset_factory import DatasetFactory
from mldatahub.factory.token_factory import TokenFactory
from mldatahub.odm.dataset_dao import DatasetDAO

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

    def get(self):
        """
        Retrieves all the datasets associated to the current token.
        :return:
        """
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        return [dataset.serialize() for dataset in token.datasets]

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

        dataset = DatasetFactory(token).create_dataset(**kwargs)

        token = TokenFactory(token).link_datasets(token.token_gui, [dataset])

        self.session.flush()

        return "Done", 201

class Dataset(TokenizedResource):

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
                    "help": "URL prefix for this dataset. Characters \"{}\" not allowed".format(
                        DatasetFactory.illegal_chars),
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

    def get(self, token_url_prefix, dataset_url_prefix):
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_url_prefix, dataset_url_prefix)

        dataset = DatasetFactory(token).get_dataset(full_dataset_url_prefix)

        return dataset.serialize(), 200

    def patch(self, token_url_prefix, dataset_url_prefix):
        required_privileges = [
            Privileges.EDIT_DATASET,
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_url_prefix, dataset_url_prefix)

        kwargs = self.post_parser.parse_args()

        DatasetFactory(token).edit_dataset(full_dataset_url_prefix, **kwargs)

        return "Done", 200

    def delete(self, token_url_prefix, dataset_url_prefix):
        required_privileges = [
            Privileges.DESTROY_DATASET,
            Privileges.ADMIN_DESTROY_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)
        full_dataset_url_prefix = "{}/{}".format(token_url_prefix, dataset_url_prefix)

        DatasetFactory(token).destroy_dataset(full_dataset_url_prefix)

        return "Done", 200
