#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import reqparse
from mldatahub.api.tokenized_resource import TokenizedResource
from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges
from mldatahub.factory.dataset_factory import DatasetFactory
from mldatahub.factory.token_factory import TokenFactory

__author__ = 'Iván de Paz Centeno'


class Datasets(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("url_prefix", type=str, required=False, help="URL prefix to get tokens from.")

        self.post_parser = reqparse.RequestParser()
        self.session = global_config.get_session()
        arguments = {
            "dataset_url_prefixes":
                {
                    "type": list,
                    "required": True,
                    "help": "URL prefixes for the linked datasets to this token.",
                    "location": "json"
                },
            "description":
                {
                    "type": str,
                    "required": True,
                    "help": "Description for the dataset.",
                    "location": "json"
                },
            "max_dataset_count":
                {
                    "type": int,
                    "required": True,
                    "help": "Max number of datasets that it is allowed to create this token.",
                    "location": "json"
                },
            "max_dataset_size":
                {
                    "type": int,
                    "required": True,
                    "help": "Max size for a dataset created by this token",
                    "location": "json"
                },
            "end_date":
                {
                    "type": str,
                    "help": "End date for this token, in format {}".format(now()),
                    "location": "json"
                },
            "privileges":
                {
                    "type": int,
                    "required": True,
                    "help": "Privileges integer for this token",
                    "location": "json"
                },
            "token_gui":
                {
                    "type": str,
                    "help": "Token GUI, if not specified a new one is generated.",
                    "location": "json"
                },
            "url_prefix":
                {
                    "type": str,
                    "help": "Token URL prefix.",
                    "location": "json"
                },

        }

        for argument, kwargs in arguments.items():
            self.post_parser.add_argument(argument, **kwargs)

    def get(self, token_url_prefix, dataset_url_prefix):
        required_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        args = self.get_parser.parse_args()
        print(args)
        tokens = DatasetFactory(token).get_dataset(**args)

        return [t.serialize() for t in tokens], 201

        full_dataset_url_prefix = "{}/{}".format(token_url_prefix, dataset_url_prefix)