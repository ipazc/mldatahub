#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask_restful import reqparse, abort
from mldatahub.api.tokenized_resource import TokenizedResource

from mldatahub.config.config import now, global_config
from mldatahub.config.privileges import Privileges
from mldatahub.factory.token_factory import TokenFactory
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = "Iván de Paz Centeno"


class Tokens(TokenizedResource):
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

    def get(self):
        required_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        args = self.get_parser.parse_args()
        print(args)
        tokens = TokenFactory(token).get_tokens(**args)

        return [t.serialize() for t in tokens], 201

    def post(self):
        required_privileges = [
            Privileges.ADMIN_CREATE_TOKEN,
            Privileges.USER_CREATE_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_all_token_privileges=required_privileges)
        args = self.post_parser.parse_args()

        created_token = TokenFactory(token).create_token(**{k:v for k,v in args.items() if v is not None})

        return created_token.serialize(), 201


class Token(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.delete_parser = self.get_parser
        self.post_parser = reqparse.RequestParser()
        self.session = global_config.get_session()

        arguments= {
            "token_gui":
            {
                "type": str,
                "required": True,
                "help": "Global Unique Identifier of the token is required.",
                "location": "json"
            }
        }

        for argument, kwargs in arguments.items():
            self.get_parser.add_argument(argument, **kwargs)

        arguments = {
            "description":
            {
                "type": str,
                "help": "Description for the dataset.",
                "location": "json"
            },
            "max_dataset_count":
            {
                "type": int,
                "help": "Max number of datasets that it is allowed to create this token.",
                "location": "json"
            },
            "max_dataset_size":
            {
                "type": int,
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

    def get(self, token_id):

        required_any_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.RO_WATCH_DATASET
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)
        args = self.get_parser.parse_args()

        view_token = TokenFactory(token).get_token(token_id)

        return view_token.serialize(), 201

    def post(self, token_id):
        required_any_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)
        args = self.get_parser.parse_args()

        if token_id != token.token_gui:
            abort(401)

        edited_token = TokenFactory(token).edit_token(token_id, **args)

        return edited_token.serialize(), 201
