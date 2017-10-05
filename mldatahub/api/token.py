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

from flask_restful import reqparse, abort
from mldatahub.api.tokenized_resource import TokenizedResource, control_access

from mldatahub.config.config import now, global_config
from mldatahub.config.privileges import Privileges
from mldatahub.factory.token_factory import TokenFactory

__author__ = "Iván de Paz Centeno"


class Tokens(TokenizedResource):
    def __init__(self):
        super().__init__()
        self.get_parser = reqparse.RequestParser()
        self.get_parser.add_argument("url_prefix", type=str, required=False, help="URL prefix to get tokens from.")

        self.post_parser = reqparse.RequestParser()
        self.session = global_config.get_session()
        arguments = {
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
            "url_prefix":
                {
                    "type": str,
                    "help": "Token URL prefix.",
                    "location": "json"
                },

        }

        for argument, kwargs in arguments.items():
            self.post_parser.add_argument(argument, **kwargs)

    @control_access()
    def get(self):
        required_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        args = self.get_parser.parse_args()

        tokens = TokenFactory(token).get_tokens(**args)

        result = [t.serialize() for t in tokens], 201

        self.session.flush()
        self.session.clear()

        return result

    @control_access()
    def post(self):
        required_privileges = [
            Privileges.ADMIN_CREATE_TOKEN,
            Privileges.USER_CREATE_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_all_token_privileges=required_privileges)
        args = self.post_parser.parse_args()

        created_token = TokenFactory(token).create_token(**{k:v for k,v in args.items() if v is not None})

        result = created_token.serialize(), 201

        self.session.flush()
        self.session.clear()

        return result


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
                "help": "Description for the token.",
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

    @control_access()
    def get(self, token_id):

        required_any_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.RO_WATCH_DATASET
        ]
        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)

        view_token = TokenFactory(token).get_token(token_id)

        result = view_token.serialize(), 201

        self.session.flush()
        self.session.clear()

        return result

    @control_access()
    def patch(self, token_id):
        required_any_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)
        args = self.get_parser.parse_args()

        if token_id != token.token_gui:
            abort(401)

        edited_token = TokenFactory(token).edit_token(token_id, **args)
        result = edited_token.serialize(), 201

        self.session.flush()
        self.session.clear()

        return result

    @control_access()
    def delete(self, token_id):
        required_any_privileges = [
            Privileges.ADMIN_DESTROY_TOKEN,
            Privileges.USER_DESTROY_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)
        args = self.get_parser.parse_args()

        if token_id != token.token_gui:
            abort(401)

        TokenFactory(token).delete_token(token_id)

        self.session.flush()
        self.session.clear()

        return "Done", 201


class TokenLinker(TokenizedResource):
    """
    API to link/unlink a token to a dataset
    """

    def __init__(self):
        super().__init__()
        self.session = global_config.get_session()

    @control_access()
    def put(self, token_id, token_prefix, dataset_prefix):
        required_any_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)
        url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        token = TokenFactory(token).link_datasets(token_id, [url_prefix])

        self.session.flush()
        self.session.clear()

        return "Done", 200

    @control_access()
    def delete(self, token_id, token_prefix, dataset_prefix):
        required_any_privileges = [
            Privileges.ADMIN_EDIT_TOKEN,
            Privileges.USER_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_any_privileges)
        url_prefix = "{}/{}".format(token_prefix, dataset_prefix)

        token = TokenFactory(token).unlink_datasets(token_id, [url_prefix])

        self.session.flush()
        self.session.clear()

        return "Done", 200
