#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import request
from flask_restful import reqparse, Resource, abort

from mldatahub.config.config import now, global_config
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = "Iván de Paz Centeno"


class TokenizedRequestParser(reqparse.RequestParser):

    def add_token_request(self):
        self.add_argument('_tok', type=str, required=False, help='Invalid token value')

    def parse_args(self, req=None, strict=False, required_token_privileges=None):
        args = super().parse_args(req=req, strict=strict)

        if required_token_privileges is None:
            required_token_privileges = []

        token_gui = args['_tok']

        if token_gui is None:
            abort(404)

        token = TokenDAO.query.get(token_gui=token_gui)

        if token is None:
            print("Token invalid {}".format(token_gui))
            abort(404)

        PBAC_ok = all([bool(token.privileges & privilege) for privilege in required_token_privileges])

        if not PBAC_ok:
            print("Illegal access")
            abort(401)

        return args, token

class Tokens(Resource):
    def __init__(self):
        self.token_parser = TokenizedRequestParser()
        self.token_parser.add_token_request()
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
            }
        }

        for argument, kwargs in arguments.items():
            self.post_parser.add_argument(argument, **kwargs)

    def get(self):
        required_privileges = [
            Privileges.ADMIN_EDIT_TOKEN
        ]

        args, token = self.token_parser.parse_args(required_token_privileges=required_privileges)

        tokens = TokenDAO.query.find()

        return [t.serialize() for t in tokens], 201

    def post(self):
        required_privileges = [
            Privileges.ADMIN_EDIT_TOKEN
        ]

        _, token = self.token_parser.parse_args(required_token_privileges=required_privileges)
        args = self.post_parser.parse_args()

        datasets = [DatasetDAO.query.get(url_prefix=url_prefix) for url_prefix in request.json['dataset_url_prefixes']]

        index = -1
        for d in datasets:
            index += 1
            if d is None:
                abort("Dataset {} doesn't exist!".format(request.json['dataset_url_prefixes'][index]))


        args['dataset_url_prefixes'] = request.json['dataset_url_prefixes']

        token_args = {arg_k: arg_v for arg_k, arg_v in args.items() if arg_v is not None and arg_k != 'dataset_url_prefixes'}
        new_token = TokenDAO(**token_args)
        new_token.link_datasets(datasets)

        self.session.flush()

        return new_token.serialize(), 201

    """def get(self):
        token, author, input_query = self._get_request(allow_public=True)
        return Dataset.m.find({'author': author})

    def post(self):
        token, author, input_query = self._get_request()

        input_query['author'] = author
        dataset = Dataset(**input_query)
        dataset.m.save()

        return "Done", 201

    def _get_request(self, allow_public=False):
        input_query = request.get_json()

        token = input_query['tokens']
        del input_query['tokens']

        #author = get_token_author(tokens)

        if author is None and not allow_public:
            raise Exception("Forbidden")

        return token, author, input_query
    """
