#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import reqparse, abort, Resource
from mldatahub.config.config import now
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = 'Iván de Paz Centeno'



class TokenizedRequestParser(reqparse.RequestParser):

    def add_token_request(self):
        self.add_argument('_tok', type=str, required=False, help='Invalid token value')

    def parse_args(self, req=None, strict=False, required_any_token_privileges=None, required_all_token_privileges=None):
        args = super().parse_args(req=req, strict=strict)

        if required_any_token_privileges is None:
            required_any_token_privileges = []

        if required_all_token_privileges is None:
            required_all_token_privileges = []

        token_gui = args['_tok']

        if token_gui is None:
            abort(404)

        token = TokenDAO.query.get(token_gui=token_gui)

        if token is None:
            print("Token invalid {}".format(token_gui))
            abort(404)

        pbac_ok = all([bool(token.privileges & privilege) for privilege in required_all_token_privileges]) and \
                any([bool(token.privileges & privilege) for privilege in required_any_token_privileges])


        if not pbac_ok:
            abort(401)

        if token.end_date < now():
            abort(410)

        return args, token


class TokenizedResource(Resource):

    def __init__(self):
        super().__init__()
        self.token_parser = TokenizedRequestParser()
        self.token_parser.add_token_request()
