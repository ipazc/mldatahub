#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import request
from flask_restful import reqparse, abort, Resource
from mldatahub.config.config import now, global_config
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.restapi_dao import RestAPIDAO
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


MAX_ACCESS_TIMES = global_config.get_max_access_times()
ACCESS_RESET_TIME = global_config.get_access_reset_time()
session = global_config.get_session()

def control_access():
    def func_wrap(func):
        def args_wrap(*args, **kwargs):
            remote_ip = request.remote_addr

            ip_control = RestAPIDAO.query.get(ip=remote_ip)

            if ip_control is None:
                ip_control = RestAPIDAO(ip=remote_ip, last_access=now(), num_accesses=0)

            if (now() - ip_control.last_access).total_seconds() > ACCESS_RESET_TIME:
                ip_control.last_access = now()
                ip_control.num_accesses = 0

            if ip_control.num_accesses > MAX_ACCESS_TIMES:
                abort(429)

            ip_control.num_accesses += 1

            session.flush()

            return func(*args, **kwargs)
        return args_wrap
    return func_wrap
