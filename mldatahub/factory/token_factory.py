#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import abort
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = 'Iván de Paz Centeno'


class TokenFactory(object):

    def __init__(self, token):
        self.token = token
        self.session = global_config.get_session()

    def create_token(self, *args, **kwargs):

        if "url_prefix" in kwargs:
            url_prefix = kwargs["url_prefix"]
            del kwargs["url_prefix"]
        else:
            url_prefix = args[3]

        # Security: user can only create tokens within his url_prefix
        if url_prefix != self.token.url_prefix and not bool(self.token.privilege & Privileges.ADMIN_CREATE_TOKEN):
            abort(401)

        token = TokenDAO(*args, **kwargs)
        self.session.flush()

        return token

    def get_tokens(self, **args):
        # Important check: if not ADMIN_EDIT_TOKEN then it can only be the same token.
        # This is a security issue: otherwise, any user can see other's tokens data if known.
        can_see_others = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        try:
            prefix = args['url_prefix']
        except KeyError as ex:
            prefix = None

        if prefix is None and not can_see_others:
            prefix = self.token.url_prefix

        if prefix != self.token.url_prefix and not can_see_others:
            abort(401)

        query = {}

        if prefix is not None:
            query["url_prefix"] = prefix

        tokens = TokenDAO.query.find(query)

        return tokens

    def get_token(self, token_gui):

        # Important check: if not ADMIN_EDIT_TOKEN then it can only be the same token.
        # This is a security issue: otherwise, any user can see other's tokens data if known.
        can_see_others = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if token_gui != self.token.token_gui and not can_see_others:
            abort(401)

        view_token = TokenDAO.query.get(token_gui=token_gui)

        return view_token


    def edit_token(self, *args, **kwargs):
        pass