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

    def _compatible_privileges(self, privileges_src, privileges_dst):
        """
        Checks whether privileges_dst are a subset of privileges_src
        :param privileges_src:
        :param privileges_dst:
        :return:
        """
        privilege = 0x01

        for i in str(bin(privileges_dst))[2:]:
            if bool(privileges_dst & privilege) and not bool(privileges_src & privilege):
                return False  # Found a privilege not owned by the token owner.
            privilege <<= 1
        return True

    def create_token(self, *args, **kwargs):
        kwargs = dict(kwargs)

        if "url_prefix" in kwargs:
            url_prefix = kwargs["url_prefix"]
            del kwargs["url_prefix"]
        else:
            url_prefix = args[3]

        if "privileges" in kwargs and not bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN):
            privileges = kwargs["privileges"]

            # We need to check privileges creation. An exploit would be to allow creation of tokens with highest privileges
            # from non-trusted tokens.
            if not self._compatible_privileges(self.token.privileges, privileges):
                abort(409)

        # Security: user can only create tokens within his url_prefix
        if url_prefix != self.token.url_prefix and not bool(self.token.privileges & Privileges.ADMIN_CREATE_TOKEN):
            abort(401)

        kwargs["url_prefix"] = url_prefix

        token = TokenDAO(*args, **kwargs)
        self.session.flush()

        return token

    def get_tokens(self, **args):
        # Important check: if not ADMIN_EDIT_TOKEN then it can only be the same token.
        # This is a security issue: otherwise, any user can see other's tokens data if known.
        can_see_others = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)
        can_see_all_inner_tokens = bool(self.token.privileges & Privileges.USER_EDIT_TOKEN)

        if not can_see_all_inner_tokens and not can_see_others:  # Exploit fix: a token should not be able to watch other tokens unless it is admin.
            abort(401)

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

        tokens = [t for t in TokenDAO.query.find(query)]

        return tokens

    def get_token(self, token_gui):

        # Important check: if not ADMIN_EDIT_TOKEN then it can only be the same token.
        # This is a security issue: otherwise, any user can see other's tokens data if known.
        can_see_others = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)

        if token_gui != self.token.token_gui and not can_see_others:
            abort(401)

        view_token = TokenDAO.query.get(token_gui=token_gui)

        if view_token is None:
            abort(400)

        return view_token

    def edit_token(self, token_gui, **kwargs):
        # Important check: if not ADMIN_EDIT_TOKEN then it can only be token from the same user.
        # This is a security issue: otherwise, any user can edit other's tokens data if known.
        can_edit_others = bool(self.token.privileges & Privileges.ADMIN_EDIT_TOKEN)
        can_edit_all_inner_tokens = bool(self.token.privileges & Privileges.USER_EDIT_TOKEN)

        edit_token = TokenDAO.query.get(token_gui=token_gui)

        if edit_token is None:
            abort(400)

        if not can_edit_others:
            if edit_token.url_prefix != self.token.url_prefix:
                abort(401)

            if not can_edit_all_inner_tokens:
                abort(401)

        if 'url_prefix' in kwargs and not can_edit_others:
            # URL prefixes can't be changed once created unless it is done by admin.
            abort(401)

        try:
            for token_arg, value in kwargs.items():

                if value is not None:
                    edit_token[token_arg] = value
        except Exception as ex:
            abort(400)

        self.session.flush()

        return edit_token