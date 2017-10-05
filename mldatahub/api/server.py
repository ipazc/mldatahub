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

__author__ = "Iván de Paz Centeno"


from flask import request
from mldatahub import __version__
from mldatahub.api.tokenized_resource import TokenizedResource, control_access
from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges
from mldatahub.factory.dataset_factory import DatasetFactory
from mldatahub.factory.token_factory import TokenFactory


class Server(TokenizedResource):

    def __init__(self):
        super().__init__()
        self.session = global_config.get_session()

    @control_access()
    def get(self):
        """
        Retrieves all the information of the server.
        :return:
        """
        required_privileges = [
            Privileges.RO_WATCH_DATASET,
        ]

        _, token = self.token_parser.parse_args(required_any_token_privileges=required_privileges)

        response = {
            'Server': 'mldatahub {}'.format(__version__),
            'Page-Size': global_config.get_page_size()
        }

        return response
