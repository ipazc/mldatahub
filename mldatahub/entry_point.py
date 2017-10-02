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

from flask import Flask
from flask_restful import Api
from mldatahub.api.dataset import Datasets, Dataset, DatasetForker
from mldatahub.api.dataset_element import DatasetElements, DatasetElement, DatasetElementContent, DatasetElementsBundle, \
    DatasetElementContentBundle
from mldatahub.api.server import Server
from mldatahub.api.token import Tokens, Token, TokenLinker
from mldatahub.config.config import global_config
from mldatahub.observer.garbage_collector import GarbageCollector

__author__ = "Iván de Paz Centeno"


def main():

    app = Flask(__name__)
    api = Api(app)

    garbage_collector = GarbageCollector()
    api.add_resource(Server, '/server')
    api.add_resource(Tokens, '/tokens')
    api.add_resource(Token, '/tokens/<token_id>')
    api.add_resource(TokenLinker, '/tokens/<token_id>/link/<token_prefix>/<dataset_prefix>')
    api.add_resource(Datasets, '/datasets')
    api.add_resource(Dataset, '/datasets/<token_prefix>/<dataset_prefix>')
    api.add_resource(DatasetForker, '/datasets/<token_prefix>/<dataset_prefix>/fork/<dest_token_gui>')
    api.add_resource(DatasetElements, '/datasets/<token_prefix>/<dataset_prefix>/elements')
    api.add_resource(DatasetElementsBundle, '/datasets/<token_prefix>/<dataset_prefix>/elements/bundle')
    api.add_resource(DatasetElement, '/datasets/<token_prefix>/<dataset_prefix>/elements/<element_id>')
    api.add_resource(DatasetElementContent, '/datasets/<token_prefix>/<dataset_prefix>/elements/<element_id>/content')
    api.add_resource(DatasetElementContentBundle, '/datasets/<token_prefix>/<dataset_prefix>/elements/content')
    app.run(host=global_config.get_host(), port=global_config.get_port(), debug=False, threaded=True)
    garbage_collector.stop()
    global_config.get_local_storage().close()

if __name__ == '__main__':
    main()
    print("Exiting application... (it may take a few seconds to clean up everything)")
