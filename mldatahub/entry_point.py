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

import signal
import sys
import mldatahub
from mldatahub.config.config import global_config
import argparse

__author__ = "Iván de Paz Centeno"


def main():
    parser = argparse.ArgumentParser(description="ML Data Hub.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--version", action="store_true")
    group.add_argument("-d", "--deploy", action="store_true", help="Deploys the ML Data Hub using the /etc/mldatahub/config.json options.")
    group.add_argument("-p", "--purge-database", action="store_true", dest="purge_database", help="Purges the database and leaves it in a clean state.")
    group.add_argument("-c", "--create-token", action="store_true", dest="create_token", help="Creates a standard privileged token (create datasets).")
    group.add_argument("-g", "--garbage-collector", action="store_true", dest="garbage_collector", help="Instances the Garbage Collector for freed files.")

    if "--create-token" in sys.argv:
        index = sys.argv.index("--create-token")
    elif "-c" in sys.argv:
        index = sys.argv.index("-c")
    else:
        index = -1

    if index > -1:
        args_to_parse = sys.argv[1:index] + ["--create-token"]
    else:
        args_to_parse = sys.argv[1:]

    args = parser.parse_args(args_to_parse)

    if args.version:
        print(mldatahub.__version__)
    elif args.deploy:
        deploy()
    elif args.purge_database:
        purge_database()
    elif args.create_token:
        create_token(sys.argv[index+1:])
    elif args.garbage_collector:
        deploy_gc()
    else:
        parser.print_help()


def purge_database():
    from mldatahub.odm.dataset_dao import DatasetDAO, DatasetCommentDAO, DatasetElementDAO, DatasetElementCommentDAO
    from mldatahub.odm.restapi_dao import RestAPIDAO
    from mldatahub.odm.file_dao import FileDAO
    from mldatahub.odm.token_dao import TokenDAO
    TokenDAO.query.remove()
    print("Purging tokens...")
    DatasetDAO.query.remove()
    print("Purging datasets...")
    DatasetCommentDAO.query.remove()
    print("Purging dataset comments...")
    DatasetElementDAO.query.remove()
    print("Purging datasets' elements...")
    DatasetElementCommentDAO.query.remove()
    print("Purging datasets' elements comments...")
    FileDAO.query.remove()
    print("Purging files...")
    RestAPIDAO.query.remove()
    print("Purging accesses records...")
    print("Finished.")


def create_token(args):
    from mldatahub.config.privileges import Privileges

    privileges = Privileges.CREATE_DATASET + Privileges.EDIT_DATASET + Privileges.DESTROY_DATASET + \
                 Privileges.ADD_ELEMENTS + Privileges.EDIT_ELEMENTS + Privileges.DESTROY_ELEMENTS + \
                 Privileges.RO_WATCH_DATASET + Privileges.USER_EDIT_TOKEN

    parser = argparse.ArgumentParser(description="ML Data Hub [--create-token]")
    parser.add_argument("namespace", type=str, help="Namespace for the token")
    parser.add_argument("description", type=str, help="Description of the token")
    parser.add_argument("--maxds", type=int, help="Maximum number of datasets for this token", default=50)
    parser.add_argument("--maxl", type=int, help="Maximum number of elements for this token", default=10000000)
    parser.add_argument("--privileges", type=int, help="Privileges", default=privileges)

    args = parser.parse_args(args)

    illegal_chars = "/*;:,.ç´`+Ç¨^><¿?'¡¿!\"·$%&()@~¬"
    if any([i in args.namespace for i in illegal_chars]):
        print("[ERROR] The namespace can't hold any of the following chars:\n\"{}\"".format(illegal_chars))
        exit(-1)

    from mldatahub.odm.dataset_dao import DatasetDAO
    from mldatahub.odm.token_dao import TokenDAO
    token = TokenDAO(args.description, args.maxds, args.maxl, args.namespace, privileges=args.privileges)

    global_config.get_session().flush()
    print("*****************************************")
    print("Characteristics of token")
    print("*****************************************")
    print("NAMESPACE: {}".format(args.namespace))
    print("DESCRIPTION: {}".format(args.description))
    print("MAX DATASETS: {}".format(args.maxds))
    print("MAX ELEMENTS PER DATASET: {}".format(args.maxl))
    print("PRIVILEGES: {}".format(privileges))
    print("*****************************************")
    print("TOKEN:", token.token_gui)

def build_app():
    from flask import Flask
    from flask_restful import Api
    from mldatahub.api.dataset import Datasets, Dataset, DatasetForker, DatasetSize
    from mldatahub.api.dataset_element import DatasetElements, DatasetElement, DatasetElementContent, \
        DatasetElementsBundle, DatasetElementContentBundle
    from mldatahub.api.server import Server
    from mldatahub.api.token import Tokens, Token, TokenLinker

    app = Flask(__name__)
    api = Api(app)

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
    api.add_resource(DatasetSize, '/datasets/<token_prefix>/<dataset_prefix>/size')

    return app

def deploy():
    app = build_app()
    global_config.print_config()
    app.run(host=global_config.get_host(), port=global_config.get_port(), debug=False, threaded=True)

def deploy_gc():
    def signal_handler(signal, frame):
        print('CTRL+C detected. Exiting...')

    from mldatahub.observer.garbage_collector import GarbageCollector
    garbage_collector = GarbageCollector()
    print("Collecting garbage... hit CTRL+C to close this safely.\n Closing safely this process is **highly recommended**.")
    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()
    garbage_collector.stop()

if __name__ == '__main__':
    main()
