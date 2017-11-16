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

from bson import ObjectId
from mldatahub.config.config import global_config
from mldatahub.odm.file_dao import FileDAO, FileContentDAO
from mldatahub.storage.generic_storage import GenericStorage, File
import hashlib

__author__ = 'Iván de Paz Centeno'


FILE_SIZE_LIMIT = global_config.get_file_size_limit()


class MongoStorage(GenericStorage):
    def __init__(self):
        self.session = global_config.get_session()

    def __get_hashed_sha256_file(self, sha256_hash:str):
        return FileDAO.query.get(sha256=sha256_hash)

    def __get_hashed_sha256_files(self, sha256_hashes:list):
        return FileDAO.query.find({'sha256': {'$in': sha256_hashes}})

    def put_file_content(self, content_bytes:bytes) -> ObjectId:
        length = len(content_bytes)

        if length >= FILE_SIZE_LIMIT:
            raise Exception("File size limit of {} Bytes exceeded".format(FILE_SIZE_LIMIT))

        # Let's optimize the storage now
        sha256 = hashlib.sha256(content_bytes).hexdigest()
        file = self.__get_hashed_sha256_file(sha256)

        if file is None:
            file = FileContentDAO(content=content_bytes, size=length, sha256=sha256)
            self.session.flush()

        return file._id

    def put_files_contents(self, content_bytes_list:list) -> list:
        length_exceeded = any([len(content_bytes) >= FILE_SIZE_LIMIT for content_bytes in content_bytes_list])

        if length_exceeded:
            raise Exception("File size limit of {} Bytes exceeded".format(FILE_SIZE_LIMIT))

        sha256s = {hashlib.sha256(content_bytes).hexdigest(): content_bytes for content_bytes in content_bytes_list}

        hashed_files = {file.sha256: file for file in self.__get_hashed_sha256_files(list(sha256s.keys()))}

        unhashed_content = {hash: sha256s[hash] for hash in sha256s if hash not in hashed_files}

        files = [FileContentDAO(content=content_bytes, size=len(content_bytes), sha256=hash) for hash, content_bytes in unhashed_content.items()]
        self.session.flush()

        merged = list(hashed_files.values()) + files

        # We need to ensure the order of the output. It must be the same order as the input
        hash_by_content = {content_bytes: hash for hash, content_bytes in sha256s.items()}
        file_by_hash = {file.sha256: file for file in merged}

        return [file_by_hash[hash_by_content[content]]._id for content in content_bytes_list]

    def get_file(self, file_id:ObjectId):
        file = FileContentDAO.query.get(_id=file_id)
        return File(file._id, file.content, file.size)

    def get_files(self, files_ids:list):
        # We need to ensure the order of the output. It must be the same order as the input
        file_by_id = {file._id: File(file._id, file.content, file.size) for file in FileContentDAO.query.find({'_id': {'$in' : files_ids}})}

        return [file_by_id[id] for id in files_ids]

    def delete_file(self, file_id:ObjectId):
        file = self.get_file(file_id)

        if file is None:
            raise FileNotFoundError()

        FileDAO.query.remove({'_id': file.id})

    def delete_files(self, files_ids:list):
        FileDAO.query.remove({'_id': {'$in': files_ids}})

    def __contains__(self, item):
        return FileDAO.query.get(_id=item) is not None

    def __iter__(self):
        files_list = FileDAO.query.find()

        for f in files_list:
            yield f

    def size(self):
        return sum(f.size for f in self)

    def get_files_size(self, files_ids:list):
        files = FileDAO.query.find({'_id': {'$in' : files_ids}})
        return sum(f.size for f in files)

    def __len__(self):
        return FileDAO.query.find().count()

    def delete(self):
        FileDAO.query.remove()
