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
from ming.odm.odmsession import ODMCursor
from mldatahub.log.logger import Logger
from mldatahub.odm.file_dao import FileDAO, FileContentDAO
from mldatahub.storage.exceptions.file_size_exceeded import FileSizeExceeded
from mldatahub.storage.generic_storage import GenericStorage, File
import hashlib

__author__ = 'Iván de Paz Centeno'


logger = Logger("MONGO-STORAGE",
                verbosity_level=global_config.get_log_verbosity(),
                log_file=global_config.get_log_file())

d = logger.debug
i = logger.info
w = logger.warning
e = logger.error

FILE_SIZE_LIMIT = global_config.get_file_size_limit()


class MongoStorage(GenericStorage):
    """
    Represents the storage, backed by MongoDB.
    """
    def __init__(self):
        """
        Constructor of the storage class.
        :return:
        """
        self.session = global_config.get_session()

    def __get_hashed_sha256_file(self, sha256_hash: str) -> FileDAO:
        """
        Retrieves a FileDAO whose sha256 hash matches the specified
        :param sha256_hash: sha256_hash string
        :return: FileDAO object if found. None otherwise.
        """
        return FileDAO.query.get(sha256=sha256_hash)

    def __get_hashed_sha256_files(self, sha256_hashes: list) -> ODMCursor:
        """
        Retrieves a list of FileDAO whose sha256 hash matches any of the specified in the list.
        :param sha256_hashes: list of sha256 hashes to retrieve.
        :return: cursor pointing to the FileDAOs whose SHA256 matches the ones specified in the list.
        Ideally, it will return as many pointers as hashes specified in the list.
        """
        return FileDAO.query.find({'sha256': {'$in': sha256_hashes}})

    def put_file_content(self, content_bytes: bytes, force_id: ObjectId=None) -> ObjectId:
        """
        Puts the content of a file in the storage.
        :param content_bytes:
        :param force_id: ID to put to the file. If it already exists, it will override it.
        :return: ID of the file.
        """
        length = len(content_bytes)

        if length >= FILE_SIZE_LIMIT:
            raise FileSizeExceeded("File size limit of {} Bytes exceeded".format(FILE_SIZE_LIMIT))

        # Let's optimize the storage now
        sha256 = hashlib.sha256(content_bytes).hexdigest()
        file = self.__get_hashed_sha256_file(sha256)

        if file is None:

            if force_id is not None:
                # User is trying to force the id with a custom one.
                # It may happen that the ID already exists in the DB.

                previous_file = FileDAO.query.get(_id=force_id)

                if previous_file is not None:
                    # We delete it in case to avoid conflicts.
                    self.delete_file(force_id)

            file = FileContentDAO(content=content_bytes, size=length, sha256=sha256)

            if force_id is not None:
                file._id = force_id

            self.session.flush()

        return file._id

    def put_files_contents(self, content_bytes_list: list, force_ids: list=None) -> list:
        """
        Puts a set of content files in the storage.
        :param content_bytes_list: list of binary contents to append to the list.
        :param force_ids: list of IDs that matches each of the content bytes, if it is wanted to fix the IDs of
                          the elements. Otherwise, leave it as None and new IDs will be generated.
        :return: list of IDs of the files in the same order.
        """
        length_exceeded = any([len(content_bytes) >= FILE_SIZE_LIMIT for content_bytes in content_bytes_list])

        if length_exceeded:
            raise FileSizeExceeded("File size limit of {} Bytes exceeded".format(FILE_SIZE_LIMIT))

        #1. We get the SHA256 hash for each content
        sha256s = {hashlib.sha256(content_bytes).hexdigest(): {'content': content_bytes, 'force_id': None} for content_bytes in content_bytes_list} if force_ids is None else {hashlib.sha256(content_bytes).hexdigest(): {'content': content_bytes, 'force_id': force_id} for content_bytes, force_id in zip(content_bytes_list, force_ids)}

        #2. We get the files from the current storage that matches the specified hashes.
        hashed_files = {file.sha256: file for file in self.__get_hashed_sha256_files(list(sha256s.keys()))}

        #3. We split what hashes we don't have in the DB.
        unhashed_content = {hash: sha256s[hash] for hash in sha256s if hash not in hashed_files}

        files = []

        unhashed_content_ids = {descr['force_id'] for hash, descr in unhashed_content.items() if descr['force_id'] is not None}

        # We delete the hashes that matches the unhashed content ids
        if len(unhashed_content_ids) > 0:
            self.delete_files(list(unhashed_content_ids))

        for hash, descr in unhashed_content.items():
            content_bytes = descr['content']
            file = FileContentDAO(content=content_bytes, size=len(content_bytes), sha256=hash)
            if descr['force_id'] is not None:
                file._id = descr['force_id']
            files.append(file)

        self.session.flush()

        merged = list(hashed_files.values()) + files

        # We need to ensure the order of the output. It must be the same order as the input
        hash_by_content = {descr['content']: hash for hash, descr in sha256s.items()}
        file_by_hash = {file.sha256: file for file in merged}

        return [file_by_hash[hash_by_content[content]]._id for content in content_bytes_list]

    def get_file(self, file_id:ObjectId) -> File:
        file = FileContentDAO.query.get(_id=file_id)
        return File(file._id, file.content, file.size)

    def get_files(self, files_ids:list) -> list:
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
