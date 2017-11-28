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

import json
from pygfolder import PyGFolder
from pyzip import PyZip
from mldatahub.config.config import global_config
from mldatahub.backup.filebackuper import FileBackuper
from mldatahub.helper.timing_helper import now
from mldatahub.log.logger import Logger

__author__ = 'Iván de Paz Centeno'


logger = Logger(module_name="BAK-GDRIVE",
                verbosity_level=global_config.get_log_verbosity(),
                log_file=global_config.get_log_file())

# Some short-names for the log functions.
i = logger.info
d = logger.debug
e = logger.error
w = logger.warning


def as_bytes(c):
    """
    Converts a given input into bytes format.
    :param c: any input type
    :return: binary version of the input.
    """
    if type(c) is dict or type(c) is list:
        result = json.dumps(c).encode()
    else:
        result = str(c).encode()

    return result


class GoogleDriveFileBackuper(FileBackuper):

    def __init__(self, storage=None):
        FileBackuper.__init__(self, storage=storage)


        drive_folder = global_config.get_google_drive_folder()

        d("Initializing PYGFolder...")
        self.pygfolder = PyGFolder()
        d("Done")

        if drive_folder.endswith("/"): drive_folder = drive_folder[:-1]

        init_files = [drive_folder + "/files/indexes/init", drive_folder + "/files/contents/init"]

        d("Creating init file with current timestamp...")
        for init_file in init_files:
            timestamp = as_bytes(now())
            self.pygfolder[init_file] = timestamp
        d("Done")

        d("Accessing folder...")
        self.pygfolder = self.pygfolder[drive_folder]
        d("Done")

    def __store_packet__(self, packet, name):
        # Packet is a dictionary of format FileID->Content
        # Name is the name of the packet

        # We must store these data in the backend.
        try:
            # We must write the index reference.
            # How to do that?
            # We pick the [0:-3] elements' id as the hash of the index file that contains it.
            # We retrieve all the different indexes required by the stored elements
            # We save back into the indexes each of the element's ids and the link to the file that is hosting it.

            # Let us see an example:

            # Imagine that we have a dataset with elements whose file refs points to the ids XXXXX01e, XXXXX01f, XXXXX01g
            # and XXXXD000
            # If we want to know how to locate these files, we only need to take the [: -3] of each of them.
            # We get 2 different values doing that: "XXXXX" and "XXXXD". Thus, we only need to download those two indexes
            # to know where to locate the elements.

            # Each index file will contain the whole ID of each of the elements, pointing to the ZIP file that contains them
            # Next step is to download this ZIP file and to get the elements' content from there.

            # Summarizing:

            # 1. Get the id of the hash for elements holders
            hashes = {id[:-3] for id in packet}


            # 2. Update the indexes for the hashes:
            index_tables = {}
            recrafted_packet = {}

            for hash in hashes:
                try:
                    index_table = json.loads(self.pygfolder["files/indexes/"+hash].decode())
                except:
                    # Let's create the file with the indexes
                    index_table = {}

                if len(index_table) > 0:
                    d("Found previous index for hash {}... ({} previous elements)".format(hash, len(index_table)))

                # Some of the files might be already stored in the drive. Let's check which files are not stored yet.
                recrafted_packet.update({file_id: content for file_id, content in packet.items() if file_id not in index_table and file_id.startswith(hash)})
                recrafted_index_table = {id: name for id in recrafted_packet if id.startswith(hash)}

                if len(recrafted_index_table) > 0:
                    index_table.update({id: name for id in packet if id.startswith(hash)})

                    d("Generated new table for hash {}".format(hash))
                    index_tables["files/indexes/"+hash] = json.dumps(index_table).encode()
                else:
                    d("Skipped index table {} because no elements are appended to it".format(hash))

            if len(recrafted_packet) > 0:

                d("Storing a packet of size {}".format(len(recrafted_packet)))
                content = PyZip(recrafted_packet).to_bytes(True)
                d("Compressed into {} bytes".format(len(content)))
                self.pygfolder["files/contents/"+name] = content
                d("Saved into GDrive: {}".format(name))

                # Finally we save the indexes
                for index_table_id, index_table in index_tables.items():
                    self.pygfolder[index_table_id] = index_table

                d("Stored {} index tables in backend".format(len(index_tables)))

            if len(recrafted_packet) < len(packet):
                d("{} files skipped (duplicated in backend)".format(len(packet) - len(recrafted_packet)))

        finally:
            self._decrease_tasks_count(len(packet))

    def __retrieve_packet__(self, file_ids_list):
        """
        Retrieves a packet filled with the given file_ids
        :param file_ids_list: file_ids to build the packet.
        :return: Packet, a dict with format FileID -> Content
        """
        hashes = {str(id)[:-3] for id in file_ids_list}

        table_indexes = {}

        try:
            d("Retrieving {} table indexes...".format(len(hashes)))
            for hash_id in hashes:
                table_indexes.update(json.loads(self.pygfolder["files/indexes/"+hash_id].decode()))
            d("Done")

            table_content = {}
            for file_id in file_ids_list:
                file_id = str(file_id)
                if file_id in table_content: continue

                d("Downloading Zip file for element {}...".format(file_id))
                zip_uri = table_indexes[file_id]
                zip_content = PyZip().from_bytes(self.pygfolder["files/contents/"+zip_uri])
                d("Done.")
                table_content.update(zip_content)

            packet = {str(file_id): {'content': table_content[str(file_id)], 'count': 1} for file_id in file_ids_list}

        except KeyError as ex:
            packet = {str(file_id): {'content': "hola: "+str(ex), 'count': 1, 'error': str(ex)} for file_id in file_ids_list}

        finally:
            self._decrease_tasks_count(len(file_ids_list))


        return packet
