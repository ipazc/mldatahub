#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.storage.local.local_storage import LocalStorage

__author__ = "Iván de Paz Centeno"

storage = LocalStorage("examples/tmp_folder")
file_id = storage.put_file_content(b"content")

content = storage.get_file_content(file_id)

print(content)