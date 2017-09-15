#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Iván de Paz Centeno'

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)