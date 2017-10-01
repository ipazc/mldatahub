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

__author__ = 'Iván de Paz Centeno'


class Privileges(object):
    CREATE_DATASET      = 0x0001
    EDIT_DATASET        = 0x0002
    DESTROY_DATASET     = 0x0004

    ADD_ELEMENTS        = 0x0008
    EDIT_ELEMENTS       = 0x0010
    DESTROY_ELEMENTS    = 0x0020

    COMMENT_DATASET     = 0x0040
    COMMENT_ELEMENTS    = 0x0080

    RO_WATCH_DATASET    = 0x0100

    USER_CREATE_TOKEN   = 0x0200
    USER_EDIT_TOKEN     = 0x0400
    USER_DESTROY_TOKEN  = 0x0800

    ADMIN_CREATE_TOKEN  = 0x1000
    ADMIN_EDIT_TOKEN    = 0x2000
    ADMIN_DESTROY_TOKEN = 0x4000