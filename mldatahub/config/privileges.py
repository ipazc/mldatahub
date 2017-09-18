#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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