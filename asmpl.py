#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
from time import sleep

from flask import request
from flask_restful import abort

from mldatahub.config.config import now, global_config
from mldatahub.odm.restapi_dao import RestAPIDAO

__author__ = "Iván de Paz Centeno"


MAX_ACCESS_TIMES = global_config.get_max_access_times()
ACCESS_RESET_TIME = global_config.get_access_reset_time()
session = global_config.get_session()

def control_access():
    def func_wrap(func):
        def args_wrap(*args, **kwargs):
            remote_ip = request.remote_addr

            ip_control = RestAPIDAO.query.get(ip=remote_ip)

            if ip_control is None:
                ip_control = RestAPIDAO(ip=remote_ip, last_access=now(), num_accesses=0)

            if (now() - ip_control.last_access).total_seconds() > ACCESS_RESET_TIME:
                ip_control.last_access = now()
                ip_control.num_accesses = 0

            if ip_control.num_accesses > MAX_ACCESS_TIMES:
                abort(429)

            ip_control.num_accesses += 1

            session.flush()

            return func(*args, **kwargs)
        return args_wrap
    return func_wrap

@control_access()
def t(x):
    return "{}".format(x)


for x in range(150):
    print(t(x))
#print(t(3))

