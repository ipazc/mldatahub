#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetCommentDAO, DatasetElementDAO, DatasetElementCommentDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = "Iván de Paz Centeno"

#privileges = Privileges.ADMIN_CREATE_TOKEN\
#             + Privileges.ADMIN_EDIT_TOKEN\
#             + Privileges.ADMIN_DESTROY_TOKEN

TokenDAO.query.remove()

privileges = Privileges.CREATE_DATASET + Privileges.EDIT_DATASET + Privileges.DESTROY_DATASET + \
             Privileges.ADD_ELEMENTS + Privileges.EDIT_ELEMENTS + Privileges.DESTROY_ELEMENTS + \
             Privileges.RO_WATCH_DATASET + Privileges.USER_EDIT_TOKEN


token1 = TokenDAO("Ivan de Paz Centeno", 100, 10000, "ipazc", privileges=privileges)

global_config.get_session().flush()
print(token1.token_gui)