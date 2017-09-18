#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = "Iván de Paz Centeno"

#privileges = Privileges.ADMIN_CREATE_TOKEN\
#             + Privileges.ADMIN_EDIT_TOKEN\
#             + Privileges.ADMIN_DESTROY_TOKEN

privileges = Privileges.USER_CREATE_TOKEN + Privileges.USER_EDIT_TOKEN + Privileges.USER_DESTROY_TOKEN


token1 = TokenDAO("Admin1", 0, 0, "ipazc2", privileges=privileges)

global_config.get_session().flush()
print(token1.token_gui)