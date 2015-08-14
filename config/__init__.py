#!/usr/bin/python env
# coding: utf-8

from ConfigParser import ConfigParser
import os

root = os.path.abspath(".")
config_path = os.path.join(root, "config")

# 导入配置
config = ConfigParser()
config.read(os.path.join(config_path, "db.conf"))


# 要迁移的数据库
migrate_db_dict = dict(config.items("migrate_db"))
migrate_db_dict["port"] = int(migrate_db_dict["port"])
# 广告线下数据库
ads_offline_db_dict = dict(config.items("db"))
ads_offline_db_dict["port"] = int(ads_offline_db_dict["port"])