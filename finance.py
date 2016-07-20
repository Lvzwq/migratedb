#!/usr/bin/python env
# coding: utf-8
from helper.model import DBConnection
from config import finance_dict
import time

""""
mysql数据库迁移
"""


if __name__ == "__main__":
    # model = Model()
    # model.select(select_param=["id", "name", "gender"], where_param={"name = ": "jack", "gender = ": "M"})
    # model.order_by(["name desc", "gender desc"])
    # model.limit(1, 4)
    # result = model.execute()
    # print result
    # model.insert(result)

    # 配置
    print finance_dict
    # 迁移数据库
    model = DBConnection(**finance_dict)
    # 要迁移的数据库表 CpcFinanceDetail
    model.table_name = "CpcFinanceDetail"

    model_v2 = DBConnection(**finance_dict)
    # 迁移的数据迁移到 CpcFinanceDetail1
    model_v2.table_name = "CpcFinanceDetail1"

    # 从线下库中找到上次同步的id
    model_v2.select(select_param=["max(id) as max_id"])
    mid = model_v2.execute()
    max_id = mid[0]["max_id"]
    if max_id is None:
        max_id = 0
    # 同步操作
    status = True
    # while status:
    model.select(select_param=["*"], where_param={"id > ": max_id})
    model.limit(0, 1000)
    item_list = model.execute()
    print item_list
        # item_list = model.execute()
        # if not item_list:
        #     status = False
        #     break
        # count = len(item_list)
        # max_id = item_list[count - 1]["id"]
        #
        # print "正在向表 " + table + " 表中插入" + str(count) + " 条数据 当前id为 " + str(max_id)
        # # 插入到广告线下数据库
        # model_v2.insert(item_list)
        # model_v2.exe()
        # model_v2.commit()
        # # 停留0.1 s
        # time.sleep(0.1)



