#!/usr/bin/python env
# coding: utf-8
from helper.model import Model
from config import migrate_db_dict, ads_offline_db_dict
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
    print migrate_db_dict, ads_offline_db_dict
    # 迁移数据库
    model = Model(**migrate_db_dict)
    # model.table_name = "UnionKeyWord"

    # 广告线下数据库
    model_v2 = Model(**ads_offline_db_dict)
    # model_v2.table_name = "UnionKeyWord"

    # 显示所有数据表
    result = model.show_tables()
    print "检测数据库中表......."
    print model.kwargs["host"] + " 数据库中有如下表:"
    for table in result:
        print table

    """
    # 同步两个库中的表结构
    for table in result:
        model.table_name = table
        # 获取同步库的表信息
        table_info = model.show_create_table()

        model_v2.table_name = table
        table_info_v2 = model_v2.show_create_table()
        if table_info_v2 is None:
            print model_v2.kwargs["host"] + "数据库" + model_v2.db + "中" + table + "不存在"
            print "创建" + table + "中......."
            model_v2.create_table(table_info)
            print "创建成功!!!"
        else:
            print table + "已存在！！！"
    """
    # result = ["UnionActivityPlan"]
    # 同步表数据
    for table in result:
        model_v2.table_name = table
        # 从线下库中找到上次同步的id
        model_v2.select(select_param=["max(id) as max_id"])
        mid = model_v2.execute()
        max_id = mid[0]["max_id"]
        if max_id is None:
            max_id = 0
        # 同步操作
        status = True
        while status:
            model.table_name = table
            model.select(select_param=["*"], where_param={"id > ": max_id})
            model.limit(0, 1000)
            item_list = model.execute()
            if not item_list:
                status = False
                break
            count = len(item_list)
            max_id = item_list[count - 1]["id"]

            print "正在向表 " + table + " 表中插入" + str(count) + " 条数据 当前id为 " + str(max_id)
            # 插入到广告线下数据库
            model_v2.insert(item_list)
            model_v2.exe()
            model_v2.commit()
            # 停留0.1 s
            time.sleep(0.1)



