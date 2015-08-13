#!/usr/bin/python env
# coding: utf-8

""""
mysql数据库迁移
"""
import MySQLdb

host = "10.11.4.23"
user = "mogu"
port = 3306
db = "unionbusiness"
password = "shark@)$^king"


"""
host = "127.0.0.1"
user = "root"
port = 3306
db = "treediary"
password = ""
table = "user"
"""


class Model(object):
    conn = MySQLdb.connect(host=host, user=user, port=port, db=db, passwd=password)
    """
    数据库操作工具类
    """
    def __init__(self):
        self._table_name = table
        self._plain_sql = ""
        pass

    @staticmethod
    def connect():
        if Model.conn is not None:
            return Model.conn
        try:
            conn = MySQLdb.connect(host=host, user=user, port=port, db=db, passwd=password)
        except MySQLdb.Error as e:
            print e
        return conn

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def db(self):
        return "unionbusiness"

    @db.setter
    def db(self, value):
        self.conn.select_db(value)

    @property
    def sql(self):
        return self._plain_sql

    @sql.setter
    def sql(self, value):
        self._plain_sql = value

    def cur(self):
        if Model.conn is not None:
            return Model.conn.cursor(MySQLdb.cursors.DictCursor)
        conn = Model.connect()
        return conn.cursor(MySQLdb.cursors.DictCursor)

    def _build_select(self, select_param=["*"], where_param=None):
        """组装sql语句"""
        self._plain_sql = "SELECT " + ",".join(select_param) + " FROM " + self.table_name
        if where_param is not None:
            self._build_where(where_param=where_param)

    def _build_where(self, where_param=None):
        """组装where语句"""
        self._plain_sql += " WHERE "
        where_list = []
        for m_key in where_param:
            if type(where_param[m_key]) is str or type(where_param[m_key]) is unicode:
                where_list.append(m_key + "\"" + where_param[m_key] + "\"")
        self._plain_sql += " AND ".join(where_list)

    def select(self, select_param=["*"], where_param=None):
        return self._build_select(select_param=select_param, where_param=where_param)

    def limit(self, offset=0, size=50):
        if self._plain_sql == "":
            return False
        self._plain_sql += " LIMIT %s, %s" % (offset, size)

    def order_by(self, order_fields=[]):
        if not order_fields:
            return
        self._plain_sql += " ORDER BY "
        self._plain_sql += ",".join(order_fields)

    def _build_insert(self, params):
        self._plain_sql = "INSERT INTO " + self._table_name + "("
        if isinstance(params, dict):
            keys = params.iterkeys()
            var = params.itervalues()
            var = map(self.str_str, var)
            self._plain_sql += ",".join(keys) + ") VALUES (" + ",".join(var)
            self._plain_sql += ")"
        elif isinstance(params, list) or isinstance(params, tuple):
            self._plain_sql += ",".join(params[0]) + ") VALUES "
            in_arr = []
            for v in params:
                var = v.itervalues()
                var = map(self.str_str, var)
                in_arr.append("(" + ",".join(var) + ")")
            print in_arr
            self._plain_sql += ",".join(in_arr)
        print self._plain_sql

    def insert(self, params):
        if params is None:
            return
        self._build_insert(params)

    def execute(self):
        cursor = self.cur()
        count = cursor.execute(self._plain_sql)
        print self._plain_sql
        print count
        self._plain_sql = ""
        return cursor.fetchall()

    def close(self):
        pass

    def str_str(self, param):
        if isinstance(param, str) or isinstance(param, unicode):
            return "\"" + param + "\""
        elif isinstance(param, int) or isinstance(param, long):
            return str(param)
        else:
            return param

if __name__ == "__main__":
    # model = Model()
    # model.select(select_param=["id", "name", "gender"], where_param={"name = ": "jack", "gender = ": "M"})
    # model.order_by(["name desc", "gender desc"])
    # model.limit(1, 4)
    # result = model.execute()
    # print result
    # model.insert(result)

    model = Model()
    model.table_name = "UnionKeyWord"
    model.select(select_param=["id", "KeyWord"])
    model.limit(0, 10)
    print model.sql
