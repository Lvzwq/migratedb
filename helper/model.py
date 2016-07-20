#!/usr/bin/python env
# coding: utf-8
import MySQLdb


class DBConnection(object):
    # conn = None
    """
    数据库操作工具类
    """
    def __init__(self, **kwargs):
        """初始化"""
        self._table_name = kwargs.get("table", None)
        self._kwargs = dict()
        self._kwargs["host"] = kwargs.get("host", "127.0.0.1")
        self._kwargs["port"] = kwargs.get("port", 3306)
        self._kwargs["user"] = kwargs.get("user", "root")
        self._kwargs["passwd"] = kwargs.get("passwd", "root")
        self._kwargs["db"] = kwargs.get("db", "test")
        self._plain_sql = ""
        self.conn = None
        self.conn = self.connect(**self._kwargs)

    @property
    def kwargs(self):
        return self._kwargs

    def connect(self, **kwargs):
        if self.conn is None:
            if kwargs is None:
                return None
            try:
                self.conn = MySQLdb.connect(**kwargs)
            except MySQLdb.Error as e:
                print e
            return self.conn
        else:
            return self.conn

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
        self._kwargs["db"] = value
        self.conn.select_db(value)

    @property
    def sql(self):
        return self._plain_sql

    @sql.setter
    def sql(self, value):
        self._plain_sql = value

    def init_sql(self, sql=""):
        """清空sql"""
        self._plain_sql = sql

    def cur(self):
        if self.conn is not None:
            return self.conn.cursor(MySQLdb.cursors.DictCursor)
        conn = self.connect()
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
        print where_param
        for m_key in where_param:
            if isinstance(where_param[m_key], str) or isinstance(where_param[m_key], unicode):
                where_list.append(m_key + "\"" + where_param[m_key] + "\"")
            else:
                where_list.append(m_key + str(where_param[m_key]))
        self._plain_sql += " AND ".join(where_list)

    def select(self, select_param=["*"], where_param=None):
        return self._build_select(select_param=select_param, where_param=where_param)

    def limit(self, offset=0, size=50):
        if not self._plain_sql:
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
            keys = params.keys()
            var = params.values()
            var = map(self.str_str, var)
            self._plain_sql += ",".join(keys) + ") VALUES (" + ",".join(var)
            self._plain_sql += ")"
        elif isinstance(params, list) or isinstance(params, tuple):
            self._plain_sql += ", ".join(params[0].keys()) + ") VALUES "
            in_arr = []
            for v in params:
                var = v.values()
                var = map(self.str_str, var)
                in_arr.append("(" + ", ".join(var) + ")")
            self._plain_sql += ",".join(in_arr)

    def insert(self, params):
        """组装插入语句"""
        if params is None:
            return
        self._build_insert(params)

    def show_tables(self):
        if not self.db:
            print "db没有设置"
            return None
        self._plain_sql = "SHOW TABLES"
        tables_in_db = self.execute()
        self.init_sql()
        tables = list()
        for table in tables_in_db:
            tables.append(table["Tables_in_" + self.db])
        return tables

    def show_create_table(self):
        if not self.db or not self.table_name:
            return
        self.sql = "SHOW CREATE TABLE " + self.table_name
        try:
            table_info = self.execute()
            self.init_sql()
            return table_info[0]["Create Table"]
        except Exception as e:
            print e

    def execute(self):
        """执行查询操作"""
        cursor = self.cur()
        if self._plain_sql == "" or self._plain_sql is None:
            print "执行语句不能为空"
            return
        count = cursor.execute(self._plain_sql)
        print "查询语句为:" + self._plain_sql
        print "查询结果数量:" + str(count)
        self._plain_sql = ""
        return cursor.fetchall()

    def exe(self):
        """插入或者更新"""
        try:
            cursor = self.cur()
            cursor.execute(self._plain_sql)
        except Exception as e:
            print "执行" + self._plain_sql + "出错"
            print e

    def commit(self):
        """插入提交更新"""
        self.conn.commit()

    def close(self):
        pass

    def str_str(self, param):
        """对数据库字段值是字符串类型加引号"""
        if isinstance(param, str) or isinstance(param, unicode):
            param = MySQLdb.escape_string(param)
            return "\"" + param + "\""
        elif isinstance(param, int) or isinstance(param, long):
            return str(param)
        elif param is None:
            return "NULL"
        else:
            return param

    def create_table(self, table_info):
        if not table_info:
            print "创建表信息不能为空"
            return
        self._plain_sql = table_info
        self.execute()


