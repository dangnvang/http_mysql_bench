#!/usr/bin/env python

import os
import logging

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen

import pymysql
import pymysql.cursors

import _mysql

from tornado.options import define, options, parse_command_line

from tornado_mysql import pools
from tornado_mysql.cursors import DictCursor


# The endpoints are:
# /hello_world => plain hello world w/o DB access
# /basic_query/# => DB access without hitting a table (e.g.: 'SELECT 1')
# /parent_query/# => DB access hitting one table for all columns
# /full_query/# => DB access hitting two tables with a JOIN, multiple rows returned

# initialized in main after http server to avoid IOLoop error
TORNADO_MYSQL_POOL = None

PYMYSQL_CONNECTION = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='mysql_benchmark',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)


MYSQLCLIENT_DB = _mysql.connect('localhost', 'root', '', 'mysql_benchmark')

class HelloWorldHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")


class TornadoMySQLBasicHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        cur = yield TORNADO_MYSQL_POOL.execute('SELECT %s', (id,))
        cur.fetchall()
        self.write("Hello, tornado")


class TornadoMySQLParentHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        cur = yield TORNADO_MYSQL_POOL.execute('SELECT * FROM `buzz` WHERE `id`=%s', (id,))
        cur.fetchall()
        self.write("Hello, tornado")


class TornadoMySQLFullHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        cur = yield TORNADO_MYSQL_POOL.execute(
            'SELECT * FROM `buzz` INNER JOIN `sub_buzz` ON `buzz`.`id`=`sub_buzz`.`buzz_id` WHERE `buzz`.`id`=%s', (id,))
        cur.fetchall()
        self.write("Hello, tornado")


class PyMySQLBasicHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        with PYMYSQL_CONNECTION.cursor() as cursor:
            cursor.execute('SELECT %s', (id,))
            cursor.fetchall()
        self.write("Hello, pymysql")


class PyMySQLParentHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        with PYMYSQL_CONNECTION.cursor() as cursor:
            cursor.execute('SELECT * FROM `buzz` WHERE `id`=%s', (id,))
            cursor.fetchall()
        self.write("Hello, pymysql")


class PyMySQLFullHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        with PYMYSQL_CONNECTION.cursor() as cursor:
            cursor.execute('SELECT * FROM `buzz` INNER JOIN `sub_buzz` ON `buzz`.`id`=`sub_buzz`.`buzz_id` WHERE `buzz`.`id`=%s', (id,))
            cursor.fetchall()
        self.write("Hello, pymysql")


class MySQLClientBasicHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        MYSQLCLIENT_DB.query('SELECT ' + id)
        result = MYSQLCLIENT_DB.store_result()
        result.fetch_row()
        self.write("Hello, mysqlclient")


class MySQLClientParentHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        MYSQLCLIENT_DB.query('SELECT * FROM `buzz` WHERE `id`=' + id)
        result = MYSQLCLIENT_DB.store_result()
        result.fetch_row()
        self.write("Hello, mysqlclient")


class MySQLClientFullHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, id):
        MYSQLCLIENT_DB.query('SELECT * FROM `buzz` INNER JOIN `sub_buzz` ON `buzz`.`id`=`sub_buzz`.`buzz_id` WHERE `buzz`.`id`=' + id)
        result = MYSQLCLIENT_DB.store_result()
        result.fetch_row(maxrows=0)
        self.write("Hello, pymysql")


# the 'driver' command line option determines the following
DRIVER_HANDLER_MAPPING = {
    'tornado_mysql': {
        'basic_query': TornadoMySQLBasicHandler,
        'parent_query': TornadoMySQLParentHandler,
        'full_query': TornadoMySQLFullHandler
    },
    'pymysql': {
        'basic_query': PyMySQLBasicHandler,
        'parent_query': PyMySQLParentHandler,
        'full_query': PyMySQLFullHandler

    },
    'mysqlclient': {
        'basic_query': MySQLClientBasicHandler,
        'parent_query': MySQLClientParentHandler,
        'full_query': MySQLClientFullHandler
    }
}


class Application(tornado.web.Application):
    def __init__(self, driver):
        app_settings = {
            'debug': False,
        }

        routes = [
            (r'^/hello_world$', HelloWorldHandler),
            (r'^/basic_query/([^/]+)$', DRIVER_HANDLER_MAPPING[driver]['basic_query']),
            (r'^/parent_query/([^/]+)$', DRIVER_HANDLER_MAPPING[driver]['parent_query']),
            (r'^/full_query/([^/]+)$', DRIVER_HANDLER_MAPPING[driver]['full_query']),
        ]

        super(Application, self).__init__(routes, **app_settings)


if __name__ == '__main__':
    logging.getLogger().setLevel('INFO')
    define("port", default=42001)
    define("driver", default="tornado_mysql")
    define("num_procs", default=1)
    parse_command_line()

    address = '0.0.0.0'  # bind to all available IPs
    logging.info('starting on %s:%d', address, options.port)

    application = Application(options.driver)

    http_server = tornado.httpserver.HTTPServer(request_callback=application, xheaders=True)
    http_server.bind(options.port, address=address)
    http_server.start(options.num_procs)

    if options.driver == 'tornado_mysql':
        TORNADO_MYSQL_POOL = pools.Pool(
            connect_kwargs={
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'passwd': '',
                'db': 'mysql_benchmark',
                'charset': 'utf8mb4',
                'cursorclass': DictCursor
            })

    tornado.ioloop.IOLoop.instance().start()
