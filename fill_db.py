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


# initialized in main after http server to avoid IOLoop error
TORNADO_MYSQL_POOL = None


async def insert_buzz(num):
    sql = """
    INSERT INTO `buzz` (
    `col1`,
    `col2`,
    `col3`,
    `col4`,
    `col5`,
    `col6`,
    `col7`,
    `col8`,
    `col9`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    result = await TORNADO_MYSQL_POOL.execute(sql, (
        num,
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
    ))
    return result.lastrowid


async def insert_subbuzz(buzz_id, num):
    sql = """
    INSERT INTO `sub_buzz` (
    `buzz_id`,
    `col1`,
    `col2`,
    `col3`,
    `col4`,
    `col5`,
    `col6`,
    `col7`,
    `col8`,
    `col9`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    result = await TORNADO_MYSQL_POOL.execute(sql, (
        buzz_id,
        num,
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
        str(num),
    ))
    return result.lastrowid


async def fill_db():
    logging.info('Filling DB...')
    for i in range(1000):
        buzz_id = await insert_buzz(i)
        for j in range(10):
            await insert_subbuzz(buzz_id, j)
    logging.info('Done')
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == '__main__':
    logging.getLogger().setLevel('INFO')
    parse_command_line()

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

    tornado.ioloop.IOLoop.instance().spawn_callback(fill_db)
    tornado.ioloop.IOLoop.instance().start()
