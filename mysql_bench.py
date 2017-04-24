#!/usr/bin/env python

import asyncio
import logging
import os
import time

import tornado
import tornado.ioloop
import tornado.options
import tornado_mysql

from tornado_mysql import pools
from tornado import gen
from tornado.concurrent import run_on_executor
from concurrent.futures import ProcessPoolExecutor

import pymysql
import _mysql


tornado_mysql_db_pool = pools.Pool(
    connect_kwargs={
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'passwd': '',
        'db': 'mysql_benchmark',
        'charset': 'utf8mb4',
        'init_command': 'SET NAMES \'utf8mb4\'',
    },
    max_idle_connections=10,
    max_open_connections=10,
    max_recycle_sec=60
)


# define the # of processes for multi-proc runs
MAX_WORKERS = 8


async def do_tornado_mysql(num_queries):
    all_executes = []
    for _ in range(num_queries):
        all_executes.append(tornado_mysql_db_pool.execute('SELECT 1'))
    await gen.multi(all_executes)


async def run_tornado_mysql_single(num_queries):
    logging.info('In run_tornado_mysql_single()')
    start_time = time.time()
    await do_tornado_mysql(num_queries)
    total_time = time.time() - start_time
    logging.info('time = %s', total_time)
    logging.info('throughput = %s q/s', num_queries / total_time)
    tornado.ioloop.IOLoop.instance().stop()


def do_pymysql(num_queries):
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 db='mysql_benchmark',
                                 charset='utf8mb4')
    with connection.cursor() as cursor:
        for _ in range(num_queries):
            cursor.execute('SELECT 1')
    connection.close()


async def run_pymysql_single(num_queries):
    logging.info('In run_pymysql_single()')
    start_time = time.time()
    do_pymysql(num_queries)
    total_time = time.time() - start_time
    logging.info('time = %s', total_time)
    logging.info('throughput = %s q/s', num_queries / total_time)
    tornado.ioloop.IOLoop.instance().stop()


async def run_pymysql_multi(num_queries, num_threads=1):
    logging.info('In run_pymysql_multi()')
    start_time = time.time()
    all_executes = []
    executor = ProcessPoolExecutor(max_workers=num_threads)
    for _ in range(num_threads):
        all_executes.append(executor.submit(do_pymysql, num_queries))
    for future in all_executes:
        future.result()
    total_time = time.time() - start_time
    logging.info('time = %s', total_time)
    logging.info('throughput = %s q/s', num_queries * num_threads / total_time)
    tornado.ioloop.IOLoop.instance().stop()


def do_mysqlclient(num_queries):
    connection = _mysql.connect('localhost', 'root', '', 'mysql_benchmark')
    for _ in range(num_queries):
        connection.query('SELECT 1')
        result = connection.store_result()
        result.fetch_row()
    connection.close()


async def run_mysqlclient_single(num_queries):
    logging.info('In run_mysqlclient_single()')
    start_time = time.time()
    do_mysqlclient(num_queries)
    total_time = time.time() - start_time
    logging.info('time = %s', total_time)
    logging.info('throughput = %s q/s', num_queries / total_time)
    tornado.ioloop.IOLoop.instance().stop()


async def run_mysqlclient_multi(num_queries, num_threads=1):
    logging.info('In run_mysqlclient_multi()')
    start_time = time.time()
    all_executes = []
    executor = ProcessPoolExecutor(max_workers=num_threads)
    for _ in range(num_threads):
        all_executes.append(executor.submit(do_mysqlclient, num_queries))
    for future in all_executes:
        future.result()
    total_time = time.time() - start_time
    logging.info('time = %s', total_time)
    logging.info('throughput = %s q/s', num_queries * num_threads / total_time)
    tornado.ioloop.IOLoop.instance().stop()


async def run_tornado_mysql_multi(num_queries, num_threads=1):
    logging.info('In run_tornado_mysql_multi()')
    start_time = time.time()
    all_executes = []
    executor = ProcessPoolExecutor(max_workers=num_threads)
    for _ in range(num_threads):
        all_executes.append(executor.submit(do_tornado_mysql, num_queries))
    for future in all_executes:
        future.result()
    total_time = time.time() - start_time
    logging.info('time = %s', total_time)
    logging.info('throughput = %s q/s', num_queries * num_threads / total_time)
    tornado.ioloop.IOLoop.instance().stop()


def run_benchmarks():
    tornado.ioloop.IOLoop.instance().spawn_callback(run_tornado_mysql_single, 10000)
    tornado.ioloop.IOLoop.instance().start()
    # can't seem to get async tornado to run in multiple processes w/o http...
    # asyncio.get_event_loop().run_until_complete(run_tornado_mysql_multiproc(10000, MAX_WORKERS))
    # tornado.ioloop.IOLoop.instance().start()
    tornado.ioloop.IOLoop.instance().spawn_callback(run_pymysql_single, 10000)
    tornado.ioloop.IOLoop.instance().start()
    tornado.ioloop.IOLoop.instance().spawn_callback(run_pymysql_multi, 10000, MAX_WORKERS)
    tornado.ioloop.IOLoop.instance().start()
    tornado.ioloop.IOLoop.instance().spawn_callback(run_mysqlclient_single, 10000)
    tornado.ioloop.IOLoop.instance().start()
    tornado.ioloop.IOLoop.instance().spawn_callback(run_mysqlclient_multi, 10000, MAX_WORKERS)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    logging.getLogger().setLevel('DEBUG')
    tornado.options.parse_command_line()

    logging.info("Running Python MySQL Benchmarks...")
    run_benchmarks()
