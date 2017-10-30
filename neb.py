#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import logging
import json
import re
import time
from functools import wraps
from pymongo import MongoClient

_LOG_FILE = 'log.txt'

PROF_DATA = {}


def profile(fn):
    @wraps(fn)
    def with_profiling(*args, **kwargs):
        start_time = time.time()

        ret = fn(*args, **kwargs)

        elapsed_time = time.time() - start_time

        if fn.__name__ not in PROF_DATA:
            PROF_DATA[fn.__name__] = [0, []]
        PROF_DATA[fn.__name__][0] += 1
        PROF_DATA[fn.__name__][1].append(elapsed_time)

        return ret

    return with_profiling


def print_prof_data():
    for fname, data in PROF_DATA.items():
        max_time = max(data[1])
        avg_time = sum(data[1]) / len(data[1])
        print "方法 %s 共被执行了 %d 次, " % (fname, data[0]),
        print '耗时: %.3f max, %.3f min' % (max_time, avg_time)


def clear_prof_data():
    global PROF_DATA
    PROF_DATA = {}


def traversalCategories():
    pass


def connectMongo(url):
    global db
    db = MongoClient(url).ecant


@profile
def loadFlatCategories(filename):
    cdic = {}
    with open(filename) as f:
        r = re.compile(r'"(\d+)","(.+)","(-?\d+)"')
        for line in f:
            m = r.match(line)
            if m is None:
                continue

            g = m.groups()
            cdic[int(g[0])] = {
                'name': g[1],
                'parent': int(g[2])
            }

    for k, v in cdic.iteritems():
        if v['parent'] == -1:
            continue

        pid = v['parent']
        while cdic[pid]['parent'] != -1:
            print pid
            pid = cdic[pid]['parent']

        print '%d: %s -> %d ... %d' % (k, v['name'], v['parent'], pid)


if __name__ == '__main__':
    if os.path.isfile(_LOG_FILE):
        os.remove(_LOG_FILE)

    logging.basicConfig(filename=_LOG_FILE, level=logging.DEBUG)

    logging.getLogger().addHandler(logging.StreamHandler())

    reload(sys)
    sys.setdefaultencoding("utf8")

    config = None
    with open('config.json') as json_data:
        config = json.load(json_data)

    print json.dumps(config, indent=2)
    connectMongo(config['mongo_uri'])
    loadFlatCategories(config["data"]["categories"])

    print_prof_data()
