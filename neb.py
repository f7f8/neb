#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import logging
import json
import zlib
import re
import time
import dateutil.parser
from functools import wraps
from pymongo import MongoClient
import signal
from multiprocessing import Pool
import redis

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
    global DB
    DB = MongoClient(url).baichuan


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
            # print '一级分类 %d: %s' % (k, v['name'])
            continue

        pid = v['parent']
        while cdic[pid]['parent'] != -1:
            pid = cdic[pid]['parent']

        v['root'] = pid
        # print '%d: %s -> %d ... %d' % (k, v['name'], v['parent'], pid)

    return cdic


@profile
def loadStores(filename):
    stores = {}
    with open(filename) as f:
        r = re.compile(r'"(\d+)","(.*)","(\d+)"')
        for line in f:
            m = r.match(line)
            if m is None:
                print line
                continue

            g = m.groups()
            stores[int(g[0])] = {
                'name': g[1],
                'province': int(g[2])
            }

    return stores


@profile
def loadGoodsToRedis(filename):
    gc = 0
    lc = 0
    bc0 = {}
    bc1 = {}
    with open(filename) as f:
        r = re.compile(r'"(\d+)","(.+)","(\d+)","(\d+)"')
        for line in f:
            lc += 1
            m = r.match(line)
            if m is None:
                logging.error('未能正确识别的文本行: %s' % line)
                continue

            g = m.groups()

            v = '{"name": "%s","price":%d,"cid":%d}' % (
                g[1], int(g[2]), int(g[3])
            )

            if int(g[0]) % 2 == 0:
                bc0[g[0]] = v
            else:
                bc1[g[0]] = v

            gc += 1

            if len(bc0) >= 200000:
                R0.mset(bc0)
                bc0 = {}

            if len(bc1) >= 200000:
                R1.mset(bc1)
                bc1 = {}

            if gc % 100000 == 0:
                logging.debug('已加载商品 %d / %d 行' % (gc, lc))

        if len(bc0) > 0:
            R0.mset(bc0)

        if len(bc1) > 0:
            R1.mset(bc1)
        logging.debug('成功加载商品 %d / %d 行' % (gc, lc))


@profile
def loadGoods(filename):
    goods = {}
    gc = 0
    lc = 0
    with open(filename) as f:
        r = re.compile(r'"(\d+)","(.+)","(\d+)","(\d+)"')
        for line in f:
            lc += 1
            m = r.match(line)
            if m is None:
                logging.error('未能正确识别的文本行: %s' % line)
                continue

            g = m.groups()
            goods[int(g[0])] = {
                'name': zlib.compress(g[1]),
                # 'price': int(g[2]),
                'cid': int(g[3])
            }
            gc += 1

            if gc % 500000 == 0:
                logging.debug('已加载商品 %d / %d 行' % (gc, lc))

        logging.debug('成功加载商品 %d / %d 行' % (gc, lc))

    return goods


def saveOrder(order, mkey):
    dbkey = 'tr%s_%d' % (
        mkey,
        order["uid"] % 10
    )

    query = {'order_id': order['order_id']}
    update = {'$set': order}

    DB[dbkey].update_one(query, update, upsert = True)


def updateStoreStat(stid, qty, amount, mkey):
    pvid = STORES[stid]['province']

    query = {'stid': stid, 'mkey': mkey}
    update = {
        '$set': {'stid': stid, 'mkey': mkey, 'pvid': pvid},
        '$inc': {'qty': qty, 'amount': amount}
    }

    DB['store_stats'].update(query, update, upsert = True)


def updateCategoryStat(cid, qty, amount, mkey):
    rootcid = FLATC[cid]['root'] if 'root' in FLATC[cid] else cid

    query = {'rootcid': rootcid, 'mkey': mkey}
    update = {
        '$set': {'rootcid': rootcid, 'mkey': mkey},
        '$inc': {'qty': qty, 'amount': amount}
    }

    DB['category_stats'].update(query, update, upsert = True)


def loadTransactions(filename, stores):
    lc = 0
    tc = 0
    oc = 0
    with open(filename) as f:
        r = re.compile(
            r'"(\d+)","(\d+)","(\d+)","(\d+)","(.*)","(\d+)","(\d+)"'
        )

        lastOrder = {
            'order_id': '-1'
        }

        mkey = ''

        for line in f:
            lc += 1
            m = r.match(line)
            if m is None:
                logging.error('未能正确识别的文本行: %s' % line)
                continue

            g = m.groups()
            oid = g[0]

            lastoid = lastOrder['order_id']
            if lastoid == "-1" or lastoid != oid:
                if lastoid != '-1':
                    saveOrder(lastOrder, mkey)
                    oc += 1

                tm = dateutil.parser.parse(g[4])

                mkey = tm.strftime("%Y%m")
                uid = int(g[5])
                stid = int(g[6])
                lastOrder = {
                    'order_id': oid,
                    'store_id': stid,
                    'uid': uid,
                    'created_at': tm,
                    'total_amount': 0,
                    'items': []
                }

            sku = int(g[1])
            price = int(g[2])
            qty = int(g[3])

            amount = price * qty
            lastOrder['total_amount'] += amount

            s = ''
            if sku % 2 == 0:
                s = R0.get(str(sku))
            else:
                s = R1.get(str(sku))

            s = s.replace('\\', '\\\\')
            g = json.loads(s, strict=False)

            lastOrder['items'].append({
                'sku': sku,
                'title': g['name'],
                'price': price,
                'qty': qty,
                'amount': amount
            })

            updateStoreStat(lastOrder['store_id'], qty, amount, mkey)
            updateCategoryStat(g['cid'], qty, amount, mkey)

            tc += 1

            if tc % 10000 == 0:
                logging.debug('已加载交易 %d / %d 行' % (tc, lc))

        saveOrder(lastOrder, mkey)
        oc += 1
        logging.debug('成功加载交易 %d / %d 行, 共 %d 个订单' % (tc, lc, oc))


def doLoadTrTask(filename):
    logging.info('[neb] 开始加载交易信息 <-- %s' % filename)
    loadTransactions(filename, STORES)


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


@profile
def loadAllTransactions():
    trange = range(CONFIG["trs_from"], CONFIG["trs_to"] + 1)
    tasks = [CONFIG["data"]["transactions"] % i for i in trange]
    pool = Pool(len(tasks), init_worker)

    try:
        logging.debug('交易处理并行任务已经启动，按Ctrl + C中止！')
        pool.map_async(doLoadTrTask, tasks).get(0xffffffff)
        pool.close()
        pool.join()
        logging.debug('交易处理并行任务完成！')
    except KeyboardInterrupt:
        logging.warn('任务强制中断！')
        pool.terminate()
        pool.join()


if __name__ == '__main__':
    if os.path.isfile(_LOG_FILE):
        os.remove(_LOG_FILE)

    logging.basicConfig(filename=_LOG_FILE, level=logging.DEBUG)

    logging.getLogger().addHandler(logging.StreamHandler())

    reload(sys)
    sys.setdefaultencoding("utf8")

    global CONFIG
    CONFIG = None
    with open('config.json') as json_data:
        CONFIG = json.load(json_data)

    print json.dumps(CONFIG, indent=2)

    global R0
    R0 = redis.StrictRedis(
        host=CONFIG["R0"]["host"], port=CONFIG["R0"]["port"], db=0
    )

    global R1
    R1 = redis.StrictRedis(
        host=CONFIG["R1"]["host"], port=CONFIG["R1"]["port"], db=0
    )

    connectMongo(CONFIG['db_baichuan'])

    global FLATC
    logging.info('[neb] 开始加载品类信息 <-- %s' % CONFIG["data"]["categories"])
    FLATC = loadFlatCategories(CONFIG["data"]["categories"])
    logging.info('[neb] 共发现 %d 个品类' % len(FLATC))

    global STORES
    logging.info('[neb] 开始加载门店信息 <-- %s' % CONFIG["data"]["stores"])
    STORES = loadStores(CONFIG["data"]["stores"])
    logging.info('[neb] 共发现 %d 个门店' % len(STORES))

    # logging.info('[neb] 开始加载商品信息 <-- %s' % CONFIG["data"]["goods"])
    # loadGoodsToRedis(CONFIG["data"]["goods"])

    #loadAllTransactions()
    doLoadTrTask(CONFIG["data"]["transactions"] % 1)
    print_prof_data()
