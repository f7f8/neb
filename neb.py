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
import signal
from multiprocessing import Pool
import concurrent
from concurrent.futures import ThreadPoolExecutor
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


def connectMongo():
    global DB0
    DB0 = MongoClient(CONFIG['db0']).baichuan

    global DB1
    DB1 = MongoClient(CONFIG['db1']).baichuan


def getGoodsFromLocal(sku):
    return GOODS[sku - 1]


def getGoodsFromRedis(sku):
    s = ''
    if sku % 2 == 0:
        s = R0.get(str(sku))
    else:
        s = R1.get(str(sku))

    s = s.replace('\\', '\\\\')
    g = json.loads(s, strict=False)
    return g


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
    goods = []
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
            goods.append({
                'name': g[1],
                # 'price': int(g[2]),
                'cid': int(g[3])
            })

            gc += 1

            if gc % 500000 == 0:
                logging.debug('已加载商品 %d / %d 行' % (gc, lc))

        logging.debug('成功加载商品 %d / %d 行' % (gc, lc))

    return goods


def saveBulkOrders(blk):
    for k, v in blk.iteritems():
        if int(k[-1:]) % 2 == 0:
            DB0[k].insert(v)
        else:
            DB1[k].insert(v)


def updateStoreStat(blk):
    op = DB0['store_stats'].initialize_ordered_bulk_op()
    for k, v in blk.iteritems():
        query = {'stid': v['stid'], 'mkey': v['mkey']}
        update = {
            '$set': {'stid': v['stid'], 'mkey': v['mkey'], 'pvid': v['pvid']},
            '$inc': {'qty': v['qty'], 'amount': v['amount']}
        }
        op.find(query).upsert().update(update)

    op.execute()


def updateCategoryStat(blk):
    op = DB0['category_stats'].initialize_ordered_bulk_op()
    for k, v in blk.iteritems():
        query = {'rootcid': v['cid'], 'mkey': v['mkey']}
        update = {
            '$set': {'rootcid': v['cid'], 'mkey': v['mkey']},
            '$inc': {'qty': v['qty'], 'amount': v['amount']}
        }
        op.find(query).upsert().update(update)

    op.execute()


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

        bulkSet = {}
        bulkUpdStores = {}
        bulkUpdCategories = {}
        for line in f:
            lc += 1
            m = r.match(line)
            if m is None:
                logging.error('未能正确识别的文本行: %s' % line)
                continue

            g = m.groups()
            oid = g[0]
            stid = int(g[6])

            lastoid = lastOrder['order_id']
            if lastoid == "-1" or lastoid != oid:
                if lastoid != '-1':
                    dbkey = 'tr%s_%d' % (
                        mkey,
                        lastOrder["uid"] % 10
                    )

                    if dbkey not in bulkSet:
                        bulkSet[dbkey] = []

                    bulkSet[dbkey].append(lastOrder)
                    oc += 1

                    if oc % 10000 == 0:
                        saveBulkOrders(bulkSet)
                        bulkSet = {}

                    if oc % 1000000 == 0:
                        updateStoreStat(bulkUpdStores)
                        bulkUpdStores = {}
                        updateCategoryStat(bulkUpdCategories)
                        bulkUpdCategories = {}

                # tm = dateutil.parser.parse(g[4])
                tm = g[4][:26]

                # mkey = tm.strftime("%Y%m")
                mkey = tm[:4] + tm[5:7]
                dkey = tm[:4] + tm[5:7] + tm[8:10]
                uid = int(g[5])
                lastOrder = {
                    'order_id': oid,
                    'dkey': int(dkey),
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

            go = getGoodsFromLocal(sku)

            lastOrder['items'].append({
                'sku': sku,
                'title': go['name'],
                'price': price,
                'qty': qty,
                'amount': amount
            })

            stbkey = '%d_%s' % (stid, mkey)
            if stbkey not in bulkUpdStores:
                pvid = STORES[stid]['province']
                bulkUpdStores[stbkey] = {
                    'stid': stid,
                    'pvid': pvid,
                    'qty': qty,
                    'amount': amount,
                    'mkey': mkey
                }

            bulkUpdStores[stbkey]['qty'] += qty
            bulkUpdStores[stbkey]['amount'] += amount

            cid = go['cid']
            rootcid = FLATC[cid]['root'] if 'root' in FLATC[cid] else cid
            cibkey = '%d_%s' % (rootcid, mkey)
            if cibkey not in bulkUpdCategories:
                bulkUpdCategories[cibkey] = {
                    'cid': rootcid,
                    'qty': qty,
                    'amount': amount,
                    'mkey': mkey
                }

            bulkUpdCategories[cibkey]['qty'] += qty
            bulkUpdCategories[cibkey]['amount'] += amount

            tc += 1

            if tc % 10000 == 0:
                logging.debug('已加载交易 %d / %d 行' % (tc, lc))

        oc += 1
        saveBulkOrders(bulkSet)
        bulkSet = {}
        updateStoreStat(bulkUpdStores)
        bulkUpdStores = {}
        updateCategoryStat(bulkUpdCategories)
        bulkUpdCategories = {}
        logging.debug('成功加载交易 %d / %d 行, 共 %d 个订单' % (tc, lc, oc))


def doLoadTrTask(filename):
    connectMongo()
    logging.info('[neb] 开始加载交易信息 <-- %s' % filename)
    loadTransactions(filename, STORES)


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


@profile
def loadAllThreads2():
    trange = range(CONFIG["trs_from"], CONFIG["trs_to"] + 1)
    tasks = [CONFIG["data"]["transactions"] % i for i in trange]

    try:
        fs = []
        e = ThreadPoolExecutor(max_workers=4)
        for task in tasks:
            fs.append(e.submit(doLoadTrTask, task))

        e.shutdown(False)
        concurrent.futures.wait(fs)
    except KeyboardInterrupt:
        logging.debug('任务强制中断！')


@profile
def loadAllThreads():
    trange = range(CONFIG["trs_from"], CONFIG["trs_to"] + 1)
    tasks = [CONFIG["data"]["transactions"] % i for i in trange]
    futures = set()
    with ThreadPoolExecutor(max_workers=4) as executor:
        for c in tasks:
            future = executor.submit(doLoadTrTask, c)
            futures.add(future)

    try:
        for future in concurrent.futures.as_completed(futures):
            err = future.exception()
            if err is not None:
                raise err

    except KeyboardInterrupt:
        logging.debug('任务强制中断！')
        executor._threads.clear()
        concurrent.futures.thread._threads_queues.clear()


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

    global FLATC
    logging.info('[neb] 开始加载品类信息 <-- %s' % CONFIG["data"]["categories"])
    FLATC = loadFlatCategories(CONFIG["data"]["categories"])
    logging.info('[neb] 共发现 %d 个品类' % len(FLATC))

    global STORES
    logging.info('[neb] 开始加载门店信息 <-- %s' % CONFIG["data"]["stores"])
    STORES = loadStores(CONFIG["data"]["stores"])
    logging.info('[neb] 共发现 %d 个门店' % len(STORES))

    global GOODS
    logging.info('[neb] 开始加载商品信息 <-- %s' % CONFIG["data"]["goods"])
    # loadGoodsToRedis(CONFIG["data"]["goods"])
    GOODS = loadGoods(CONFIG["data"]["goods"])

    loadAllThreads()
    # loadAllTransactions()
    # doLoadTrTask(CONFIG["data"]["transactions"] % 1)
    print_prof_data()
