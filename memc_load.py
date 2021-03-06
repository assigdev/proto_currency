#!/usr/bin/env python
# -*- coding: utf-8 -*-
import Queue
import collections
import glob
import gzip
import logging
import multiprocessing as mp
import os
import sys
import threading
import time
from optparse import OptionParser

import memcache

import appsinstalled_pb2

NORMAL_ERR_RATE = 0.01
THREADS_COUNT = 3
WORKERS_COUNT = 4
MEMC_TIMEOUT = 5
PORTION_SIZE = 10
RETRY_COUNT = 4
PATTERN = "/data/appsinstalled/*.tsv.gz"
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class ThreadInsert(threading.Thread):
    def __init__(self, queue, out_queue, connection, retry_count, dry):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        self.connection = connection
        self.retry_count = retry_count
        self.dry = dry
        self.memc_addr = self.get_memc_addr()
        self.processed = 0
        self.errors = 0

    def run(self):
        while True:
            out = self.queue.get()
            if out == 'Done':
                self.queue.task_done()
                self.out_queue.put((self.processed, self.errors))
            else:

                processed, errors = self.insert_appsinstalled(out)
                self.processed += processed
                self.errors += errors
                self.queue.task_done()

    def get_memc_addr(self):
        address = self.connection.buckets[0].address
        return "{0}:{1}".format(address[0], address[1])

    def insert_appsinstalled(self,  out):
        processed = 0
        errors = 0
        result = []
        items = {}
        for args in out:
            appsinstalled, packed, ua = args
            key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
            if self.dry:
                logging.debug("%s - %s -> %s" % (self.memc_addr, key, str(ua).replace("\n", " ")))
                processed += 1
            else:
                items[key] = packed
        if not self.dry:
            try:
                connect_num = 0
                while result != [] and connect_num < self.retry_count:
                    result = self.connection.set_multi(items)
                    connect_num += 1
            except ValueError, e:
                logging.exception("Cannot write to memc %s: %s" % (self.memc_addr, e))
                result = len(items)

            if len(result) > 0:
                for key in result:
                    logging.error("Cannot set data to memc %s key: %s" % (self.memc_addr, key))
                errors += len(result)
                processed += len(items) - len(result)
            else:
                processed += len(items)
        return processed, errors


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def worker(args):
    in_queues = {}
    args_portions = {}
    out_queue = Queue.Queue()
    start = time.time()
    filename, options, device_memc = args

    connections = create_memcache_connections(device_memc, options.memc_timeout)
    for key, connection in connections.items():
        in_queue = Queue.Queue()
        in_queues[key] = in_queue
        args_portions[key] = []
        t = ThreadInsert(in_queue, out_queue, connection, options.retry_count, options.dry)
        t.setDaemon(True)
        t.start()
    processed = errors = 0
    logging.info('Processing %s' % filename)
    fd = gzip.open(filename)
    args_portion = []
    for line in fd:
        line = line.strip()
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue
        dev_type = appsinstalled.dev_type
        memc_addr = device_memc.get(dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknown device type: %s" % appsinstalled.dev_type)
            continue
        packed, ua = serialize_data(appsinstalled)
        args_portions[dev_type].append((appsinstalled, packed, ua))

        if len(args_portions[dev_type]) == options.portion_size:
            in_queues[dev_type].put(args_portions[dev_type])
            args_portions[dev_type] = []
    for in_queue in in_queues.values():
        if len(args_portion):
            in_queue.put(args_portion)
        in_queue.put('Done')
        in_queue.join()

    while not out_queue.empty():
        processed_chunk, errors_chunk = out_queue.get()
        processed += processed_chunk
        errors += errors_chunk
        out_queue.task_done()

    out_queue.join()
    if not processed:
        fd.close()
        return filename

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    fd.close()
    logging.info("Worker with {0} End at {1}".format(fd.filename, time.time() - start))
    return filename


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def create_memcache_connections(device_memc, memc_timeout):
    memcache_connections = {}
    for key, value in device_memc.items():
        memcache_connections[key] = memcache.Client([value], socket_timeout=memc_timeout)
    return memcache_connections


def serialize_data(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return packed, ua


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    pool = mp.Pool(int(options.workers_count))
    filename_list = sorted([fn for fn in glob.iglob(options.pattern)])
    args_list = [(filename, options, device_memc) for filename in filename_list]

    for filename in pool.imap(worker, args_list):
        dot_rename(filename)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t123,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False, help='run test fixtures')
    op.add_option("-l", "--log", action="store", default=None, help='Log file path')
    op.add_option("--dry", action="store_true", default=False, help='Debug mode')
    op.add_option("--pattern", action="store", default=PATTERN, help='Pattern for files path')
    op.add_option("--idfa", action="store", default="127.0.0.1:33013", help='memcash  ip:port for iphone ids')
    op.add_option("--gaid", action="store", default="127.0.0.1:33014", help='memcash  ip:port for android gaid')
    op.add_option("--adid", action="store", default="127.0.0.1:33015", help='memcash  ip:port for android adid')
    op.add_option("--dvid", action="store", default="127.0.0.1:33016", help='memcash  ip:port for android dvid')
    op.add_option("-w", "--workers_count", action="store", default=WORKERS_COUNT, type='int', help='count of workers')
    op.add_option("--portion_size", action="store", default=PORTION_SIZE, type='int', help='portion size for queue')
    op.add_option(
        "--retry_count",
        action="store",
        default=RETRY_COUNT,
        type='int',
        help='count of retry set data in memcache')
    op.add_option(
        "--memc_timeout",
        action="store",
        default=MEMC_TIMEOUT,
        type='int',
        help='timeout for memcache connection'
    )
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
