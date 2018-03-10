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
from optparse import OptionParser

import memcache

import appsinstalled_pb2

NORMAL_ERR_RATE = 0.01
THREADS_COUNT = 3
WORKERS_COUNT = 4
PATTERN = "/data/appsinstalled/*.tsv.gz"
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class ThreadInsert(threading.Thread):
    def __init__(self, queue, out_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue

    def run(self):
        while True:
            args = self.queue.get()
            ok = insert_appsinstalled(*args)
            self.out_queue.put(ok)
            self.queue.task_done()


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception, e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


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
    in_queue = Queue.Queue()
    out_queue = Queue.Queue()
    start = time.time()
    filename, options, device_memc = args
    for i in range(options.threads_count):
        t = ThreadInsert(in_queue, out_queue)
        t.setDaemon(True)
        t.start()
    processed = errors = 0
    logging.info('Processing %s' % filename)
    fd = gzip.open(filename)

    for line in fd:
        line = line.strip()
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknown device type: %s" % appsinstalled.dev_type)
            continue
        in_queue.put((memc_addr, appsinstalled, options.dry))
    in_queue.join()

    while not out_queue.empty():
        ok = out_queue.get()
        out_queue.task_done()
        if ok:
            processed += 1
        else:
            errors += 1
    out_queue.join()
    if not processed:
        fd.close()
        dot_rename(filename)
        return

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    fd.close()
    dot_rename(filename)
    logging.info("Worker with {0} End at {1}".format(fd.filename, time.time() - start))


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
    pool.map(worker, args_list)


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
    op.add_option("--threads_count", action="store", default=THREADS_COUNT, type='int', help='count of threads')
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        import time
        start = time.time()
        main(opts)
        logging.info('enf of {0}'.format(time.time()-start))
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
