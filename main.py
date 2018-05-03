#!/usr/bin/python3
import sys
import threading
import json
import argparse
import subprocess
import logging
import os
import time
import rados
import configparser
from itertools import cycle, count
from pprint import pprint
log = logging.getLogger(__name__)


def do_bench(secs, name, ioctx, data):
    b = a = time.monotonic()
    stop = a + secs
    ops = 0
    try:
        while b <= stop:
            ioctx.write(name, next(data))
            # oncomplete, onsafe
            #ioctx.aio_write_full(name, next(data), oncomplete=lambda *args: lock.release())
            #lock.acquire()
            b = time.monotonic()
            ops += 1
    finally:
        try:
            log.debug('Removing object %s.', name)
            ioctx.remove_object(name)
        except Exception as e:
            log.error('Failed to remove object %s: %r', name, e)
    return b-a, ops


def main():
    logging.basicConfig(level=logging.INFO)
    conf = {'keyring': './keyring.conf'}
    pool = 'single'
    secs = 10 # secs to benchmark
    bytesperobj = 4 * 1024 * 1024
    bigdata = cycle([os.urandom(bytesperobj), os.urandom(bytesperobj)])


    # TODO: ugly code. pass whole client name, not rados_id to Rados constructor
    config = configparser.ConfigParser()
    config.read(conf['keyring'])
    (client, rados_id) = config.sections()[0].split('.')
    if client != 'client':
        raise ValueError

    log.info('Getting map osd -> host.')
    osd2host = {}
    info = json.loads(subprocess.check_output(['ceph', 'osd', 'tree', '--format=json']).decode('utf-8'))
    for i in info['nodes']:
        if i ['type'] != 'host':
            continue
        for j in i['children']:
            osd2host[j] = i['name']

    log.info('Getting pg => acting set.')
    info = json.loads(subprocess.check_output(['ceph', '--format=json', 'osd', 'pool', 'stats', 'single']).decode('utf-8'))
    pool_id = info[0]['pool_id']
    info = json.loads(subprocess.check_output(['ceph', '--format=json', 'pg', 'dump', 'pgs_brief']).decode('utf-8'))
    pgid2acting = {i['pgid']:tuple(i['acting']) for i in info if i['pgid'].startswith(str(pool_id))}

    MODE = 'HOST'

    if MODE == 'HOST':
        bench_items = set(tuple(osd2host[i] for i in osds) for osds in pgid2acting.values())
    else:
        bench_items = set(pgid2acting.values())

    obj2info = dict()
    cnt = 0
    log.info('Figuring out object names for %d %s combinations.', len(bench_items), MODE)
    while bench_items:
        cnt = cnt + 1
        name = 'bench_%d' % cnt
        info = json.loads(subprocess.check_output(['ceph', '-f', 'json', 'osd', 'map', pool, name]).decode('utf-8'))
        acting = tuple(info['acting'])
        hosts = tuple(osd2host[osd] for osd in acting)

        if MODE == 'HOST':
            bench_item = hosts
        else:
            bench_item = acting

        if bench_item not in bench_items:
            continue
        bench_items.remove(bench_item)

        obj2info[name] = (hosts, acting)

    obj2info=dict(sorted(obj2info.items(), key=lambda i: i[1]))

    # obj2info={ k:v for k,v in obj2info.items() if v[0] == ('node1',)}

#    lock = threading.Lock()
#    lock.acquire()
    log.debug('Attaching to CEPH cluster.')
    with rados.Rados(conffile='/etc/ceph/ceph.conf', rados_id=rados_id, conf=conf) as cluster:
        log.debug('Opening IO context for pool %s.', pool)
        with cluster.open_ioctx(pool) as ioctx:
            log.info('Start benchmarking of %d %ss. %d*2 seconds each.', len(obj2info), MODE, secs)
            for (name, (hosts, acting)) in obj2info.items():

                delay, ops = do_bench(secs, name, ioctx, cycle([b'q', b'w']))
                iops = ops / delay
                lat = delay / ops # in sec

                delay, ops = do_bench(secs, name, ioctx, bigdata)
                bsec = ops * bytesperobj / delay

                log.info(
                    'OSD %r (%r): %2.2f IOPS, lat=%.4f ms. %2.2f MB/sec (%2.2f MBit/s).',
                    acting,
                    hosts,
                    iops,
                    lat * 1000,
                    bsec / 1000000,
                    bsec * 8 / 1000000,
                )


if __name__ == '__main__':
    main()
