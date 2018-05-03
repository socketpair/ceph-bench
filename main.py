#!/usr/bin/python3
import sys
import json
import argparse
import logging
import os
import time
import rados
import configparser
from itertools import cycle, count
from pprint import pprint

import ceph_argparse

log = logging.getLogger(__name__)

def do_bench(secs, name, ioctx, data):
    b = a = time.monotonic()
    stop = a + secs
    ops = 0
    try:
        while b <= stop:
            ioctx.write(name, next(data))
            b = time.monotonic()
            ops += 1
    finally:
        try:
            log.debug('Removing object %s.', name)
            ioctx.remove_object(name)
        except Exception as e:
            log.error('Failed to remove object %s: %r', name, e)
    return b-a, ops

def _cmd(cluster, cmd, **kwargs):
    target = ceph_argparse.find_cmd_target(cmd.split())

    qwe = {
            'prefix': cmd,
            'target': target,
            'format': 'json',
    }
    qwe.update(kwargs)
    ret, outbuf, outs = ceph_argparse.json_command(
        cluster,
        target=target,
        prefix=None,
        argdict=qwe
    )
    if ret:
        raise RuntimeError(outs)
    return json.loads(outbuf)



def main():
    logging.basicConfig(level=logging.INFO)
    conf = {'keyring': 'keyring.conf'}
    pool = 'single'
    MODE = 'HOST'  # HOST or OSD
    secs = 10 # secs to benchmark
    bytesperobj = 4 * 1024 * 1024
    bigdata = cycle([os.urandom(bytesperobj), os.urandom(bytesperobj)])

    assert MODE in ('HOST', 'OSD')

    # TODO: ugly code. pass whole client name, not rados_id to Rados constructor
    config = configparser.ConfigParser()
    config.read(conf['keyring'])
    (client, rados_id) = config.sections()[0].split('.')
    if client != 'client':
        raise ValueError

    log.info('Attaching to CEPH cluster. pool=%s, rados_id=%s', pool, rados_id)
    with rados.Rados(conffile='/etc/ceph/ceph.conf', rados_id=rados_id, conf=conf) as cluster:
        log.info('Getting map osd -> host.')
        #info = json.loads(subprocess.check_output(['ceph', 'osd', 'tree', '--format=json']).decode('utf-8'))
        info = _cmd(cluster, 'osd tree')
        osd2host = {}
        for i in info['nodes']:
            if i ['type'] != 'host':
                continue
            for j in i['children']:
                osd2host[j] = i['name']
        pool_id = cluster.pool_lookup(pool)


        log.info('Getting pg => acting set.')
        #info = json.loads(subprocess.check_output(['ceph', '--format=json', 'pg', 'dump', 'pgs_brief']).decode('utf-8'))
        info = _cmd(cluster, 'pg dump', dumpcontents=['pgs_brief'])


        pgid2acting = {i['pgid']:tuple(i['acting']) for i in info if i['pgid'].startswith(str(pool_id))}
        if MODE == 'HOST':
            bench_items = set(tuple(osd2host[i] for i in osds) for osds in pgid2acting.values())
        else:
            bench_items = set(pgid2acting.values())


        log.info('Figuring out object names for %d %s combinations.', len(bench_items), MODE)
        obj2info = dict()
        cnt = 0
        totlen=len(bench_items)
        while bench_items:
            cnt = cnt + 1
            name = 'bench_%d' % cnt

            #info = json.loads(subprocess.check_output(['ceph', '-f', 'json', 'osd', 'map', pool, name]).decode('utf-8'))
            info = _cmd(cluster, 'osd map', object=name, pool=pool)

            acting = tuple(info['acting'])
            hosts = tuple(osd2host[osd] for osd in acting)

            if MODE == 'HOST':
                bench_item = hosts
            else:
                bench_item = acting

            if bench_item not in bench_items:
                continue

            bench_items.remove(bench_item)
            log.info('Found %d/%d', totlen-len(bench_items), totlen)

            obj2info[name] = (hosts, acting)

        obj2info=dict(sorted(obj2info.items(), key=lambda i: i[1]))

        log.debug('Opening IO context for pool %s.', pool)
        with cluster.open_ioctx(pool) as ioctx:
            log.info('Start benchmarking of %d %ss. %d * 2 seconds each.', len(obj2info), MODE, secs)
            for (name, (hosts, acting)) in obj2info.items():
                log.info('Benchmarking IOPS on OSD %r (%r)', list(acting),list(hosts))
                delay, ops = do_bench(secs, name, ioctx, cycle([b'q', b'w']))
                iops = ops / delay
                lat = delay / ops # in sec
                log.info('Benchmarking Linear write on OSD %r (%r) blocksize=%d MiB', list(acting),list(hosts), bytesperobj//(1024*1024))
                delay, ops = do_bench(secs, name, ioctx, bigdata)
                bsec = ops * bytesperobj / delay

                log.info(
                    'OSD %r (%r): %2.2f IOPS, lat=%.4f ms. %2.2f MB/sec (%2.2f Mbit/s).',
                    list(acting),
                    list(hosts),
                    iops,
                    lat * 1000,
                    bsec / 1000000,
                    bsec * 8 / 1000000,
                )


if __name__ == '__main__':
    main()
