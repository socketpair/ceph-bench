#!/usr/bin/python3

import json
import logging
import os
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from itertools import cycle

import rados

if sys.version_info >= (3, 0):
    from time import monotonic, sleep
else:
    from time import time as monotonic, sleep

log = logging.getLogger(__name__)


def do_bench(secs, name, ioctx, data):
    b = a = monotonic()
    stop = a + secs
    ops = 0
    try:
        while b <= stop:
            ioctx.write(name, next(data))
            b = monotonic()
            ops += 1
    finally:
        try:
            log.debug('Removing object %s.', name)
            ioctx.remove_object(name)
        except Exception as e:
            log.error('Failed to remove object %s: %r', name, e)
    return b - a, ops


def get_pool_size(cluster, pool):
    (ret, outbuf, outs) = cluster.mon_command(
        json.dumps({
            "prefix": "osd pool get",
            "pool": pool,
            "format": "json",
            "var": "size",
        }),
        '',
        0
    )
    if ret:
        raise RuntimeError(outs)
    result = json.loads(outbuf.decode('utf-8'))
    return result['size']


def get_pg2acting_primary(cluster, pool):
    (ret, outbuf, outs) = cluster.mgr_command(
        json.dumps({
            "prefix": "pg ls-by-pool",
            "poolstr": pool,
            "target": ["mgr", ""],
            "format": "json",
        }),
        '',
        0
    )
    if ret:
        raise RuntimeError(outs)
    result = json.loads(outbuf.decode('utf-8'))
    return {i['pgid']: i['acting_primary'] for i in result}


def get_osd_location(cluster, osd):
    (ret, outbuf, outs) = cluster.mon_command(
        json.dumps({
            "prefix": "osd find",
            "id": osd,
            "format": "json",
        }),
        '',
        0
    )
    if ret:
        raise RuntimeError(outs)
    result = json.loads(outbuf.decode('utf-8'))
    result = result['crush_location']
    result['osd'] = osd
    return result


def get_obj_acting_primary(cluster, pool, name):
    (ret, outbuf, outs) = cluster.mon_command(
        json.dumps({
            "prefix": "osd map",
            "object": name,
            "pool": pool,
            "format": "json",
        }),
        '',
        0
    )
    if ret:
        raise RuntimeError(outs)
    result = json.loads(outbuf.decode('utf-8'))
    return result['acting_primary']


def get_description(cluster, location):
    osd = location['osd']
    (ret, outbuf, outs) = cluster.mon_command(
        json.dumps({
            "prefix": "osd metadata",
            "id": osd,
            "format": "json",
        }),
        '',
        0
    )
    if ret:
        raise RuntimeError(outs)
    result = json.loads(outbuf.decode('utf-8'))

    descr = location.copy()
    descr["rot_journal"] = int(result["journal_rotational"]) == 1
    descr["rot_data"] = int(result["rotational"]) == 1
    descr["type"] = result["osd_objectstore"]
    return json.dumps(descr, sort_keys=True, ensure_ascii=False)


def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--debug', action='store_true', help='Enable debug mode.')
    parser.add_argument('--duration', type=int, default=10, help='Time limit for each test.', metavar='SECONDS')
    parser.add_argument('--bigsize', type=int, default=4 * 1024 * 1024, help='Size of object for linear write.',
                        metavar='BYTES')
    parser.add_argument('--smallsize', type=int, default=1, help='Size of object for linear IOPS write test.',
                        metavar='BYTES')
    parser.add_argument('--keyring', type=str, default='./keyring.conf', help='Path to keyring file.', metavar='PATH')
    parser.add_argument('pool', help='Ceph pool name.')
    parser.add_argument('mode', default='host',
                        help='Test item selection. Possible values: any key from crush location, e.g. "host", "root". And also special value "osd" to test each OSD.')

    params = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if params.debug else logging.INFO)
    conf = {'keyring': params.keyring}
    pool = params.pool
    mode = params.mode
    secs = params.duration
    bigsize = params.bigsize
    smallsize = params.bigsize * 1024 * 1024

    bigdata = cycle([os.urandom(bigsize), os.urandom(bigsize)])
    smalldata = cycle([os.urandom(smallsize), os.urandom(smallsize)])

    if next(smalldata) == next(smalldata):
        raise ValueError('You are looser.')

    log.info('Attaching to CEPH cluster. pool=%s', pool)
    with rados.Rados(conffile='/etc/ceph/ceph.conf', conf=conf) as cluster:
        sleep(0.1)  # https://tracker.ceph.com/issues/24114

        log.debug('Checking that pool %r size is 1.', pool)
        if get_pool_size(cluster, pool) != 1:
            raise RuntimeError('Pool %r size must be 1.' % pool)

        log.debug('Getting map of pg => acting_primary for pool %r.', pool)
        pg2acting_primary = get_pg2acting_primary(cluster, pool)
        # osds = sorted({j for i in pg2acting.values() for j in i})  # for 'acting' and size >= 1  (instead of acting_primary)
        osds = sorted({i for i in pg2acting_primary.values()})  # since size is 1
        log.debug('Got info about %d PGs. Total OSDs in this pool: %d.', len(pg2acting_primary), len(osds))

        log.info('Getting OSD locations.')
        osd2location = {osd: get_osd_location(cluster, osd) for osd in osds}

        bench_items = set(v[mode] for v in osd2location.values())
        totlen = len(bench_items)
        log.info('Figuring out object names for %d %ss.', totlen, mode)
        name2location = []
        cnt = 0
        while bench_items:
            cnt = cnt + 1
            name = 'bench_%d' % cnt

            osd = get_obj_acting_primary(cluster, pool, name)
            location = osd2location[osd]
            bench_item = location[mode]

            if bench_item in bench_items:
                bench_items.remove(bench_item)
                log.info('Found %d/%d', totlen - len(bench_items), totlen)
                description = get_description(cluster, location)
                name2location.append((name, bench_item, description))

        name2location = sorted(name2location, key=lambda i: i[1])  # sort object names by bench item.

        log.debug('Opening IO context for pool %s. Each benchmark will last %d secs.', pool, secs)
        with cluster.open_ioctx(pool) as ioctx:
            log.info('Start write IOPS benchmarking of %d %ss.', len(name2location), mode)
            for (name, bench_item, description) in name2location:
                log.debug('Benchmarking write IOPS on %r', bench_item)
                delay, ops = do_bench(secs, name, ioctx, smalldata)
                iops = ops / delay
                lat = delay / ops  # in sec
                log.info(
                    '%s %r: %2.2f IOPS, lat=%.4f ms. %s.',
                    mode,
                    bench_item,
                    iops,
                    lat * 1000,
                    description,
                )

            log.info('Start Linear write benchmarking of %d %ss. blocksize=%d MiB.', len(name2location), mode,
                     bigsize // (1024 * 1024))
            for (name, bench_item, description) in name2location:
                log.debug('Benchmarking Linear write on %r', bench_item)
                delay, ops = do_bench(secs, name, ioctx, bigdata)
                bsec = ops * bigsize / delay
                log.info(
                    '%s %r: %2.2f MB/sec (%2.2f Mbit/s). %s.',
                    mode,
                    bench_item,
                    bsec / 1000000,
                    bsec * 8 / 1000000,
                    description,
                )


if __name__ == '__main__':
    main()
