#!/usr/bin/python3
# coding: utf-8

import json
import logging
import os
import signal
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from itertools import cycle
from threading import Thread

import rados

if sys.version_info >= (3, 0):
    from time import monotonic, sleep
else:
    from time import time as monotonic, sleep

log = logging.getLogger(__name__)

DO_ABORT = False


def _do_bench(secs, name, ioctx, data):
    ops = []
    data = cycle(data)
    b = monotonic()
    stop = b + secs
    try:
        while not DO_ABORT and b <= stop:
            ioctx.write(name, next(data))
            b2 = monotonic()
            ops.append(b2 - b)
            b = b2
    finally:
        try:
            log.debug('Removing object %s.', name)
            ioctx.remove_object(name)
        except Exception as e:
            log.error('Failed to remove object %s: %r', name, e)
    return ops


def signal_handler(*args):
    global DO_ABORT
    log.info('Aborted by signal.')
    DO_ABORT = True


def do_bench(secs, object_names, ioctx, data):
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    try:
        results = {}
        thrds = {
            # Appending is thread safe ?
            Thread(
                target=lambda name2: results.setdefault(name2, _do_bench(secs, name2, ioctx, data)),
                args=(name,)
            )
            for name in object_names
        }

        for i in thrds:
            i.start()

        # Thread.join() is not signal-interruptible (!)
        while thrds:
            for i in list(thrds):
                i.join(1)
                if not i.is_alive():
                    thrds.remove(i)
    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    if DO_ABORT:
        raise RuntimeError('Aborted')

    return results


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


def get_osds(cluster, pool):
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
    return {i['acting_primary'] for i in result}


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
    log.debug('Location of OSD %r is %r.', osd, result)
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
    acting_primary = result['acting_primary']
    log.debug('Acting primary OSD %r (for object %r).', acting_primary, name)
    return acting_primary


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

    if result["osd_objectstore"] == 'filestore':
        x = [
            'jrn=%s' % ('hdd' if int(result["journal_rotational"]) else 'ssd'),
            'dat=%s' % ('hdd' if int(result["rotational"]) else 'ssd'),
        ]
    elif result["osd_objectstore"] == 'bluestore':
        x = [
            'db=%s(%s)' % (result['bluefs_db_type'], result["bluefs_db_model"].rstrip()),
            'dat=%s(%s)' % (result['bluestore_bdev_type'], result["bluestore_bdev_model"].rstrip()),
        ]
    else:
        x = []

    return ' '.join(
        [
            'r=%s,h=%s,osd.%s' % (location['root'], location['host'], osd),
            result["osd_objectstore"],
        ] + x + [result['cpu']]
    )


def main():
    parser = ArgumentParser(
        description="Socketair Ceph tester. You should create pool of size 1 and provide a keyring file with user having rights to write to this pool.",
        formatter_class=ArgumentDefaultsHelpFormatter,
        epilog="For all questions contact Коренберг Марк <socketpair@gmail.com> and/or Telegram user @socketpair, as well as @socketpair on GitHub."
    )
    parser.add_argument('--debug', action='store_true', help='Enable debug mode.')
    parser.add_argument('--duration', type=int, default=10, help='Time limit for each test.', metavar='SECONDS')
    parser.add_argument('--bigsize', type=int, default=4 * 1024 * 1024, help='Size of object for linear write.',
                        metavar='BYTES')
    parser.add_argument('--smallsize', type=int, default=1, help='Size of object for linear IOPS write test.',
                        metavar='BYTES')
    parser.add_argument('--keyring', type=str, default='./keyring.conf', help='Path to keyring file.', metavar='PATH')
    parser.add_argument('--threads', type=int, default=1,
                        help='Parallel testing using multiple threads and different object in each.', metavar='COUNT')
    parser.add_argument('pool', help='Ceph pool name.')
    parser.add_argument('mode', default='host',
                        help='Test item selection. Possible values: any key from crush location, e.g. "host", "root". And also special value "osd" to test each OSD.')

    params = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if params.debug else logging.INFO,
        format='%(levelname)s:%(name)s:%(message)s' if params.debug else '%(message)s',
    )
    conf = {'keyring': params.keyring}
    pool = params.pool
    mode = params.mode
    secs = params.duration
    bigsize = params.bigsize
    smallsize = params.smallsize
    threads_count = params.threads

    bigdata = [os.urandom(bigsize), os.urandom(bigsize)]
    smalldata = [os.urandom(smallsize), os.urandom(smallsize)]

    if smalldata[0] == smalldata[1]:
        raise ValueError('You are looser.')

    log.info('Attaching to CEPH cluster. pool=%s', pool)
    with rados.Rados(conffile='/etc/ceph/ceph.conf', conf=conf) as cluster:
        sleep(0.1)  # https://tracker.ceph.com/issues/24114

        log.debug('Checking that pool %r size is 1.', pool)
        if get_pool_size(cluster, pool) != 1:
            raise RuntimeError('Pool %r size must be 1.' % pool)

        log.debug('Getting list of OSDs for pool %r.', pool)
        osds = sorted(get_osds(cluster, pool))
        log.debug('Total OSDs in this pool: %d.', len(osds))

        log.info('Getting OSD locations.')
        osd2location = {osd: get_osd_location(cluster, osd) for osd in osds}

        bench_items = set(v[mode] for v in osd2location.values())
        totlen = len(bench_items)
        log.info('Figuring out object names for %d %ss.', totlen, mode)
        name2location = {}  # benchitem -> ([name1, name2], description)
        cnt = 0
        foundcnt = 0

        while bench_items:
            cnt += 1
            name = 'bench_%d' % cnt

            osd = get_obj_acting_primary(cluster, pool, name)
            location = osd2location[osd]
            bench_item = location[mode]

            if bench_item not in bench_items:
                continue

            foundcnt += 1
            xxx = name2location.get(bench_item)
            if xxx is None:
                xxx = [[name], get_description(cluster, location) if threads_count == 1 else '*multiple*']
                name2location[bench_item] = xxx
            else:
                xxx[0].append(name)

            if len(xxx[0]) == threads_count:
                bench_items.remove(bench_item)

            log.info('Found %d/%d', foundcnt, totlen * threads_count)

        name2location = sorted(
            (
                (bench_item, names, descr)
                for bench_item, (names, descr) in name2location.items()
            ),
            key=lambda i: i[0]
        )

        log.debug('Opening IO context for pool %s. Each benchmark will last %d secs.', pool, secs)
        with cluster.open_ioctx(pool) as ioctx:
            log.info('Start write IOPS benchmarking of %d %ss with %d thread(s).', len(name2location), mode,
                     threads_count)
            for (bench_item, names, description) in name2location:
                log.debug('Benchmarking write IOPS on %r', bench_item)
                # { 'name1': [1.2, 3.4, 5.6, ...], 'name2': [...], ...}
                results = do_bench(secs, names, ioctx, smalldata)

                latencies = []
                for v in results.values():
                    latencies.extend(v)
                latencies.sort()

                elapsed = max(sum(v) for v in results.values())
                ops = sum(len(v) for v in results.values())

                iops = ops / elapsed
                log.info(
                    '%s %r: %2.2f IOPS (%2.2f ops), minlat=%.4f ms, maxlat=%.4f ms. %s.',
                    mode,
                    bench_item,
                    iops,
                    ops,
                    latencies[0] * 1000,
                    latencies[-1] * 1000,
                    description,
                )

            log.info('Start Linear write benchmarking of %d %ss. blocksize=%d MiB with %d thread(s).',
                     len(name2location), mode,
                     bigsize // (1024 * 1024), threads_count)
            for (bench_item, names, description) in name2location:
                log.debug('Benchmarking Linear write on %r', bench_item)
                results = do_bench(secs, names, ioctx, bigdata)

                elapsed = max(sum(v) for v in results.values())
                ops = sum(len(v) for v in results.values())

                bsec = ops * bigsize / elapsed
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
