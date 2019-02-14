#include <chrono>
#include <cmath>
#include <csignal>
#include <iostream>
#include <librados.hpp>
#include <map>
#include <set>
#include <string>
#ifdef YA_PIDOR
#include <fstream>
#else
#include <sys/random.h>
#endif
#include <thread>
#include <vector>
#include <system_error>

// TODO: RMOVE IT !
#include <json/json.h>

#include "mysignals.h"
#include "radosutil.h"

using namespace librados;
using namespace std;
using namespace chrono;

template <class T> static double dur2sec(const T &dur) {
  return duration_cast<duration<double>>(dur).count();
}

template <class T> static double dur2msec(const T &dur) {
  return duration_cast<duration<double, milli>>(dur).count();
}

template <class T> static uint64_t dur2nsec(const T &dur) {
  return duration_cast<duration<uint64_t, nano>>(dur).count();
}

template <class T>
static void print_breakdown(const vector<T> &summary, size_t thread_count,
                            size_t block_size) {

  T totaltime(0);

  map<size_t, size_t> dur2count;
  map<size_t, T> dur2totaltime;

  T mindur(minutes(42));
  T maxdur(0);
  size_t maxcount = 0;
  for (const auto &res : summary) {
    totaltime += res;
    if (res > maxdur)
      maxdur = res;
    if (res < mindur)
      mindur = res;

    const auto nsec = dur2nsec(res);
    const size_t baserange = powl(10, size_t(log10((long double)nsec)));
    const size_t range = (nsec / baserange) * baserange;

    const auto cnt = ++(dur2count[range]);
    if (cnt > maxcount)
      maxcount = cnt;

    dur2totaltime[range] += res;
  }

  cout << "min delay " << dur2msec(mindur) << " msec." << endl;
  cout << "max delay " << dur2msec(maxdur) << " msec." << endl;

  size_t sum = 0;
  T sumtime(0);
  const size_t maxbarsize = 30;

  auto x = [block_size](size_t count, T dur) -> void {
    cout << " cnt=" << count;
    cout << ", ";
    cout << (count / dur2sec(dur)) << " IOPS";
    cout << ", ";
    cout << (count * block_size / (dur2sec(dur) * 1000000.0)) << " MB/s";
    cout << ", ";
    cout << (count * block_size * 8 / (dur2sec(dur) * 1000000.0)) << " Mb/s";
  };

  for (const auto &p : dur2count) {
    const auto &nsecgrp = p.first;
    const auto &count = p.second;
    const auto barsize = count * maxbarsize / maxcount;

    auto bar = string(barsize, '#') + string(maxbarsize - barsize, ' ');
    cout << ">=" << setw(5) << nsecgrp / 1000000.0;
    cout << " ms: " << setw(3) << count * 100 / summary.size() << "% " << bar;
    x(count, dur2totaltime.at(nsecgrp));
    cout << endl;
    if (count > maxcount / 100.0) {
      sum += count;
      sumtime += dur2totaltime.at(nsecgrp);
    }
  }

  cout << "ops: " << (summary.size() * thread_count) / dur2sec(totaltime)
       << endl;

  cout << "ops (count > 0.01 of max): ";
  x(sum * thread_count, sumtime);
  cout << endl;

  if (thread_count > 1)
    cout << "ops per thread: " << summary.size() / dur2sec(totaltime) << endl;
}

static void fill_urandom(void *buf_, size_t len) {
  char *buf = static_cast<char *>(buf_);

#ifdef YA_PIDOR
  ifstream infile;
  infile.exceptions(ifstream::failbit | ifstream::badbit);
  infile.open("/dev/urandom", ios::binary | ios::in);
  infile.read(buf, len);
#else
  while (len) {
    ssize_t res;
    if ((res = getrandom(buf, len, 0)) == -1)
      throw system_error(errno, system_category(),
                         "Failed to get random bytes");
    buf += res;
    len -= res;
  }
#endif
}

// May be called in a thread.
static void _do_bench(unsigned int secs, const string &obj_name, IoCtx &ioctx,
                      vector<steady_clock::duration> *ops, size_t block_size) {

  // TODO: pass bufferlist as arguments
  bufferlist bar1;
  bufferlist bar2;

  bar1.append(ceph::buffer::create(block_size));
  fill_urandom(bar1.c_str(), block_size);

  bar2.append(ceph::buffer::create(block_size));
  fill_urandom(bar2.c_str(), block_size);

  if (bar1.contents_equal(bar2))
    throw "You are looser";

  //  utime_t end = ceph_clock_now(); ?!
  auto b = steady_clock::now();
  const auto stop = b + seconds(secs);
  try {
    while (b <= stop) {
      abort_if_signalled();

      {
        unique_ptr<AioCompletion, void (*)(AioCompletion *)> compl2(
            Rados::aio_create_completion(NULL, NULL, NULL),
            [](AioCompletion *x) { x->release(); });

        if (ioctx.aio_write_full(obj_name, compl2.get(),
                                 (ops->size() % 2) ? bar1 : bar2) < 0)
          throw "Write error";

        if (compl2->wait_for_safe() < 0)
          throw "Error waiting to be safe";
      }
      const auto b2 = steady_clock::now();
      ops->push_back(b2 - b);
      b = b2;
    }
  } catch (...) {
    ioctx.remove(obj_name); // ignore errors.
    throw;
  }
  ioctx.remove(obj_name); // ignore errors.
}

static void do_bench(unsigned int secs, const vector<string> &names,
                     IoCtx &ioctx, size_t block_size) {

  vector<steady_clock::duration> summary;

  if (names.size() > 1) {
    vector<thread> threads;
    vector<vector<steady_clock::duration> *> listofopts;

    for (const auto &name : names) {

      // TODO: memory leak on exception...
      auto results = new vector<steady_clock::duration>;
      listofopts.push_back(results);

      sigset_t new_set;
      sigset_t old_set;
      sigfillset(&new_set);
      int err;
      if ((err = pthread_sigmask(SIG_SETMASK, &new_set, &old_set)))
        throw std::system_error(err, std::system_category(),
                                "Failed to set thread sigmask");

      threads.push_back(
          thread(_do_bench, secs, name, ref(ioctx), results, block_size));

      if ((err = pthread_sigmask(SIG_SETMASK, &old_set, NULL)))
        throw std::system_error(err, std::system_category(),
                                "Failed to restore thread sigmask");
    }

    for (auto &th : threads)
      th.join();

    // just an optimisation :)
    size_t qwe = 0;
    for (const auto &res : listofopts)
      qwe += res->size();
    summary.reserve(qwe);

    for (const auto &res : listofopts) {
      summary.insert(summary.end(), res->begin(), res->end());
      delete res;
    }
  } else {
    _do_bench(secs, names.at(0), ioctx, &summary, block_size);
  }
  print_breakdown(summary, names.size(), block_size);
}

static void _main(int argc, const char *argv[]) {
  struct {
    string pool;
    string mode;
    string specific_bench_item;
    unsigned int threads;
    unsigned int secs;
    size_t block_size;
  } settings;

  switch (argc) {
  case 3:
    settings.pool = argv[1];
    settings.mode = argv[2];
    break;
  case 4:
    settings.pool = argv[1];
    settings.mode = argv[2];
    settings.specific_bench_item = argv[3];
    break;
  default:
    cerr << "Usage: " << argv[0]
         << " [poolname] [mode=host|osd] <specific item name to test>" << endl;
    throw "Wrong cmdline";
  }

  settings.secs = 10;
  settings.threads = 1;
  settings.block_size = 4096 * 1024;

  Rados rados;
  int err;
  if ((err = rados.init("admin")) < 0) {
    cerr << "Failed to init: " << strerror(-err) << endl;
    throw "Failed to init";
  }

  if ((err = rados.conf_read_file("/etc/ceph/ceph.conf")) < 0) {
    cerr << "Failed to read conf file: " << strerror(-err) << endl;
    throw "Failed to read conf file";
  }

  if ((err = rados.conf_parse_argv(argc, argv)) < 0) {
    cerr << "Failed to parse argv: " << strerror(-err) << endl;
    throw "Failed to parse argv";
  }

  if ((err = rados.connect()) < 0) {
    cerr << "Failed to connect: " << strerror(-err) << endl;
    throw "Failed to connect";
  }

  // https://tracker.ceph.com/issues/24114
  this_thread::sleep_for(milliseconds(100));

  try {
    auto rados_utils = RadosUtils(&rados);

    if (rados_utils.get_pool_size(settings.pool) != 1)
      throw "It's required to have pool size 1";

    map<unsigned int, map<string, string>> osd2location;

    set<string> bench_items; // node1, node2 ||| osd.1, osd.2, osd.3

    for (const auto &osd : rados_utils.get_osds(settings.pool)) {
      const auto &location = rados_utils.get_osd_location(osd);

      // TODO: do not fill this map if specific_bench_item specified
      osd2location[osd] = location;

      const auto &qwe = location.at(settings.mode);
      if (settings.specific_bench_item.empty() ||
          qwe == settings.specific_bench_item) {
        bench_items.insert(qwe);
      }
    }

    // benchitem -> [name1, name2] ||| i.e. "osd.2" => ["obj1", "obj2"]
    map<string, vector<string>> name2location;
    unsigned int cnt = 0;

    // for each bench_item find thread_count names.
    // store every name in name2location = [bench_item, names, description]
    const string prefix = "bench_";
    while (bench_items.size()) {
      string name = prefix + to_string(++cnt);

      unsigned int osd =
          rados_utils.get_obj_acting_primary(name, settings.pool);

      const auto &location = osd2location.at(osd);
      const auto &bench_item = location.at(settings.mode);
      if (!bench_items.count(bench_item))
        continue;

      auto &names = name2location[bench_item];
      if (names.size() == settings.threads) {
        bench_items.erase(bench_item);
        continue;
      }

      names.push_back(name);

      cout << name << " - " << bench_item << endl;
    }

    IoCtx ioctx;

    if (rados.ioctx_create(settings.pool.c_str(), ioctx) < 0)
      throw "Failed to create ioctx";

    for (const auto &p : name2location) {
      const auto &bench_item = p.first;
      const auto &obj_names = p.second;
      cout << "Benching " << settings.mode << " " << bench_item << endl;
      do_bench(settings.secs, obj_names, ioctx, settings.block_size);
    }
  } catch (...) {
    rados.watch_flush();
    throw;
  }
  rados.watch_flush();

  // rados_ioctx_destroy(io);
  // rados_shutdown(cluster);
}

int main(int argc, const char *argv[]) {
  try {
    setup_signal_handlers();
    _main(argc, argv);
  } catch (const AbortException &msg) {
    cerr << "Test aborted" << endl;
    return 1;
  } catch (const char *msg) {
    cerr << "Unhandled exception: " << msg << endl;
    return 2;
  }
  cout << "Exiting successfully." << endl;
  return 0;
}
