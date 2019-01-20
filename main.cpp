#include <chrono>
#include <csignal>
#include <iostream>
#include <json/json.h>
#include <librados.hpp>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

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

template <class T>
static void print_breakdown(const vector<T> &summary, size_t thread_count) {

  T totaltime(0);

  map<size_t, size_t> dur2count;
  map<size_t, T> dur2totaltime;
  static const size_t usecs[] = {
      100,   200,   300,   400,   500,   600,   700,   800,    900,       1000,
      2000,  3000,  4000,  5000,  6000,  7000,  8000,  9000,   10000,     20000,
      30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000, 1000000000};

  for (const auto &m : usecs) {
    dur2count[m] = 0;
    dur2totaltime[m] = T(0);
  }

  T mindur(minutes(42));
  T maxdur(0);
  for (const auto &res : summary) {
    totaltime += res;
    if (res > maxdur)
      maxdur = res;
    if (res < mindur)
      mindur = res;
    for (const auto &m : usecs) {
      if (res <= microseconds(m)) {
        dur2count.at(m)++;
        dur2totaltime.at(m) += res;
        break;
      }
    }
  }

  cout << "min delay " << dur2msec(mindur) << " msec." << endl;
  cout << "max delay " << dur2msec(maxdur) << " msec." << endl;

  // skip first counters equal zero
  auto b = dur2count.begin();
  while (b != dur2count.end() && !b->second)
    b++;

  // skip last counters equal zero
  auto e = dur2count.end();
  if (e != b) {
    e--;
    while (e != b && !e->second)
      e--;
    e++;
  }

  size_t maxcount = 0;
  for (auto p = b; p != e; p++) {
    const auto &usecgroup = p->first;
    const auto &count = p->second;
    //      const auto &timespent = dur2totaltime.at(usecgroup);

    if (count > maxcount)
      maxcount = count;

    auto bar = std::string(count * 70 / summary.size(), '#');
    if (usecgroup == 1000000000)
      cout << ">   100";
    else
      cout << "<=" << setw(5) << usecgroup / 1000.0;
    cout << " ms: " << setw(3) << count * 100 / summary.size() << "% " << bar
         << " cnt=" << count;
    //     cout << " (" << (count * thread_count) / dur2sec(timespent)
    //           << " IOPS)";
    cout << endl;
  }

  size_t sum = 0;
  T sumtime(0);
  for (auto p = b; p != e; p++) {
    const auto &usecgroup = p->first;
    const auto &count = p->second;
    if (count > maxcount / 100.0) {
      sum += count;
      sumtime += dur2totaltime.at(usecgroup);
    }
  }

  cout << "ops: " << (summary.size() * thread_count) / dur2sec(totaltime)
       << endl;

  cout << "ops (count > 0.01 of max): "
       << (sum * thread_count) / dur2sec(sumtime) << endl;

  if (thread_count > 1)
    cout << "ops per thread: " << summary.size() / dur2sec(totaltime) << endl;
}

// Called in a thread.
static void _do_bench(unsigned int secs, const string &obj_name, IoCtx &ioctx,
                      vector<steady_clock::duration> *ops) {

  //      cout<<"tt" <<ops<<endl;

  bufferlist bar1;
  bufferlist bar2;
  // interleave buffers
  // TODO: 4096in order not to read on writer ! But anyway
  // during test this block should be cached
  bar1.append("q");
  bar2.append("w");

  auto b = steady_clock::now();
  const auto stop = b + seconds(secs);
  try {
    while (b <= stop) {
      abort_if_signalled();
      if (ioctx.write_full(obj_name, ops->size() % 2 ? bar1 : bar2) < 0)
        throw "Write error";
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
                     IoCtx &ioctx) {

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

      threads.push_back(thread(_do_bench, secs, name, ref(ioctx), results));

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
    _do_bench(secs, names.at(0), ioctx, &summary);
  }
  print_breakdown(summary, names.size());
}

static void _main(int argc, const char *argv[]) {
  struct {
    string pool;
    string mode;
    string specific_bench_item;
    unsigned int threads;
    unsigned int secs;
  } settings;

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
      do_bench(settings.secs, obj_names, ioctx);
    }
  } catch (...) {
    rados.watch_flush();
    throw;
  }
  rados.watch_flush();
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
