#include <exception>
#include <map>
#include <memory>
#include <set>
#include <string>

namespace Json {
class Reader;
class FastWriter;
class Value;
} // namespace Json

namespace librados {
class Rados;
}

class RadosUtils {
public:
  explicit RadosUtils(librados::Rados *rados_);
  unsigned int get_obj_acting_primary(const std::string &name,
                                      const std::string &pool);
  std::map<std::string, std::string> get_osd_location(unsigned int osd);
  std::set<unsigned int> get_osds(const std::string &pool);
  unsigned int get_pool_size(const std::string &pool);

private:
  Json::Value do_mon_command(Json::Value &cmd);
  librados::Rados *rados;
  std::unique_ptr<Json::Reader> json_reader;
  std::unique_ptr<Json::FastWriter> json_writer;
};

class MyRadosException : public std::exception {
public:
  MyRadosException(int err, const std::string &msg)
      : descr("Rados err " + std::to_string(err) + ": " + msg){};
  const char *what() const throw() { return descr.c_str(); }

private:
  std::string descr;
};
