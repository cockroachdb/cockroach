
#include <google/protobuf/stubs/stringprintf.h>
#include <string>

std::string FormatString(const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  std::string str;
  google::protobuf::StringAppendV(&str, fmt, ap);
  va_end(ap);
  return str;
}
