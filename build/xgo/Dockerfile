FROM karalabe/xgo-1.7.1

MAINTAINER Tamir Duberstein <tamird@gmail.com>

# Install mingw-w64 headers from source to pick up missing Secure Template Overloads.
#
# See https://github.com/karalabe/xgo/issues/52.
ADD https://sourceforge.net/p/mingw-w64/mingw-w64/ci/master/tree/mingw-w64-headers/crt/sec_api/string_s.h?format=raw /usr/share/mingw-w64/include/sec_api/
