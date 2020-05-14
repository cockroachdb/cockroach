#!/bin/bash
set -euxo pipefail

rm -f ../shim/*.c
rm -f ../wrap-*.c
rm -rf c-libedit
git add -u ..

rm -rf libbsd[-_]* libedit[-_]* build

apt-get source libbsd0
apt-get source libedit

mkdir build
(cd build \
     && export ac_cv_func_secure_getenv=no \
     && export ac_cv_func___secure_getenv=no \
     && export ac_cv_header_sys_cdefs_h=no \
     && export ac_cv_header_curses_h=no \
     && export ac_cv_header_ncurses_h=no \
     && ../libedit-*/configure \
     && make SUBDIRS=src)

mkdir -p c-libedit/linux-build c-libedit/editline

cp -a libedit-*/src/editline c-libedit/
cp -a libedit-*/src/*.[ch] c-libedit/
cp -a build/config.h build/src/*.h c-libedit/linux-build/

# This Linux readline is out of sync with the main BSD repo. The Newer
# BSD readlines have 3 extra arguments on fn_complete().  Make them
# exist so that the C calls don't crash.
patch -p1 <fncomplete.patch

# This ensures that the GNU libc secure_getenv is used when available.
patch -p1 <secure_getenv.patch

rm -f c-libedit/*.orig

(cd c-libedit &&
     for i in *.c; do
	 echo "#ifndef GO_LIBEDIT_NO_BUILD">../libedit-$i
	 echo "#include \"$i\"">>../libedit-$i
	 echo "#endif">>../libedit-$i
	 echo "// Nothing to see here.">../../shim/libedit-$i
	 echo "#include \"libedit-$i\"">../../wrap-$i
     done)

git add ../shim/*.c
git add ../wrap-*.c
git add libedit-*.c
git add c-libedit
