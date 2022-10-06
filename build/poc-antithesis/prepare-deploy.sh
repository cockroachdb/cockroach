#!/usr/bin/env bash

# Absolute path to the toplevel cockroach directory.
cockroach_toplevel=$(dirname "$(cd "../"; pwd)")

ANTITHESIS_ARTIFACTS=$cockroach_toplevel/artifacts/instrument

# copy instrumented binary, symbol map and uninstrumented binary (used for workload generation)
cp $ANTITHESIS_ARTIFACTS/go-*.sym.tsv deploy/
# N.B. the instrumented binary will be renamed as cockroach (see deploy/Dockerfile_antithesis)
cp $ANTITHESIS_ARTIFACTS/cockroach-instrumented deploy/instrument

# .so is write-protected, so force overwrite
cp -f $ANTITHESIS_ARTIFACTS/libvoidstar.so deploy/

# N.B. the un-instrumented binary will be renamed as cockroach and used for workload generation (see deploy/Dockerfile)
cp $cockroach_toplevel/cockroachshort-linux-2.6.32-gnu-amd64 deploy/cockroach
cp $cockroach_toplevel/lib/libgeos.so $cockroach_toplevel/lib/libgeos_c.so deploy/
cp -r $cockroach_toplevel/licenses deploy/

chmod 755 deploy/instrument
chmod 755 deploy/cockroach
