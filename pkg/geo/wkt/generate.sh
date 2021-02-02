#!/bin/sh
set -eux

goyacc -o wkt_generated.go -p "wkt" wkt.y
# TODO(ayang) modify goyacc to accept a cmd line flag to set this
cat wkt_generated.go | sed -e 's/wktErrorVerbose = false/wktErrorVerbose = true/' > wkt_generated.go.tmp
mv wkt_generated.go.tmp wkt_generated.go
