#!/bin/sh

goyacc -o wkt_generated.go -p "wkt" wkt.y
#todo (ayang) modify goyacc to accept a cmd line flag to set this
sed -i '' 's/wktErrorVerbose = false/wktErrorVerbose = true/' wkt_generated.go
