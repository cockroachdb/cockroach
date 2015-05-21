#!/bin/bash

cd $(dirname $0)/..
build/builder.sh make install
go test -v -tags acceptance ./acceptance
