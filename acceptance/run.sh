#!/bin/bash

cd $(dirname $0)/..
build/builder.sh make install
go run acceptance/*.go
