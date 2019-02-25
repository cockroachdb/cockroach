#!/bin/sh

set -e

echo psql | kinit tester@MY.EX

go test -tags gss /test/gss_test.go
