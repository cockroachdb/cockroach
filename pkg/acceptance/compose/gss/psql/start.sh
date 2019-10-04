#!/bin/sh

set -e

echo psql | kinit tester@MY.EX

go test -v -tags gss_compose /test/gss_test.go
