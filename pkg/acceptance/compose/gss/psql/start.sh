#!/bin/sh

set -e

echo psql | kinit tester@MY.EX

go test -tags gss_compose /test/gss_test.go
