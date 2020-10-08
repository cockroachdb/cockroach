#!/bin/sh

set -e

echo psql | kinit tester@MY.EX

./gss.test
