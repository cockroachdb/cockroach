#!/bin/bash
set -e
. ./bisect-util.sh

trap 'echo error on line $LINENO. aborting; exit 1' ERR
test -d ./scripts/bisection

echo "hello there"
