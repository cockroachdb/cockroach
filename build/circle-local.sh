#!/usr/bin/env bash

set -euo pipefail

"$(dirname "${0}")"/circle-deps.sh
"$(dirname "${0}")"/circle-test.sh
