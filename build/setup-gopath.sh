#!/bin/bash

# This script uses the vendor manifest and glock to setup GOPATH such that it
# contains all the dependencies at the same revisions as the vendor directory,
# for use if/when one wants a managed GOPATH.
gvt list | while read -r -a i; do echo "${i[0]}" "${i[3]}"; done | glock sync -n
