#!/usr/bin/env bash

# Copyright (c) 2016 Travis CI GmbH <contact@travis-ci.org>
#
# Use of this source code is governed by the MIT license that can be found in
# licenses/MIT-travis.txt.
#
# Copied from github.com/travis-ci/travis-build and modified to pass shellcheck.

travis_retry() {
  local result=0
  local count=1
  while [ $count -le 3 ]; do
    [ $result -ne 0 ] && {
      echo -e "\n${ANSI_RED}The command \"$*\" failed. Retrying, $count of 3.${ANSI_RESET}\n" >&2
    }
    "$@"
    result=$?
    [ $result -eq 0 ] && break
    count=$((count + 1))
    sleep 10
  done

  [ $count -gt 3 ] && {
    echo -e "\n${ANSI_RED}The command \"$*\" failed 3 times.${ANSI_RESET}\n" >&2
  }

  return $result
}

travis_retry "$@"
