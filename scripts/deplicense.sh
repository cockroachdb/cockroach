#!/usr/bin/env bash

set -euo pipefail

# Output the license info for the current directory's dependencies.
#
# The output is sorted by package name:
#   <package-repo-root> <license info>

function isApache2() {
  # The first pair of patterns matches the apache license itself; the
  # last is the header that some projects use in place of the full
  # license.
  (grep -q '[[:space:]]*Apache License' "$1" && \
   grep -q '[[:space:]]*Version 2\.0,' "$1") || \
  grep -q '^Licensed under the Apache License, Version 2.0 (the "License")' "$1"
}

function isBSD2Clause() {
  grep -q 'Redistributions of source code must retain the above copyright' "$1" && \
  grep -q 'list of conditions and the following disclaimer' "$1" &&
  grep -q 'Redistributions in binary form must reproduce the above' "$1"
}

function isBSD3Clause() {
  egrep -q 'Neither the name of .* nor the names of its' "$1"
}

function isBSD4Clause() {
  grep -q 'All advertising materials mentioning features' "$1"
}

function isLGPLV3() {
  grep -q '[[:space:]]*GNU LESSER GENERAL PUBLIC LICENSE' "$1" && \
  grep -q '[[:space:]]Version 3' "$1"
}

function isMIT() {
  if grep -q 'MIT License' "$1"; then
    return 0
  fi
  grep -q 'Permission is hereby granted, free of charge, to any person' "$1" && \
  grep -q 'The above copyright notice and this permission notice' "$1"
}

function isCC0() {
  grep -q 'This work is subject to the CC0 1.0 Universal (CC0 1.0) Public Domain Dedication' "$1"
}

function isMPL2() {
  grep -q '^Mozilla Public License Version 2.0' "$1" || \
      grep -q 'http://mozilla.org/MPL/2.0/' "$1"

}

function isISC() {
  grep -q '^ISC License$' "$1"
}

function inspect() {
  local dir="$1"

  local files="$(ls ${dir}/{LICENSE,COPYING} 2>/dev/null)"
  files="${files} $(ls ${dir}/LICEN[CS]E.{md,txt,code} 2>/dev/null)"
  files="${files} $(ls ${dir}/*/{LICENSE,COPYING} 2>/dev/null)"

  for file in $files; do
    if [ -e "${file}" ]; then
      if isApache2 "${file}"; then
        echo "Apache License 2.0"
        return
      elif isBSD2Clause "${file}"; then
        if isBSD4Clause "${file}"; then
          echo "BSD 4-Clause License"
        elif isBSD3Clause "${file}"; then
          echo "BSD 3-Clause License"
        else
          echo "BSD 2-Clause License"
        fi
        return
      elif isLGPLV3 "${file}"; then
        echo "GNU Lesser General Public License 3.0"
        return
      elif isMIT "${file}"; then
        echo "MIT License"
        return
      elif isCC0 "${file}"; then
        echo "Creative Commons CC0"
        return
      elif isMPL2 "${file}"; then
        echo "Mozilla Public License 2.0"
        return
      elif isISC "${file}"; then
        echo "ISC license"
        return
      fi
      # TODO(pmattis): This is incomplete. Add other license
      # detectors as necessary.
      break
    fi
  done

  echo "unable to determine license"
}

pkgs=$(grep '^  name = ' Gopkg.lock | cut -d'"' -f2)

for pkg in ${pkgs}; do
  info=$(inspect "vendor/${pkg}")
  printf "%-50s %s\n" "${pkg}" "${info}"
done
