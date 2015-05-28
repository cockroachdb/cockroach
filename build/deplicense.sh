#!/bin/bash
#
# Output the license info for the current directory's dependencies.
#
# The output is sorted by package name:
#   <package-repo-root> <license info>

set -e

dirs=()
function visit() {
  local dir="$1"
  for dir in "${dirs[@]}"; do
    if test "${dir}" = "${toplevel}"; then
      return 0
    fi
    done
    dirs+=("${toplevel}")
    return 1
}

function isApache2() {
  grep -q '[[:space:]]*Apache License' "$1" && \
  grep -q '[[:space:]]*Version 2\.0,' "$1"
}

function isBSD2Clause() {
  grep -q 'Redistributions of source code must retain the above copyright' "$1" && \
  grep -q 'this list of conditions and the following disclaimer' "$1" &&
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

function inspect() {
  local dir="$1"

  local files="${toplevel}/LICENSE"
  files="${files} ${toplevel}/COPYING"
  files="${files} ${toplevel}/internal/LICENSE"
  files="${files} ${toplevel}/internal/COPYING"
  files="${files} $(ls ${toplevel}/*/{LICENSE,COPYING} 2>/dev/null)"

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
      fi
      # TODO(pmattis): This is incomplete. Add other license
      # detectors as necessary.
      break
    fi
  done

  echo "unable to determine license"
}

# List the dependencies which are not part of the standard library
# (i.e. packages that contain a least one dot in the first component
# of their name).
pkgs=$(go list -f '{{range .Deps}}{{printf "%s\n" .}}{{end}}' ./... 2>/dev/null | \
  sort -u | egrep '[^/]+\.[^/]+/')

# For each package, list the package directory and package root.
pkginfo=($(go list -f '{{.Dir}} {{.Root}}' ${pkgs} 2>/dev/null))

# Loop over the package info which comes in pairs in the pkginfo
# array.
for (( i=0; i < ${#pkginfo[@]}; i+=2 )); do
  dir=${pkginfo[$i]}
  if ! test -d "${dir}"; then
    continue
  fi

  toplevel=$(git -C "${dir}" rev-parse --show-toplevel 2>/dev/null)

  git=1
  if test "${toplevel}" = ""; then
    toplevel=$(hg --cwd "${dir}" root 2>/dev/null)
    if test "${toplevel}" = ""; then
      # TODO(pmattis): Handle subversion/bazaar.
      continue
    fi
    git=0
  fi

  if visit "${toplevel}"; then
    continue
  fi

  info=$(inspect "${toplevel}")
  root=${pkginfo[$i+1]}
  printf "%-50s %s\n" "${toplevel#$root/src/}" "${info}"
done
