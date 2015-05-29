#!/bin/bash
#
# Output the revision control version information (git/hg SHA) for the
# current directory's dependencies. If multiple packages are contained
# within the same git/hg repo we only output the top-level repo and
# SHA once.
#
# The output is sorted by package name:
#   <package-repo-root>:<sha>

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

# List the current package and all of the dependencies which are not
# part of the standard library (i.e. packages that contain a least one
# dot in the first component of their name).
pkgs=$(go list -f '{{printf "%s\n" .ImportPath}}{{range .Deps}}{{printf "%s\n" .}}{{end}}' . 2>/dev/null | \
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

  if test "${git}" -eq 1; then
    vers=$(git -C "${dir}" rev-parse HEAD 2>/dev/null )
  else
    # file ownership sometimes doesn't work in docker and hg gets grumpy about
    # this file, which is basically empty anyway, so just delete it
    rm "${dir}/.hg/hgrc"
    vers=$(hg --cwd "${dir}" parent --template '{node}')
  fi

  root=${pkginfo[$i+1]}
  echo ${toplevel#$root/src/}:${vers}
done
