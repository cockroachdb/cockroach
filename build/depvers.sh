#!/usr/bin/env bash
#
# Output the revision control version information (git/hg SHA) for the
# current directory's dependencies. If multiple packages are contained
# within the same git/hg repo we only output the top-level repo and
# SHA once.
#
# The output is sorted by package name:
#   <package-repo-root>:<sha>

set -eo pipefail

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

# List the current package and all of the dependencies.
pkgs=$(go list -f '{{.ImportPath}}{{"\n"}}{{join .Deps "\n" }}' . | sort -u)

# For each package which is not part of the standard library, list the package
# directory and package root.
pkginfo=$(echo "$pkgs" | xargs go list -f '{{if not .Standard}}{{.Dir}} {{.Root}}{{end}}')

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
    vers=$(hg --cwd "${dir}" parent --template '{node}')
  fi

  root=${pkginfo[$i+1]}
  echo ${toplevel#$root/src/}:${vers}
done
