#!/bin/sh

set -e

pkgs=$(git grep 'go:generate' | grep add-leaktest.sh | awk -F: '{print $1}' | xargs -n1 dirname)
for pkg in ${pkgs}; do
  if [ -z "$(ls ${pkg}/*_test.go 2>/dev/null)" ]; then
    # skip packages without _test.go files.
    continue
  fi

  awk -F'[ (]' '
/func Test.*testing.T\) {/ {
  test = $2
  next
}

/defer leaktest.AfterTest\(.+\)\(\)/ {
  test = 0
  next
}

{
  if (test) {
    printf "%s: %s: missing defer leaktest.AfterTest\n", FILENAME, test
    test = 0
    code = 1
  }
}

END {
  exit code
}
' ${pkg}/*_test.go
done
