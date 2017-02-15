#!/bin/bash

cd $(dirname $0)

pkgs=$(go list -f '{{ join .Deps "\n"}}' . | egrep '/c-(protobuf|rocksdb|snappy)')

function clean() {
  for pkg in ${pkgs}; do
    local info=($(go list -f '{{ .Dir }} {{ .Name }}' ${pkg}))
    local dir=${info[0]}
    local name=${info[1]}
    rm -fr ${name}_*.{cpp,cc}
  done
}

function setup() {
  for pkg in ${pkgs}; do
    local info=($(go list -f '{{ .Dir }} {{ .Name }}' ${pkg}))
    local dir=${info[0]}
    local name=${info[1]}
    for file in $(go list -f '{{ join .CXXFiles "\n" }}{{ join .CFiles "\n" }}' ${pkg}); do
      echo ${name}_${file}
      ln -sf ${dir}/${file} ${name}_${file}
    done
  done
}

case $1 in
  clean)
    clean
  ;;
  setup)
    setup
  ;;
  *)
    echo "usage: $(basename $0) <setup|clean>"
esac
