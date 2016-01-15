#!/bin/bash

pkgs=$(go list -f '{{printf "%s\n" .ImportPath}}{{range .Deps}}{{printf "%s\n" .}}{{end}}' "$@")
for pkg in ${pkgs}; do
  stale=$(go list -f '{{ .Stale }}' "${pkg}")
  if [ "${stale}" = "true" ]; then
    target=$(go list -f '{{ .Target }}' "${pkg}")
    dir=$(go list -f '{{ .Dir }}' "${pkg}")
    if [ ! -e "${target}" ]; then
      echo "${target}: does not exist"
      continue
    fi

    echo "${target}"
    files=$(go list -f '{{range .GoFiles}}{{printf "%s\n" .}}{{end}}{{range .CgoFiles}}{{printf "%s\n" .}}{{end}}{{range .CXXFiles}}{{printf "%s\n" .}}{{end}}{{range .CFiles}}{{printf "%s\n" .}}{{end}}{{range .HFiles}}{{printf "%s\n" .}}{{end}}{{range .SFiles}}{{printf "%s\n" .}}{{end}}{{range .MFiles}}{{printf "%s\n" .}}{{end}}{{range .SwigFiles}}{{printf "%s\n" .}}{{end}}{{range .SwigCXXFiles}}{{printf "%s\n" .}}{{end}}{{range .SysoFiles}}{{printf "%s\n" .}}{{end}}' "${pkg}")
    for file in ${files}; do
      path="${dir}/${file}"
      if [ "${path}" -nt "${target}" ]; then
        echo "    ${file}"
      fi
    done
  fi
done
