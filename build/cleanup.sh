#!/usr/bin/env bash

for obsolete in $(find pkg/sql/colexec -type f -name '*.eg.go' "$@" 2>/dev/null) \
		$(find pkg/sql/exec -type f -name '*.eg.go' "$@" 2>/dev/null); do \
  echo "Removing obsolete file $obsolete..."; \
  rm -f $obsolete; \
done
