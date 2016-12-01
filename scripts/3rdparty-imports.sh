#!/usr/bin/env bash

set -euxo pipefail

go list -f '{{ join .XTestImports "\n"}}{{"\n"}}{{ join .TestImports "\n"}}{{"\n"}}' . ./pkg/... |
	sort | uniq |
	xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' |
	sed s,github.com/cockroachdb/cockroach/vendor/,, |
	grep -v cockroachdb/cockroach