#!/bin/bash

d=$(mktemp -d)
echo $d

./dev gen go && ./dev build short

cat >> pkg/kv/kvserver/main_test.go <<EOF

func flabberogaster() int {
  return 123
}

var _ = flabberogaster()
EOF

time -o "$d/gen" ./dev gen go
time -o "$d/buildshort" ./dev build short

time -o "$d/gen-again" ./dev gen go
time -o "$d/buildshort-again" ./dev build short
