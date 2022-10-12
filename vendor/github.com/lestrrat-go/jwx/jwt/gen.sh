#!/bin/bash

pushd internal/cmd/gentoken
go build -o gentoken main.go
popd

./internal/cmd/gentoken/gentoken -objects=internal/cmd/gentoken/objects.yml

rm internal/cmd/gentoken/gentoken
