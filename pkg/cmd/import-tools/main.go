// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// import-tools adds a blank import to tools we use such that `go mod tidy`
// doesn't clean up needed dependencies when running `go install`.

// +build tools

package main

import (
	"fmt"

	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/client9/misspell/cmd/misspell"
	_ "github.com/cockroachdb/crlfmt"
	_ "github.com/cockroachdb/go-test-teamcity"
	_ "github.com/cockroachdb/gostdlib/cmd/gofmt"
	_ "github.com/cockroachdb/gostdlib/x/tools/cmd/goimports"
	_ "github.com/cockroachdb/stress"
	_ "github.com/go-swagger/go-swagger/cmd/swagger"
	_ "github.com/golang/mock/mockgen"
	_ "github.com/goware/modvendor"
	_ "github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway"
	_ "github.com/kevinburke/go-bindata/go-bindata"
	_ "github.com/kisielk/errcheck"
	_ "github.com/mattn/goveralls"
	_ "github.com/mibk/dupl"
	_ "github.com/mmatczuk/go_generics/cmd/go_generics"
	_ "github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc"
	_ "github.com/wadey/gocovmerge"
	_ "golang.org/x/lint/golint"
	_ "golang.org/x/perf/cmd/benchstat"
	_ "golang.org/x/tools/cmd/goyacc"
	_ "golang.org/x/tools/cmd/stringer"
	_ "golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow"
	_ "honnef.co/go/tools/cmd/staticcheck"
)

func main() {
	fmt.Printf("You just lost the game\n")
}
