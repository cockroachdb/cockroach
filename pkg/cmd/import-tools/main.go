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

//go:build tools
// +build tools

package main

import (
	"fmt"

	_ "github.com/cockroachdb/crlfmt"
	_ "github.com/mmatczuk/go_generics/cmd/go_generics"
	_ "github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc"
)

func main() {
	fmt.Printf("You just lost the game\n")
}
