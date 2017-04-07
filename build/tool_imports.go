// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build glide

package main

// Our vendoring tool, glide, only considers imports as the set of roots used
// to compute the transitive closure of dependencies it should resolve.
// We depend on many tools which we don't actually import directly elsewhere –
// e.g in a `go generate` or our Makefiles – but convincing `glide` to vendor
// them, and their dependencies, is difficult: https://github.com/Masterminds/glide/issues/690
//
// Thus, instead of trying to use `glide get` or `glide.yaml` to document those
// additional dependency roots, we add fake imports here for glide to find.
// NB: Some of these are `package main` so they cannot actually be imported, so
// this file is build-tagged +glide to prevent attempting to build it.

import (
	_ "github.com/Masterminds/glide"
	_ "github.com/client9/misspell/cmd/misspell"
	_ "github.com/cockroachdb/crlfmt"
	_ "github.com/cockroachdb/stress"
	_ "github.com/golang/lint/golint"
	_ "github.com/google/pprof"
	_ "github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway"
	_ "github.com/jteeuwen/go-bindata/go-bindata"
	_ "github.com/kisielk/errcheck"
	_ "github.com/mattn/goveralls"
	_ "github.com/mdempsky/unconvert"
	_ "github.com/mibk/dupl"
	_ "github.com/wadey/gocovmerge"
	_ "golang.org/x/tools/cmd/goimports"
	_ "golang.org/x/tools/cmd/goyacc"
	_ "golang.org/x/tools/cmd/guru"
	_ "golang.org/x/tools/cmd/stringer"
)
