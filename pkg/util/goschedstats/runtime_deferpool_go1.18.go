// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build gc && go1.18
// +build gc,go1.18

package goschedstats

type deferpool struct {
	deferpool    []uintptr // pool of available defer structs (see panic.go)
	deferpoolbuf [32]uintptr
}
