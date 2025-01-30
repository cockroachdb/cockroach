// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !cgo

package build

func cgoVersion() string {
	return "cgo-disabled"
}
