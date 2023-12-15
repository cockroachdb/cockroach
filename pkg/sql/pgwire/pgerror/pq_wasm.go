// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build wasm

package pgerror

// fullErrorFromPQ is a shim for wasm because github.com/lib/pq does not support
// wasm.
func fullErrorFromPQ(err error) (string, bool) {
	return "", false
}
