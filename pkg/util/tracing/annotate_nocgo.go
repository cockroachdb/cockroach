// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !cgo

package tracing

// AnnotateTrace adds an annotation to the golang executation tracer by calling
// a no-op cgo function.
func AnnotateTrace() {
}
