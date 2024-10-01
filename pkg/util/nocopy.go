// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

// NoCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type NoCopy struct{}

// Silence unused warnings.
var _ = NoCopy{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*NoCopy) Lock() {}

// Unlock is a no-op used by -copylocks checker from `go vet`.
func (*NoCopy) Unlock() {}
