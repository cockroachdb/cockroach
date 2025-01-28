// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build amd64 || arm64 || arm64be || ppc64 || ppc64le || mips64 || mips64le || s390x || sparc64

package storage

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
)
