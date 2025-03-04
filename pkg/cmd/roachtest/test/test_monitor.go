// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package test

// Monitor is an interface for monitoring cockroach processes during a test.
type Monitor interface {
	ExpectDeath()
	ExpectDeaths(count int32)
	ResetDeaths()
}
