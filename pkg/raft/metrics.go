// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

// Metrics contains all the metrics reported inside Raft.
type Metrics struct{}

// NewMetrics creates a new Metrics instance with all related metric fields.
func NewMetrics() *Metrics {
	return &Metrics{}
}
