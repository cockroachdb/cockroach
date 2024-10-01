// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"

// Node represents a target element with a given current status.
type Node struct {
	*scpb.Target
	CurrentStatus scpb.Status
}
