// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package pgrepltree contains the AST structs for pg replication types.
package pgrepltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type ReplicationStatement interface {
	tree.Statement
	replicationStatement()
}
