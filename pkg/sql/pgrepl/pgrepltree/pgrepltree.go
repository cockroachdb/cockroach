// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pgrepltree contains the AST structs for pg replication types.
package pgrepltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type ReplicationStatement interface {
	tree.Statement
	replicationStatement()
}
