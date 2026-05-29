// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// createTableChecks is the cheap prefilter that decides whether CREATE TABLE
// should be handled by the declarative schema changer. The dispatcher in
// process.go calls it before resolving any descriptors, and a return of false
// routes the statement back to the legacy schema changer.
func createTableChecks(
	n *tree.CreateTable,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	return false
}

// CreateTable implements CREATE TABLE in the declarative schema changer.
// Unsupported statements panic NotImplementedError; the panic is recovered in
// schema_change_plan_node.go and routed to the legacy planner.
func CreateTable(b BuildCtx, n *tree.CreateTable) {
	panic(scerrors.NotImplementedErrorf(n,
		"create table is not yet supported in the declarative schema changer"))
}
