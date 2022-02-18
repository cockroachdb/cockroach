// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package deprules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

func init() {
	depRule(
		"descriptor drop before dependent element removal",
		scgraph.Precedence,
		scpb.Status_ABSENT,
		element(
			scpb.Status_DROPPED,
			(*scpb.Database)(nil),
			(*scpb.Schema)(nil),
			(*scpb.Table)(nil),
			(*scpb.View)(nil),
			(*scpb.Sequence)(nil),
			(*scpb.AliasType)(nil),
			(*scpb.EnumType)(nil),
		),
		element(
			scpb.Status_ABSENT,
			// Table elements.
			(*scpb.TableLocality)(nil),
			(*scpb.ColumnFamily)(nil),
			(*scpb.Column)(nil),
			(*scpb.PrimaryIndex)(nil),
			(*scpb.SecondaryIndex)(nil),
			(*scpb.UniqueWithoutIndexConstraint)(nil),
			(*scpb.CheckConstraint)(nil),
			(*scpb.ForeignKeyConstraint)(nil),
			(*scpb.TableComment)(nil),
			(*scpb.RowLevelTTL)(nil),
			// Column elements.
			(*scpb.ColumnName)(nil),
			(*scpb.ColumnDefaultExpression)(nil),
			(*scpb.ColumnOnUpdateExpression)(nil),
			(*scpb.SequenceOwner)(nil),
			(*scpb.ColumnComment)(nil),
			// Index elements.
			(*scpb.IndexName)(nil),
			(*scpb.IndexPartitioning)(nil),
			(*scpb.IndexComment)(nil),
			// Constraint elements.
			(*scpb.ConstraintName)(nil),
			(*scpb.ConstraintComment)(nil),
			// Common elements.
			(*scpb.Namespace)(nil),
			(*scpb.Owner)(nil),
			(*scpb.UserPrivileges)(nil),
			// Database elements.
			(*scpb.DatabaseRoleSetting)(nil),
			(*scpb.DatabaseRegionConfig)(nil),
			(*scpb.DatabaseComment)(nil),
			// Schema elements.
			(*scpb.SchemaParent)(nil),
			(*scpb.SchemaComment)(nil),
			// Object elements.
			(*scpb.ObjectParent)(nil),
		),
		screl.DescID,
	).register()
}
