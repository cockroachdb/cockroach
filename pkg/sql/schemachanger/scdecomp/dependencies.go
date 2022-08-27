// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdecomp

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// CommentGetter represent an interface to fetch comment for descriptors.
type CommentGetter interface {
	// GetDatabaseComment returns comment for a database. `ok` returned indicates
	// if the comment actually exists or not.
	GetDatabaseComment(ctx context.Context, dbID catid.DescID) (comment string, ok bool, err error)

	// GetSchemaComment returns comment for a schema. `ok` returned indicates if
	// the comment actually exists or not.
	GetSchemaComment(ctx context.Context, schemaID catid.DescID) (comment string, ok bool, err error)

	// GetTableComment returns comment for a table. `ok` returned indicates if the
	// comment actually exists or not.
	GetTableComment(ctx context.Context, tableID catid.DescID) (comment string, ok bool, err error)

	// GetColumnComment returns comment for a column. `ok` returned indicates if
	// the comment actually exists or not.
	GetColumnComment(ctx context.Context, tableID catid.DescID, pgAttrNum catid.PGAttributeNum) (comment string, ok bool, err error)

	// GetIndexComment returns comment for an index. `ok` returned indicates if
	// the comment actually exists or not.
	GetIndexComment(ctx context.Context, tableID catid.DescID, indexID catid.IndexID) (comment string, ok bool, err error)

	// GetConstraintComment returns comment for a constraint. `ok` returned
	// indicates if the comment actually exists or not.
	GetConstraintComment(ctx context.Context, tableID catid.DescID, constraintID catid.ConstraintID) (comment string, ok bool, err error)
}

// ZoneConfigGetter supports reading raw zone config information
// from storage.
type ZoneConfigGetter interface {
	// GetZoneConfig reads the raw zone config from storage.
	GetZoneConfig(ctx context.Context, id descpb.ID) (*zonepb.ZoneConfig, error)
}

// ElementVisitor is the type of the visitor callback function used by
// WalkDescriptor.
type ElementVisitor func(status scpb.Status, element scpb.Element)
