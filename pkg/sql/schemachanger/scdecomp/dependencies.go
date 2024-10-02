// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scdecomp

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// CommentGetter represent an interface to fetch comment for descriptors.
type CommentGetter interface {
	// GetDatabaseComment returns comment for a database. `ok` returned indicates
	// if the comment actually exists or not.
	GetDatabaseComment(dbID catid.DescID) (comment string, ok bool)

	// GetSchemaComment returns comment for a schema. `ok` returned indicates if
	// the comment actually exists or not.
	GetSchemaComment(schemaID catid.DescID) (comment string, ok bool)

	// GetTableComment returns comment for a table. `ok` returned indicates if the
	// comment actually exists or not.
	GetTableComment(tableID catid.DescID) (comment string, ok bool)

	// GetTypeComment returns comment for a Type. `ok` returned indicates if the
	// comment actually exists or not.
	GetTypeComment(TypeID catid.DescID) (comment string, ok bool)

	// GetColumnComment returns comment for a column. `ok` returned indicates if
	// the comment actually exists or not.
	GetColumnComment(tableID catid.DescID, pgAttrNum catid.PGAttributeNum) (comment string, ok bool)

	// GetIndexComment returns comment for an index. `ok` returned indicates if
	// the comment actually exists or not.
	GetIndexComment(tableID catid.DescID, indexID catid.IndexID) (comment string, ok bool)

	// GetConstraintComment returns comment for a constraint. `ok` returned
	// indicates if the comment actually exists or not.
	GetConstraintComment(tableID catid.DescID, constraintID catid.ConstraintID) (comment string, ok bool)
}

// ZoneConfigGetter supports reading raw zone config information
// from storage.
type ZoneConfigGetter interface {
	// GetZoneConfig reads the raw zone config from storage.
	GetZoneConfig(ctx context.Context, id descpb.ID) (catalog.ZoneConfig, error)
}

// ElementVisitor is the type of the visitor callback function used by
// WalkDescriptor.
type ElementVisitor func(status scpb.Status, element scpb.Element)
