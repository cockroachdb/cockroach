// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// This file provides high-level interfaces to abstract access to the
// SQL schema (the descriptors).
//
// The following interfaces are defined:
// - DatabaseAccessor, which provides access to database descriptors.
// - DatabaseLister, which provides a service to list the contents of a database.
// - ObjectAccessor, which provides access to individual object descriptors.
//
// A common interface SchemaAccessor is provided for convenience.
//
// See physical_schema_accessors.go and logical_schema_accessors.go for
// reference implementations of these interfaces.

type (
	// ObjectName is provided for convenience and to make the interface
	// definitions below more intuitive.
	ObjectName = tree.TableName
	// DatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	DatabaseDescriptor = sqlbase.DatabaseDescriptor
	// UncachedDatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	UncachedDatabaseDescriptor = sqlbase.DatabaseDescriptor
	// MutableTableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	MutableTableDescriptor = sqlbase.MutableTableDescriptor
	// ImmutableTableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ImmutableTableDescriptor = sqlbase.ImmutableTableDescriptor
	// TableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	TableDescriptor = sqlbase.TableDescriptor
	// ViewDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ViewDescriptor = sqlbase.TableDescriptor
	// SequenceDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	SequenceDescriptor = sqlbase.TableDescriptor
	// TableNames is provided for convenience and to make the interface
	// definitions below more intuitive.
	TableNames = tree.TableNames
)

// ObjectDescriptor provides table information for results from a name lookup.
type ObjectDescriptor interface {
	tree.NameResolutionResult

	// TableDesc returns the underlying table descriptor.
	TableDesc() *TableDescriptor
}

// SchemaAccessor provides access to database descriptors.
type SchemaAccessor interface {
	// GetDatabaseDesc looks up a database by name and returns its
	// descriptor. If the database is not found and required is true,
	// an error is returned; otherwise a nil reference is returned.
	GetDatabaseDesc(ctx context.Context, txn *kv.Txn, dbName string, flags tree.DatabaseLookupFlags) (*DatabaseDescriptor, error)

	// IsValidSchema returns true and the SchemaID if the given schema name is valid for the given database.
	IsValidSchema(ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, scName string) (bool, sqlbase.ID, error)

	// GetObjectNames returns the list of all objects in the given
	// database and schema.
	// TODO(solon): when separate schemas are supported, this
	// API should be extended to use schema descriptors.
	GetObjectNames(ctx context.Context, txn *kv.Txn, db *DatabaseDescriptor, scName string, flags tree.DatabaseListFlags) (TableNames, error)

	// GetObjectDesc looks up an object by name and returns both its
	// descriptor and that of its parent database. If the object is not
	// found and flags.required is true, an error is returned, otherwise
	// a nil reference is returned.
	GetObjectDesc(ctx context.Context, txn *kv.Txn, settings *cluster.Settings, name *ObjectName, flags tree.ObjectLookupFlags) (ObjectDescriptor, error)
}
