// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
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
	// ObjectDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ObjectDescriptor = sqlbase.TableDescriptor
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

// SchemaAccessor provides access to database descriptors.
type SchemaAccessor interface {
	// GetDatabaseDesc looks up a database by name and returns its
	// descriptor. If the database is not found and required is true,
	// an error is returned; otherwise a nil reference is returned.
	GetDatabaseDesc(dbName string, flags DatabaseLookupFlags) (*DatabaseDescriptor, error)

	// IsValidSchema returns true if the given schema name is valid for the given database.
	IsValidSchema(db *DatabaseDescriptor, scName string) bool

	// GetObjectNames returns the list of all objects in the given
	// database and schema.
	// TODO(whomever): when separate schemas are supported, this
	// API should be extended to use schema descriptors.
	GetObjectNames(db *DatabaseDescriptor, scName string, flags DatabaseListFlags) (TableNames, error)

	// GetObjectDesc looks up an objcet by name and returns both its
	// descriptor and that of its parent database. If the object is not
	// found and flags.required is true, an error is returned, otherwise
	// a nil reference is returned.
	//
	// flag.allowAdding controls inclusion of non-public descriptors.
	//
	// The 2nd return value (DatabaseDescriptor) is only returned if the
	// lookup function otherwise needed to load the database descriptor.
	// It is not guaranteed to be non-nil even if the first return value
	// is non-nil.  Callers that need a database descriptor can use that
	// to avoid an extra roundtrip through a DatabaseAccessor.
	GetObjectDesc(name *ObjectName, flags ObjectLookupFlags) (*ObjectDescriptor, *DatabaseDescriptor, error)
}

// CommonLookupFlags is the common set of flags for the various accessor interfaces.
type CommonLookupFlags struct {
	ctx context.Context
	txn *client.Txn
	// if required is set, lookup will return an error if the item is not found.
	required bool
	// if avoidCached is set, lookup will avoid the cache (if any).
	avoidCached bool
}

// DatabaseLookupFlags is the flag struct suitable for GetDatabaseDesc().
type DatabaseLookupFlags = CommonLookupFlags

// DatabaseListFlags is the flag struct suitable for GetObjectNames().
type DatabaseListFlags struct {
	CommonLookupFlags
	// explicitPrefix, when set, will cause the returned table names to
	// have an explicit schema and catalog part.
	explicitPrefix bool
}

// ObjectLookupFlags is the flag struct suitable for GetObjectDesc().
type ObjectLookupFlags struct {
	CommonLookupFlags
	// if allowAdding is set, descriptors in the ADD state will be
	// included in the results as well.
	allowAdding bool
}
