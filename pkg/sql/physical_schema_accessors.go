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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//
// They are meant to be used to access stored descriptors only.
// For a higher-level implementation that also knows about
// virtual schemas, check out logical_schema_accessors.go.
//
// The following implementations are provided:
//
// - PhysicalDBAccessor, for uncached db accessors
//   - implements DatabaseAccessor
//   - implements DatabaseLister
//
// - DBAccessorWithCache, which adds a database cache
//   - implements DatabaseAccessor
//   - plugged on top another DatabaseAccessor (can be used with the
//     provided PhysicalDBAccessor, but one could imagine plugging a
//     shim instead for testing).
//   - uses a `*databaseCache` (database.go) as cache.
//
// - PhysicalObjectAccessor, for uncached object accessors
//   - implements ObjectAccessor
//   - plugged on top a DatabaseAccessor (can be used with either
//     DatabaseAccessor above, or perhaps a testing shim).
//
// - ObjectAccessorWithCache, which adds an object cache
//   - implements ObjectAccessor
//   - plugged on top another ObjectAccessor (can be used
//     with the provided PhysicalObjectAccessor, but one could
//     imagine plugging a shim instead for testing).
//   - uses a `*TableCollection` (table.go) as cache.
//

// PhysicalDBAccessor implements direct access to DB descriptors,
// without any kind of caching.
type PhysicalDBAccessor struct{}

var _ DatabaseAccessor = PhysicalDBAccessor{}
var _ DatabaseLister = PhysicalDBAccessor{}

// GetDatabaseDesc implements the DatabaseAccessor interface.
func (a PhysicalDBAccessor) GetDatabaseDesc(
	name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	desc = &sqlbase.DatabaseDescriptor{}
	found, err := getDescriptor(flags.ctx, flags.txn, databaseKey{name}, desc)
	if err != nil {
		return nil, err
	}
	if !found {
		desc = nil
	}

	// If the descriptor was required, check it's available here.
	if desc == nil && flags.required {
		return nil, sqlbase.NewUndefinedDatabaseError(name)
	}

	return desc, nil
}

// IsValidSchema implements the DatabaseLister interface.
func (a PhysicalDBAccessor) IsValidSchema(dbDesc *DatabaseDescriptor, scName string) bool {
	// At this point, only the public schema is recognized.
	return scName == tree.PublicSchema
}

// GetObjectNames implements the DatabaseLister interface.
func (a PhysicalDBAccessor) GetObjectNames(
	dbDesc *DatabaseDescriptor, scName string, flags DatabaseListFlags,
) (TableNames, error) {
	if ok := a.IsValidSchema(dbDesc, scName); !ok {
		return nil, newInvalidSchemaError(dbDesc.Name, scName)
	}

	prefix := sqlbase.MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := flags.txn.Scan(flags.ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var tableNames tree.TableNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeUnsafeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, err
		}
		tn := tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(tableName))
		tn.ExplicitCatalog = flags.explicitPrefix
		tn.ExplicitSchema = flags.explicitPrefix
		tableNames = append(tableNames, tn)
	}
	return tableNames, nil
}

// DBAccessorWithCache adds a cache on top of any DatabaseAccessor.
type DBAccessorWithCache struct {
	DatabaseAccessor
	dc *databaseCache
}

var _ DatabaseAccessor = &DBAccessorWithCache{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a *DBAccessorWithCache) GetDatabaseDesc(
	name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	if !flags.avoidCached && a.dc != nil {
		// Try to use the cache. It's ok if we miss, we'll go through the
		// physical resolver below.
		desc, err = a.dc.getCachedDatabaseDesc(name, false /*required*/)
	}
	if desc == nil {
		// We avoided the cache, or the descriptor was not in the cache.
		// Look it up.
		desc, err = a.DatabaseAccessor.GetDatabaseDesc(name, flags)
		if desc != nil && !flags.avoidCached && a.dc != nil {
			// If there's a cache and we were willing to use it, and we're
			// here, this means there was a cache miss. Remember the mapping
			// in the cache so that future lookups will hit.
			a.dc.setID(name, desc.GetID())
		}
	}
	return desc, err
}

// PhysicalObjectAccessor uses some DatabaseAccessor and KV lookups to
// access object descriptors.
type PhysicalObjectAccessor struct {
	DatabaseAccessor
}

var _ ObjectAccessor = &PhysicalObjectAccessor{}
var _ DatabaseAccessor = &PhysicalObjectAccessor{}

// GetObjectDesc implements the ObjectAccessor interface.
func (a *PhysicalObjectAccessor) GetObjectDesc(
	name *ObjectName, flags ObjectLookupFlags,
) (*ObjectDescriptor, *DatabaseDescriptor, error) {
	// At this point, only the public schema is recognized.
	if name.Schema() != tree.PublicSchema {
		return nil, nil, newInvalidSchemaError(name.Catalog(), name.Schema())
	}
	dbName, tblName := name.Catalog(), name.Table()

	// Look up the database. This uses the underlying DatabaseAccessor.
	dbDesc, err := a.DatabaseAccessor.GetDatabaseDesc(dbName, flags.CommonLookupFlags)
	if dbDesc == nil || err != nil {
		// dbDesc can be nil if the object is not required and the
		// database was not found.
		return nil, dbDesc, err
	}

	// Look up the table using the discovered database descriptor.
	desc := &sqlbase.TableDescriptor{}
	found, err := getDescriptor(flags.ctx, flags.txn,
		tableKey{parentID: dbDesc.ID, name: name.Table()}, desc)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		desc = nil
	} else {
		// We have a descriptor. Is it in the right state?
		if err := filterTableState(desc); err != nil {
			// No: let's see the flag.
			if flags.allowAdding && err == errTableAdding {
				// We'll keep that despite the ADD state.
				return desc, dbDesc, nil
			}
			// Bad state: the descriptor is essentially invisible.
			desc = nil
		}
	}

	if desc == nil && flags.required {
		return nil, nil, sqlbase.NewUndefinedRelationError(name)
	}
	return desc, dbDesc, nil
}

// ObjectAccessorWithCache adds a cache on top of any ObjectAccessor.
//
// TODO(whomever): when going through the cache, a cache miss forces
// an access via the PhysicalObjectAccessor and PhysicalDBAccessor. To
// make the abstraction cleanly layered,
// (*TableCollection).getTableVersion() should be extended to take an
// underlying DatabaseAccessor / ObjectAccessor as parameter.
type ObjectAccessorWithCache struct {
	ObjectAccessor
	tc *TableCollection
}

var _ ObjectAccessor = &ObjectAccessorWithCache{}

// GetObjectDesc implements the ObjectAccessor interface.
func (a *ObjectAccessorWithCache) GetObjectDesc(
	name *ObjectName, flags ObjectLookupFlags,
) (*ObjectDescriptor, *DatabaseDescriptor, error) {
	// Can we use the table cache?
	// - avoidCached -> the caller said no.
	// - allowAdding -> no, the table cache only wants to handle public descriptors.
	// - !required -> no, getTableVersion only knows how to cache descriptors
	//   guaranteed to exist beforehand.
	if !flags.avoidCached && !flags.allowAdding && flags.required && a.tc != nil {
		desc, err := a.tc.getTableVersion(name, flags)
		// The table cache fully owns the lookup. There's nothing to do.
		// It didn't leave us a db descriptor to return, but that's OK
		// because the interface for GetObjectDesc() says it's optional.
		return desc, nil, err
	}

	// Default path.
	return a.ObjectAccessor.GetObjectDesc(name, flags)
}
