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
// interface defined in schema_accessors.go.
//
// They are meant to be used to access stored descriptors only.
// For a higher-level implementation that also knows about
// virtual schemas, check out logical_schema_accessors.go.
//
// The following implementations are provided:
//
// - UncachedPhysicalAccessor, for uncached db accessors
//
// - CachedPhysicalAccessor, which adds an object cache
//   - plugged on top another SchemaAccessor.
//   - uses a `*TableCollection` (table.go) as cache.
//

// UncachedPhysicalAccessor implements direct access to DB descriptors,
// without any kind of caching.
type UncachedPhysicalAccessor struct{}

var _ SchemaAccessor = UncachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetDatabaseDesc(
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

// IsValidSchema implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) IsValidSchema(dbDesc *DatabaseDescriptor, scName string) bool {
	// At this point, only the public schema is recognized.
	return scName == tree.PublicSchema
}

// GetObjectNames implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectNames(
	dbDesc *DatabaseDescriptor, scName string, flags DatabaseListFlags,
) (TableNames, error) {
	if ok := a.IsValidSchema(dbDesc, scName); !ok {
		if flags.required {
			tn := tree.MakeTableNameWithSchema(tree.Name(dbDesc.Name), tree.Name(scName), "")
			return nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(&tn.TableNamePrefix))
		}
		return nil, nil
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

// GetObjectDesc implements the SchemaAccessor interface.
func (a UncachedPhysicalAccessor) GetObjectDesc(
	name *ObjectName, flags ObjectLookupFlags,
) (*ObjectDescriptor, *DatabaseDescriptor, error) {
	// At this point, only the public schema is recognized.
	if name.Schema() != tree.PublicSchema {
		if flags.required {
			return nil, nil, sqlbase.NewUnsupportedSchemaUsageError(tree.ErrString(name))
		}
		return nil, nil, nil
	}

	// Look up the database.
	dbDesc, err := a.GetDatabaseDesc(name.Catalog(), flags.CommonLookupFlags)
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

// CachedPhysicalAccessor adds a cache on top of any SchemaAccessor.
type CachedPhysicalAccessor struct {
	SchemaAccessor
	tc *TableCollection
}

var _ SchemaAccessor = &CachedPhysicalAccessor{}

// GetDatabaseDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetDatabaseDesc(
	name string, flags DatabaseLookupFlags,
) (desc *DatabaseDescriptor, err error) {
	isSystemDB := name == sqlbase.SystemDB.Name
	if !(flags.avoidCached || isSystemDB || testDisableTableLeases) {
		refuseFurtherLookup, dbID, err := a.tc.getUncommittedDatabaseID(name, flags.required)
		if refuseFurtherLookup || err != nil {
			return nil, err
		}

		if dbID != 0 {
			// Some database ID was found in the list of uncommitted DB changes.
			// Use that to get the descriptor.
			desc, err := a.tc.databaseCache.getDatabaseDescByID(flags.ctx, flags.txn, dbID)
			if desc == nil && flags.required {
				return nil, sqlbase.NewUndefinedDatabaseError(name)
			}
			return desc, err
		}

		// The database was not known in the uncommitted list. Have the db
		// cache look it up by name for us.
		return a.tc.databaseCache.getDatabaseDesc(flags.ctx,
			a.tc.leaseMgr.execCfg.DB.Txn, name, flags.required)
	}

	// We avoided the cache. Go lower.
	return a.SchemaAccessor.GetDatabaseDesc(name, flags)
}

// GetObjectDesc implements the SchemaAccessor interface.
func (a *CachedPhysicalAccessor) GetObjectDesc(
	name *ObjectName, flags ObjectLookupFlags,
) (*ObjectDescriptor, *DatabaseDescriptor, error) {
	// Can we use the table cache?
	// - avoidCached -> the caller said no.
	// - allowAdding -> no, the table cache only wants to handle public descriptors.
	if !flags.avoidCached && !flags.allowAdding {
		return a.tc.getTableVersion(flags.ctx, name, flags)
	}

	// Default path.
	return a.SchemaAccessor.GetObjectDesc(name, flags)
}
