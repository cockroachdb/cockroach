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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//
// - LogicalDatabaseLister augments a DatabaseLister with virtual schemas.
// - LogicalObjectAccessor augments an ObjectAccessor with virtual tables.
//

// LogicalDatabaseLister extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalDatabaseLister struct {
	DatabaseLister
	vt VirtualTabler
}

// IsValidSchema implements the DatabaseLister interface.
func (l *LogicalDatabaseLister) IsValidSchema(dbDesc *DatabaseDescriptor, scName string) bool {
	if _, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		return true
	}

	// Fallthrough.
	return l.DatabaseLister.IsValidSchema(dbDesc, scName)
}

// GetObjectNames implements the DatabaseLister interface.
func (l *LogicalDatabaseLister) GetObjectNames(
	dbDesc *DatabaseDescriptor, scName string, flags DatabaseListFlags,
) (TableNames, error) {
	if entry, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		names := make(TableNames, len(entry.orderedTableNames))
		for i, name := range entry.orderedTableNames {
			names[i] = tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.Name), tree.Name(entry.desc.Name), tree.Name(name))
			names[i].ExplicitCatalog = flags.explicitPrefix
			names[i].ExplicitSchema = flags.explicitPrefix
		}
		return names, nil
	}

	// Fallthrough.
	return l.DatabaseLister.GetObjectNames(dbDesc, scName, flags)
}

// LogicalObjectAccessor extends an existing ObjectAccessor with
// the ability to access tables in a virtual schema.
type LogicalObjectAccessor struct {
	ObjectAccessor
	vt VirtualTabler
}

// GetObjectDesc implements the ObjectAccessor interface.
func (l *LogicalObjectAccessor) GetObjectDesc(
	name *ObjectName, flags ObjectLookupFlags,
) (*ObjectDescriptor, *DatabaseDescriptor, error) {
	if entry, err := l.vt.getVirtualTableEntry(name); entry.desc != nil || err != nil {
		// It's OK that we don't have a db descriptor to return.
		// The spec for this API says it's optional.
		return entry.desc, nil, err
	}

	// Fallthrough.
	return l.ObjectAccessor.GetObjectDesc(name, flags)
}
