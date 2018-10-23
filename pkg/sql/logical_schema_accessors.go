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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//

// LogicalSchemaAccessor extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalSchemaAccessor struct {
	SchemaAccessor
	vt VirtualTabler
}

var _ SchemaAccessor = &LogicalSchemaAccessor{}

// IsValidSchema implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) IsValidSchema(dbDesc *DatabaseDescriptor, scName string) bool {
	if _, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		return true
	}

	// Fallthrough.
	return l.SchemaAccessor.IsValidSchema(dbDesc, scName)
}

// GetObjectNames implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) GetObjectNames(
	dbDesc *DatabaseDescriptor, scName string, flags DatabaseListFlags,
) (TableNames, error) {
	if entry, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		names := make(TableNames, len(entry.orderedDefNames))
		for i, name := range entry.orderedDefNames {
			names[i] = tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.Name), tree.Name(entry.desc.Name), tree.Name(name))
			names[i].ExplicitCatalog = flags.explicitPrefix
			names[i].ExplicitSchema = flags.explicitPrefix
		}

		return names, nil
	}

	// Fallthrough.
	return l.SchemaAccessor.GetObjectNames(dbDesc, scName, flags)
}

// GetObjectDesc implements the ObjectAccessor interface.
func (l *LogicalSchemaAccessor) GetObjectDesc(
	name *ObjectName, flags ObjectLookupFlags,
) (*ObjectDescriptor, *DatabaseDescriptor, error) {
	if scEntry, ok := l.vt.getVirtualSchemaEntry(name.Schema()); ok {
		tableName := name.Table()
		if t, ok := scEntry.defs[tableName]; ok {
			return t.desc, nil, nil
		}
		if _, ok := scEntry.allTableNames[tableName]; ok {
			return nil, nil, pgerror.Unimplemented(name.Schema()+"."+tableName,
				"virtual schema table not implemented: %s.%s", name.Schema(), tableName)
		}

		if flags.required {
			return nil, nil, sqlbase.NewUndefinedRelationError(name)
		}
		return nil, nil, nil
	}

	// Fallthrough.
	return l.SchemaAccessor.GetObjectDesc(name, flags)
}
