// Copyright 2017 The Cockroach Authors.
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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// lookupEnvironment exists to support views and (in the future)
// common table expressions. It overrides the mapping of a name to
// whatever the name should refer to in the context of a sub-query or
// a portion of a FROM clause.
//
// To understand this, one has to consider that the same relation name
// can refer to different things depending on the context. For example,
// start with the schema defined with:
//
// CREATE TABLE kv(k INT, v INT);
// CREATE VIEW v AS SELECT v FROM kv;
// ALTER TABLE kv RENAME TO kvx;
// CREATE TABLE kv(x INT, y INT);
//
// Then consider the following query:
//    SELECT * FROM v, kv
//
// This expands during FROM clause expansion to
//    SELECT * FROM (SELECT v FROM kv), kv
//                                ^^^   ^^ -- this kv has column x,y
//                                 +--------- this kv is now called kvx
//                                            and has columns k,v
//
// Also consider the following query (not yet supported in CockroachDB
// but soon to be):
//
//    WITH kv AS (TABLE kvx) SELECT * FROM v, kv
//
// This expands to:
//
//    WITH kv AS (TABLE kvx) SELECT * FROM (SELECT v FROM kv), kvx
//                                                             ^^^ renamed by WITH
//                                                       ^^^ this must not be overridden!
//
// The core insight here is to recognize both view descriptors and
// WITH clauses can nest and schadow previous mappings. This mandates
// a recursive traversal for source name resolution, using a naming
// environment.
//
// Naming environments are a common data structure in compilers: they
// consist of a mapping at each scope from name to relation. Moreover,
// because scopes can nest but we need to restore the previous mapping
// when a scope ends, we need a linked list structure: a new
// environment is added on top of the stack when entering a scope (a
// WITH clause or a view descriptor), and popped out when the scope
// ends. This meshes well with the recursive traversal already performed
// on the FROM clauses.

type lookupEnvironment struct {
	// parent is the environment to look up a name in if the current
	// environment doesn't have it. If parent is nil, then look up from
	// the lease manager / KV.
	parent *lookupEnvironment

	// Overrides of the name-to-relation mapping.
	schemaOverrides map[schemaPath]*parser.TableRef

	// Overrides of the table descriptor contents once the table ID is known.
	// This is necessary to override the index name-to-indexID mapping.
	descOverrides map[sqlbase.ID]*sqlbase.TableDescriptor_SchemaDependency
}

// schemaPath is used as a key to lookup descriptor IDs in lookupEnvironment, see above.
type schemaPath struct {
	databaseName string
	tableName    string
}

// openScope creates a new environment with some name-to-thing mappings. The
// new environment forms a stack (linked list) with the scope that contains it.
func (env *lookupEnvironment) openScope(
	visibleSchema []sqlbase.TableDescriptor_SchemaDependency,
) lookupEnvironment {
	ret := lookupEnvironment{
		parent:          env,
		schemaOverrides: make(map[schemaPath]*parser.TableRef),
		descOverrides:   make(map[sqlbase.ID]*sqlbase.TableDescriptor_SchemaDependency),
	}

	for i := range visibleSchema {
		s := &visibleSchema[i]
		ret.descOverrides[s.ID] = s

		columnIDs := make([]parser.ColumnID, len(s.ColumnIDs))
		for i, c := range s.ColumnIDs {
			columnIDs[i] = parser.ColumnID(c)
		}
		columnNames := make([]parser.Name, len(s.ColumnNames))
		for i, n := range s.ColumnNames {
			columnNames[i] = parser.Name(n)
		}
		schemaOverride := &parser.TableRef{
			TableID: int64(s.ID),
			Columns: columnIDs,
			As: parser.AliasClause{
				Alias: parser.Name(s.TableName),
				Cols:  columnNames,
			},
		}
		nameKey := schemaPath{s.DatabaseName, s.TableName}
		ret.schemaOverrides[nameKey] = schemaOverride
	}

	return ret
}

// lookupDescOrRefByName looks up a name in the current environment,
// and recurses if it is not found there. If there is no environment,
// the name is looked up from the database.
// The function returns the descriptor if one is found; or
// a table reference if one is found; or an error.
func (p *planner) lookupDescOrRefByName(
	ctx context.Context, env *lookupEnvironment, tn *parser.TableName,
) (*parser.TableRef, *sqlbase.TableDescriptor, error) {
	if env == nil {
		// No lookup environment or override stack exhausted; use the
		// regular lookup path.
		desc, err := p.getTableDesc(ctx, tn)
		return nil, desc, err
	}

	envKey := schemaPath{string(tn.DatabaseName), string(tn.TableName)}
	if ref, ok := env.schemaOverrides[envKey]; ok {
		// The schema is overriding the name-to-ID mapping. Use the result
		// of the override.
		return ref, nil, nil
	}

	// No override in this environment, recurse to the parent.
	return p.lookupDescOrRefByName(ctx, env.parent, tn)
}

// lookupDescByName goes one step beyond lookupDescOrRefByName to access
// the descriptor directly. This is suitable for uses of descriptors
// *other* than constructing a planNode for accessing a view or table.
// (For planNode construction, use getDataSource instead. This will
// properly handle the column list restriction that the environment can
// carry.)
func (p *planner) lookupDescByName(
	ctx context.Context, tn *parser.TableName,
) (*sqlbase.TableDescriptor, error) {
	tref, desc, err := p.lookupDescOrRefByName(ctx, p.lookupEnv, tn)
	if err != nil {
		return nil, err
	}
	if desc != nil {
		return desc, nil
	}
	return p.getTableDescByID(ctx, sqlbase.ID(tref.TableID))
}

// lookupIndexByName uses the environment overrides to look up an index
// by name given a table ID.
func (p *planner) lookupIndexByName(
	ctx context.Context, env *lookupEnvironment, desc *sqlbase.TableDescriptor, indexName string,
) (*sqlbase.IndexDescriptor, error) {
	if env == nil {
		// No override; look up in the table descriptor.
		if indexName == desc.PrimaryIndex.Name {
			return &desc.PrimaryIndex, nil
		}
		for i := range desc.Indexes {
			if indexName == desc.Indexes[i].Name {
				return &desc.Indexes[i], nil
			}
		}
		return nil, errors.Errorf("index %q not found", parser.ErrString(parser.Name(indexName)))
	}

	// Are there any overrides for this table ID in this scope?
	if descOverride, ok := env.descOverrides[desc.ID]; ok {
		var idxID sqlbase.IndexID
		// Is there an override specifically for this index name?
		for i, n := range descOverride.IndexNames {
			if n == indexName && i < len(descOverride.IndexIDs) {
				idxID = descOverride.IndexIDs[i]
				break
			}
		}
		if idxID != 0 {
			// An override was found, determine the index descriptor to use.
			if idxID == desc.PrimaryIndex.ID {
				return &desc.PrimaryIndex, nil
			}
			for i := range desc.Indexes {
				if idxID == desc.Indexes[i].ID {
					return &desc.Indexes[i], nil
				}
			}
		}
	}
	// Not found so far; recurse.
	return p.lookupIndexByName(ctx, env.parent, desc, indexName)
}
