// Copyright 2016 The Cockroach Authors.
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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
)

var (
	errEmptyDatabaseName = errors.New("empty database name")
	errNoDatabase        = errors.New("no database specified")
	errNoTable           = errors.New("no table specified")
)

// checkPrivilege verifies that p.session.User has `privilege` on `descriptor`.
func (p *planner) checkPrivilege(descriptor sqlbase.DescriptorProto, privilege privilege.Kind) error {
	if descriptor.GetPrivileges().CheckPrivilege(p.session.User, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		p.session.User, privilege, descriptor.TypeName(), descriptor.GetName())
}

// createDescriptor takes a Table or Database descriptor and creates it if
// needed, incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed.
func (p *planner) createDescriptor(plainKey sqlbase.DescriptorKey, descriptor sqlbase.DescriptorProto, ifNotExists bool) (bool, error) {
	idKey := plainKey.Key()
	// Check whether idKey exists.
	gr, err := p.txn.Get(idKey)
	if err != nil {
		return false, err
	}

	if gr.Exists() {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		// Key exists, but we don't want it to: error out.
		return false, fmt.Errorf("%s %q already exists", descriptor.TypeName(), plainKey.Name())
	}

	// Increment unique descriptor counter.
	if ir, err := p.txn.Inc(keys.DescIDGenerator, 1); err == nil {
		descriptor.SetID(ID(ir.ValueInt() - 1))
	} else {
		return false, err
	}

	// TODO(pmattis): The error currently returned below is likely going to be
	// difficult to interpret.
	//
	// TODO(pmattis): Need to handle if-not-exists here as well.
	//
	// TODO(pmattis): This is writing the namespace and descriptor table entries,
	// but not going through the normal INSERT logic and not performing a precise
	// mimicry. In particular, we're only writing a single key per table, while
	// perfect mimicry would involve writing a sentinel key for each row as well.
	descKey := MakeDescMetadataKey(descriptor.GetID())

	b := client.Batch{}
	descID := descriptor.GetID()
	descDesc := sqlbase.WrapDescriptor(descriptor)
	b.CPut(idKey, descID, nil)
	b.CPut(descKey, descDesc, nil)

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, idKey, descID); err != nil {
			return err
		}
		return expectDescriptor(systemConfig, descKey, descDesc)
	})

	return true, p.txn.Run(&b)
}

// getDescriptor looks up the descriptor for `plainKey`, validates it,
// and unmarshals it into `descriptor`.
// If `plainKey` doesn't exist, returns false and nil error.
// In most cases you'll want to use wrappers: `getDatabaseDesc` or
// `getTableDesc`.
func (p *planner) getDescriptor(plainKey sqlbase.DescriptorKey, descriptor sqlbase.DescriptorProto,
) (bool, error) {
	gr, err := p.txn.Get(plainKey.Key())
	if err != nil {
		return false, err
	}
	if !gr.Exists() {
		return false, nil
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	if err := p.txn.GetProto(descKey, desc); err != nil {
		return false, err
	}

	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return false, util.Errorf("%q is not a table", plainKey.Name())
		}
		*t = *table
	case *sqlbase.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return false, util.Errorf("%q is not a database", plainKey.Name())
		}
		*t = *database
	}

	if err := descriptor.Validate(); err != nil {
		return false, err
	}
	return true, nil
}

// getDescriptorsFromTargetList examines a TargetList and fetches the
// appropriate descriptors.
func (p *planner) getDescriptorsFromTargetList(targets parser.TargetList) (
	[]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.getDatabaseDesc(database)
			if err != nil {
				return nil, err
			}
			if descriptor == nil {
				return nil, databaseDoesNotExistError(database)
			}
			descs = append(descs, descriptor)
		}
		return descs, nil
	}

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	}
	descs := make([]sqlbase.DescriptorProto, 0, len(targets.Tables))
	for _, tableGlob := range targets.Tables {
		tables, err := p.expandTableGlob(tableGlob)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			descriptor, err := p.getTableDesc(table)
			if err != nil {
				return nil, err
			}
			if descriptor == nil {
				return nil, tableDoesNotExistError(table.String())
			}
			descs = append(descs, descriptor)
		}
	}
	return descs, nil
}

// expandTableGlob expands wildcards from the end of `expr` and
// returns the list of matching tables.
// `expr` is possibly modified to be qualified with the database it refers to.
// `expr` is assumed to be of one of several forms:
// 		database.table
// 		table
// 		*
func (p *planner) expandTableGlob(expr *parser.QualifiedName) (
	parser.QualifiedNames, error) {
	if len(expr.Indirect) == 0 {
		return parser.QualifiedNames{expr}, nil
	}

	if err := expr.QualifyWithDatabase(p.session.Database); err != nil {
		return nil, err
	}
	// We must have a single indirect: either .table or .*
	if len(expr.Indirect) != 1 {
		return nil, fmt.Errorf("invalid table glob: %s", expr)
	}

	switch expr.Indirect[0].(type) {
	case parser.NameIndirection:
		return parser.QualifiedNames{expr}, nil
	case parser.StarIndirection:
		dbDesc, err := p.getDatabaseDesc(string(expr.Base))
		if err != nil {
			return nil, err
		}
		if dbDesc == nil {
			return nil, databaseDoesNotExistError(string(expr.Base))
		}
		tableNames, err := p.getTableNames(dbDesc)
		if err != nil {
			return nil, err
		}
		return tableNames, nil
	default:
		return nil, fmt.Errorf("invalid table glob: %s", expr)
	}
}
