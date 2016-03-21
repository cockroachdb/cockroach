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

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

var (
	errEmptyDatabaseName = errors.New("empty database name")
	errNoDatabase        = errors.New("no database specified")
	errNoTable           = errors.New("no table specified")
)

var _ descriptorProto = &DatabaseDescriptor{}
var _ descriptorProto = &TableDescriptor{}

// descriptorKey is the interface implemented by both
// DatabaseKey and TableKey. It is used to easily get the
// descriptor key and plain name.
type descriptorKey interface {
	Key() roachpb.Key
	Name() string
}

// descriptorProto is the interface implemented by both DatabaseDescriptor
// and TableDescriptor.
// TODO(marc): this is getting rather large.
type descriptorProto interface {
	proto.Message
	GetPrivileges() *PrivilegeDescriptor
	GetID() ID
	SetID(ID)
	TypeName() string
	GetName() string
	SetName(string)
	Validate() error
}

// checkPrivilege verifies that p.session.User has `privilege` on `descriptor`.
func (p *planner) checkPrivilege(descriptor descriptorProto, privilege privilege.Kind) error {
	if descriptor.GetPrivileges().CheckPrivilege(p.session.User, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		p.session.User, privilege, descriptor.TypeName(), descriptor.GetName())
}

// createDescriptor takes a Table or Database descriptor and creates it if
// needed, incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed.
func (p *planner) createDescriptor(plainKey descriptorKey, descriptor descriptorProto, ifNotExists bool) (bool, *roachpb.Error) {
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
		return false, roachpb.NewUErrorf("%s %q already exists", descriptor.TypeName(), plainKey.Name())
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
	descDesc := wrapDescriptor(descriptor)
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
func (p *planner) getDescriptor(plainKey descriptorKey, descriptor descriptorProto) *roachpb.Error {
	gr, err := p.txn.Get(plainKey.Key())
	if err != nil {
		return err
	}
	if !gr.Exists() {
		return roachpb.NewUErrorf("%s %q does not exist", descriptor.TypeName(), plainKey.Name())
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	if pErr := p.txn.GetProto(descKey, desc); pErr != nil {
		return pErr
	}

	switch t := descriptor.(type) {
	case *TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return roachpb.NewErrorf("%q is not a table", plainKey.Name())
		}
		*t = *table
	case *DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return roachpb.NewErrorf("%q is not a database", plainKey.Name())
		}
		*t = *database
	}

	return roachpb.NewError(descriptor.Validate())
}

// getDescriptorsFromTargetList examines a TargetList and fetches the
// appropriate descriptors.
func (p *planner) getDescriptorsFromTargetList(targets parser.TargetList) (
	[]descriptorProto, *roachpb.Error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, roachpb.NewError(errNoDatabase)
		}
		descs := make([]descriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, pErr := p.getDatabaseDesc(database)
			if pErr != nil {
				return nil, pErr
			}
			descs = append(descs, descriptor)
		}
		return descs, nil
	}

	if len(targets.Tables) == 0 {
		return nil, roachpb.NewError(errNoTable)
	}
	descs := make([]descriptorProto, 0, len(targets.Tables))
	for _, tableGlob := range targets.Tables {
		tables, pErr := p.expandTableGlob(tableGlob)
		if pErr != nil {
			return nil, pErr
		}
		for _, table := range tables {
			descriptor, err := p.getTableDesc(table)
			if err != nil {
				return nil, err
			}
			descs = append(descs, &descriptor)
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
	parser.QualifiedNames, *roachpb.Error) {
	if len(expr.Indirect) == 0 {
		return parser.QualifiedNames{expr}, nil
	}

	if err := expr.QualifyWithDatabase(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}
	// We must have a single indirect: either .table or .*
	if len(expr.Indirect) != 1 {
		return nil, roachpb.NewErrorf("invalid table glob: %s", expr)
	}

	switch expr.Indirect[0].(type) {
	case parser.NameIndirection:
		return parser.QualifiedNames{expr}, nil
	case parser.StarIndirection:
		dbDesc, pErr := p.getDatabaseDesc(string(expr.Base))
		if pErr != nil {
			return nil, pErr
		}
		tableNames, pErr := p.getTableNames(dbDesc)
		if pErr != nil {
			return nil, pErr
		}
		return tableNames, nil
	default:
		return nil, roachpb.NewErrorf("invalid table glob: %s", expr)
	}
}

func wrapDescriptor(descriptor descriptorProto) *Descriptor {
	desc := &Descriptor{}
	switch t := descriptor.(type) {
	case *TableDescriptor:
		desc.Union = &Descriptor_Table{Table: t}
	case *DatabaseDescriptor:
		desc.Union = &Descriptor_Database{Database: t}
	default:
		panic(fmt.Sprintf("unknown descriptor type: %s", descriptor.TypeName()))
	}
	return desc
}
