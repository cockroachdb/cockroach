// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
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
	"github.com/cockroachdb/cockroach/util"
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

// checkPrivilege verifies that p.user has `privilege` on `descriptor`.
func (p *planner) checkPrivilege(descriptor descriptorProto, privilege privilege.Kind) error {
	if descriptor.GetPrivileges().CheckPrivilege(p.user, privilege) {
		return nil
	}
	return fmt.Errorf("user %s does not have %s privilege on %s %s",
		p.user, privilege, descriptor.TypeName(), descriptor.GetName())
}

// createDescriptor takes a Table or Database descriptor and creates it
// if needed, incrementing the descriptor counter.
func (p *planner) createDescriptor(plainKey descriptorKey, descriptor descriptorProto, ifNotExists bool) error {
	idKey := plainKey.Key()
	// Check whether idKey exists.
	gr, err := p.txn.Get(idKey)
	if err != nil {
		return err
	}

	if gr.Exists() {
		if ifNotExists {
			// Noop.
			return nil
		}
		// Key exists, but we don't want it to: error out.
		return fmt.Errorf("%s %q already exists", descriptor.TypeName(), plainKey.Name())
	}

	// Increment unique descriptor counter.
	if ir, err := p.txn.Inc(keys.DescIDGenerator, 1); err == nil {
		descriptor.SetID(ID(ir.ValueInt() - 1))
	} else {
		return err
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

	p.testingVerifyMetadata = func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, idKey, descID); err != nil {
			return err
		}
		return expectDescriptor(systemConfig, descKey, descDesc)
	}

	return p.txn.Run(&b)
}

// getDescriptor looks up the descriptor for `plainKey`, validates it,
// and unmarshals it into `descriptor`.
func (p *planner) getDescriptor(plainKey descriptorKey, descriptor descriptorProto) error {
	gr, err := p.txn.Get(plainKey.Key())
	if err != nil {
		return err
	}
	if !gr.Exists() {
		return fmt.Errorf("%s %q does not exist", descriptor.TypeName(), plainKey.Name())
	}

	descKey := MakeDescMetadataKey(ID(gr.ValueInt()))
	desc := &Descriptor{}
	if err := p.txn.GetProto(descKey, desc); err != nil {
		return err
	}

	switch t := descriptor.(type) {
	case *TableDescriptor:
		table := desc.GetTable()
		if table == nil {
			return util.Errorf("%q is not a table", plainKey.Name())
		}
		*t = *table
	case *DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return util.Errorf("%q is not a database", plainKey.Name())
		}
		*t = *database
	}

	return descriptor.Validate()
}

// getDescriptorFromTargetList examines a TargetList and fetches the
// appropriate descriptor.
// TODO(marc): support multiple targets.
func (p *planner) getDescriptorFromTargetList(targets parser.TargetList) (descriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		} else if len(targets.Databases) != 1 {
			return nil, util.Errorf("TODO(marc): multiple targets not implemented")
		}
		descriptor, err := p.getDatabaseDesc(targets.Databases[0])
		if err != nil {
			return nil, err
		}
		return descriptor, nil
	}

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	} else if len(targets.Tables) != 1 {
		return nil, util.Errorf("TODO(marc): multiple targets not implemented")
	}
	descriptor, err := p.getTableDesc(targets.Tables[0])
	if err != nil {
		return nil, err
	}
	return descriptor, nil
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
