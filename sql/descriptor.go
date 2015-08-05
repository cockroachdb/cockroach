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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

var _ descriptorProto = &structured.DatabaseDescriptor{}
var _ descriptorProto = &structured.TableDescriptor{}

// descriptorProto is the interface implemented by both DatabaseDescriptor
// and TableDescriptor.
// TODO(marc): this is getting rather large.
type descriptorProto interface {
	gogoproto.Message
	Grant(*parser.Grant) error
	Revoke(*parser.Revoke) error
	Show() (structured.UserPrivilegeList, error)
	HasPrivilege(string, parser.PrivilegeType) bool
	GetID() structured.ID
	SetID(structured.ID)
	GetName() string
	Validate() error
}

// writeDescriptor takes a Table or Database descriptor and writes it
// if needed, incrementing the descriptor counter.
func (p *planner) writeDescriptor(key proto.Key, descriptor descriptorProto, ifNotExists bool) error {
	// Check whether key exists.
	gr, err := p.db.Get(key)
	if err != nil {
		return err
	}

	if gr.Exists() {
		if ifNotExists {
			// Noop.
			return nil
		}
		// Key exists, but we don't want it to: error out.
		// TODO(marc): prettify the error (strip stuff off the type name)
		return fmt.Errorf("%T \"%s\" already exists", descriptor, key.String())
	}

	// Increment unique descriptor counter.
	if ir, err := p.db.Inc(keys.DescIDGenerator, 1); err == nil {
		descriptor.SetID(structured.ID(ir.ValueInt() - 1))
	} else {
		return err
	}

	// TODO(pmattis): The error currently returned below is likely going to be
	// difficult to interpret.
	// TODO(pmattis): Need to handle if-not-exists here as well.
	descKey := structured.MakeDescMetadataKey(descriptor.GetID())
	return p.db.Txn(func(txn *client.Txn) error {
		b := &client.Batch{}
		b.CPut(key, descKey, nil)
		b.CPut(descKey, descriptor, nil)
		return txn.Commit(b)
	})
}

// getDescriptor looks up the descriptor at `key`, validates it,
// and unmarshals it into `descriptor`.
func (p *planner) getDescriptor(key proto.Key, descriptor descriptorProto) error {
	gr, err := p.db.Get(key)
	if err != nil {
		return err
	}
	if !gr.Exists() {
		// TODO(marc): prettify the error (strip stuff off the type name)
		return fmt.Errorf("%T \"%s\" does not exist", descriptor, key.String())
	}

	descKey := gr.ValueBytes()
	if err := p.db.GetProto(descKey, descriptor); err != nil {
		return err
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

	if targets.Tables == nil {
		return nil, util.Errorf("no targets speciied")
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
