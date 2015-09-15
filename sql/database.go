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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// databaseKey implements descriptorKey.
type databaseKey struct {
	name string
}

func (dk databaseKey) Key() proto.Key {
	return MakeNameMetadataKey(keys.RootNamespaceID, dk.name)
}

func (dk databaseKey) Name() string {
	return dk.name
}

func makeDatabaseDesc(p *parser.CreateDatabase) DatabaseDescriptor {
	return DatabaseDescriptor{
		Name:       p.Name.String(),
		Privileges: NewDefaultPrivilegeDescriptor(),
	}
}

// getDatabaseDesc looks up the database descriptor given its name.
func (p *planner) getDatabaseDesc(name string) (*DatabaseDescriptor, error) {
	desc := DatabaseDescriptor{}
	if err := p.getDescriptor(databaseKey{name}, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}
