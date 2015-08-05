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
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

func makeDatabaseDesc(p *parser.CreateDatabase) structured.DatabaseDescriptor {
	return structured.DatabaseDescriptor{
		Name: p.Name.String(),
		PrivilegeDescriptor: structured.PrivilegeDescriptor{
			Read:  []string{security.RootUser},
			Write: []string{security.RootUser},
		},
	}
}

// getDatabaseDesc looks up the database descriptor given its name.
func (p *planner) getDatabaseDesc(name string) (*structured.DatabaseDescriptor, error) {
	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, name)
	desc := structured.DatabaseDescriptor{}
	if err := p.getDescriptor(nameKey, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}
