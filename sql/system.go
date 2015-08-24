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
	"log"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
)

const (
	// MaxReservedDescID is the maximum reserved descriptor ID.
	// All objects with ID <= MaxReservedDescID are system object
	// with special rules.
	MaxReservedDescID ID = 999
	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID ID = 0

	// System IDs should remain <= MaxReservedDescID.
	// systemDatabaseID is the ID of the system database.
	systemDatabaseID ID = 1
	// namespaceTableID is the ID of the namespace table.
	namespaceTableID ID = 2
	// descriptorTableID is the ID of the descriptor table.
	descriptorTableID ID = 3

	// sql CREATE commands and full schema for each system table.
	// TODO(marc): wouldn't it be better to use a pre-parsed version?
	namespaceTableSchema = `
CREATE TABLE system.namespace (
  "parentID" INT,
  "name"     CHAR,
  "id"       INT,
  PRIMARY KEY (parentID, name)
);`

	descriptorTableSchema = `
CREATE TABLE system.descriptor (
  "id"   INT PRIMARY KEY,
  "desc" BLOB
);`
)

var (
	// SystemDB is the descriptor for the system database.
	SystemDB = DatabaseDescriptor{
		Name:       "system",
		ID:         systemDatabaseID,
		Privileges: NewSystemObjectPrivilegeDescriptor(),
	}

	// NamespaceTable is the descriptor for the namespace table.
	NamespaceTable = createSystemTable(namespaceTableID, namespaceTableSchema)

	// DescriptorTable is the descriptor for the descriptor table.
	DescriptorTable = createSystemTable(descriptorTableID, descriptorTableSchema)
)

func createSystemTable(id ID, cmd string) TableDescriptor {
	stmts, err := parser.Parse(cmd)
	if err != nil {
		log.Fatal(err)
	}

	desc, err := makeTableDesc(stmts[0].(*parser.CreateTable))
	if err != nil {
		log.Fatal(err)
	}

	desc.Privileges = SystemDB.Privileges
	desc.ID = id
	if err := desc.AllocateIDs(); err != nil {
		log.Fatal(err)
	}

	return desc
}

// GetInitialSystemValues returns a list of key/value pairs.
// They are written at cluster bootstrap time (see storage/node.go:BootstrapCLuster).
func GetInitialSystemValues() []proto.KeyValue {
	systemData := []struct {
		parentID ID
		desc     descriptorProto
	}{
		{RootNamespaceID, &SystemDB},
		{SystemDB.ID, &NamespaceTable},
		{SystemDB.ID, &DescriptorTable},
	}

	numEntries := 1 + len(systemData)*2
	ret := make([]proto.KeyValue, numEntries, numEntries)
	i := 0

	// We reserve the system IDs.
	value := proto.Value{}
	value.SetInteger(int64(MaxReservedDescID + 1))
	ret[i] = proto.KeyValue{
		Key:   keys.DescIDGenerator,
		Value: value,
	}
	i++

	// System database and tables.
	for _, d := range systemData {
		value = proto.Value{}
		value.SetInteger(int64(d.desc.GetID()))
		ret[i] = proto.KeyValue{
			Key:   MakeNameMetadataKey(d.parentID, d.desc.GetName()),
			Value: value,
		}
		i++

		value = proto.Value{}
		if err := value.SetProto(d.desc); err != nil {
			log.Fatalf("could not marshal %v", d.desc)
		}
		ret[i] = proto.KeyValue{
			Key:   MakeDescMetadataKey(d.desc.GetID()),
			Value: value,
		}
		i++
	}

	return ret
}

// IsSystemID returns true if this ID is reserved for system objects.
func IsSystemID(id ID) bool {
	return id > 0 && id <= MaxReservedDescID
}
