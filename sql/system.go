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
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

const (
	// MaxReservedDescID is the maximum reserved descriptor ID.
	// All objects with ID <= MaxReservedDescID are system object
	// with special rules.
	MaxReservedDescID ID = keys.MaxReservedDescID
	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID ID = 0

	// System IDs should remain <= MaxReservedDescID.
	systemDatabaseID  ID = 1
	namespaceTableID  ID = 2
	descriptorTableID ID = 3
	usersTableID      ID = 4
	zonesTableID      ID = 5

	// sql CREATE commands and full schema for each system table.
	namespaceTableSchema = `
CREATE TABLE system.namespace (
  parentID INT,
  name     CHAR,
  id       INT,
  PRIMARY KEY (parentID, name)
);`

	descriptorTableSchema = `
CREATE TABLE system.descriptor (
  id         INT PRIMARY KEY,
  descriptor BLOB
);`

	usersTableSchema = `
CREATE TABLE system.users (
  username       CHAR PRIMARY KEY,
  hashedPassword BLOB
);`

	// Zone settings per DB/Table.
	zonesTableSchema = `
CREATE TABLE system.zones (
  id     INT PRIMARY KEY,
  config BLOB
);`
)

var (
	// SystemDB is the descriptor for the system database.
	SystemDB = DatabaseDescriptor{
		Name: "system",
		ID:   systemDatabaseID,
		// Assign max privileges to root user.
		Privileges: NewPrivilegeDescriptor(security.RootUser,
			SystemAllowedPrivileges[systemDatabaseID]),
	}

	// NamespaceTable is the descriptor for the namespace table.
	NamespaceTable = createSystemTable(namespaceTableID, namespaceTableSchema)

	// DescriptorTable is the descriptor for the descriptor table.
	DescriptorTable = createSystemTable(descriptorTableID, descriptorTableSchema)

	// UsersTable is the descriptor for the users table.
	UsersTable = createSystemTable(usersTableID, usersTableSchema)

	// ZonesTable is the descriptor for the zones table.
	ZonesTable = createSystemTable(zonesTableID, zonesTableSchema)

	// SystemAllowedPrivileges describes the privileges allowed for each
	// system object. No user may have more than those privileges, and
	// the root user must have exactly those privileges.
	// CREATE|DROP|ALL should always be denied.
	SystemAllowedPrivileges = map[ID]privilege.List{
		systemDatabaseID:  privilege.ReadData,
		namespaceTableID:  privilege.ReadData,
		descriptorTableID: privilege.ReadData,
		usersTableID:      privilege.ReadWriteData,
		zonesTableID:      privilege.ReadWriteData,
	}

	// NumUsedSystemIDs is only used in tests that need to know the
	// number of system objects created at initialization.
	// It gets automatically set to "number of created system tables"
	// + 1 (for system database).
	NumUsedSystemIDs = 1
)

func createSystemTable(id ID, cmd string) TableDescriptor {
	stmts, err := parser.ParseTraditional(cmd)
	if err != nil {
		log.Fatal(err)
	}

	desc, err := makeTableDesc(stmts[0].(*parser.CreateTable))
	if err != nil {
		log.Fatal(err)
	}

	// Assign max privileges to root user.
	desc.Privileges = NewPrivilegeDescriptor(security.RootUser,
		SystemAllowedPrivileges[id])

	desc.ID = id
	if err := desc.AllocateIDs(); err != nil {
		log.Fatal(err)
	}

	NumUsedSystemIDs++
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
		{SystemDB.ID, &UsersTable},
		{SystemDB.ID, &ZonesTable},
	}

	// Initial kv pairs:
	// - ID generator
	// - 2 per table/database
	numEntries := 1 + len(systemData)*2
	ret := make([]proto.KeyValue, numEntries, numEntries)
	i := 0

	// Descriptor ID generator.
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
