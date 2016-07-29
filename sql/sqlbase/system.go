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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sqlbase

import (
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
	"golang.org/x/net/context"
)

const (
	// sql CREATE commands and full schema for each system table.

	// NamespaceTableSchema is checked in TestSystemTables.
	NamespaceTableSchema = `
CREATE TABLE system.namespace (
  parentID INT,
  name     STRING,
  id       INT,
  PRIMARY KEY (parentID, name)
);`

	// DescriptorTableSchema is checked in TestSystemTables.
	DescriptorTableSchema = `
CREATE TABLE system.descriptor (
  id         INT PRIMARY KEY,
  descriptor BYTES
);`

	// LeaseTableSchema is checked in TestSystemTables.
	LeaseTableSchema = `
CREATE TABLE system.lease (
  descID     INT,
  version    INT,
  nodeID     INT,
  expiration TIMESTAMP,
  PRIMARY KEY (descID, version, expiration, nodeID)
);`

	// UsersTableSchema is checked in TestSystemTables.
	UsersTableSchema = `
CREATE TABLE system.users (
  username       STRING PRIMARY KEY,
  hashedPassword BYTES
);`

	// ZonesTableSchema is checked in TestSystemTables.
	// Zone settings per DB/Table.
	ZonesTableSchema = `
CREATE TABLE system.zones (
  id     INT PRIMARY KEY,
  config BYTES
);`

	// UITableSchema is checked in TestSystemTables.
	// blobs based on unique keys.
	UITableSchema = `
CREATE TABLE system.ui (
	key         STRING PRIMARY KEY,
	value       BYTES,
	lastUpdated TIMESTAMP NOT NULL
);`
)

var (
	// SystemDB is the descriptor for the system database.
	SystemDB = DatabaseDescriptor{
		Name: "system",
		ID:   keys.SystemDatabaseID,
		// Assign max privileges to root user.
		Privileges: NewPrivilegeDescriptor(security.RootUser,
			SystemConfigAllowedPrivileges[keys.SystemDatabaseID]),
	}

	// NamespaceTable is the descriptor for the namespace table.
	NamespaceTable = CreateSystemConfigTable(keys.NamespaceTableID, NamespaceTableSchema)

	// DescriptorTable is the descriptor for the descriptor table.
	DescriptorTable = CreateSystemConfigTable(keys.DescriptorTableID, DescriptorTableSchema)

	// UsersTable is the descriptor for the users table.
	UsersTable = CreateSystemConfigTable(keys.UsersTableID, UsersTableSchema)

	// ZonesTable is the descriptor for the zones table.
	ZonesTable = CreateSystemConfigTable(keys.ZonesTableID, ZonesTableSchema)

	// SystemConfigAllowedPrivileges describes the privileges allowed for each
	// system config object. No user may have more than those privileges, and
	// the root user must have exactly those privileges. CREATE|DROP|ALL
	// should always be denied.
	SystemConfigAllowedPrivileges = map[ID]privilege.List{
		keys.SystemDatabaseID:  privilege.ReadData,
		keys.NamespaceTableID:  privilege.ReadData,
		keys.DescriptorTableID: privilege.ReadData,
		keys.UsersTableID:      privilege.ReadWriteData,
		keys.ZonesTableID:      privilege.ReadWriteData,
	}

	// NumSystemDescriptors should be set to the number of system descriptors
	// above (SystemDB and each system table). This is used by tests which need
	// to know the number of system descriptors intended for installation; it starts at
	// 1 for the SystemDB descriptor created above, and is incremented by every
	// call to createSystemTable().
	NumSystemDescriptors = 1
)

// CreateSystemConfigTable wraps transforming a CREATE stmt to a descriptor.
func CreateSystemConfigTable(id ID, schema string) TableDescriptor {
	NumSystemDescriptors++

	// System tables have the system database as a parent, with privileges
	// from the SystemAllowedPrivileges table assigned to the root user.
	return CreateTableDescriptor(id, keys.SystemDatabaseID, schema,
		NewPrivilegeDescriptor(security.RootUser, SystemConfigAllowedPrivileges[id]))
}

// CreateTableDescriptor transforms a CREATE stmt into a descriptor.
func CreateTableDescriptor(id, parentID ID, schema string, privileges *PrivilegeDescriptor) TableDescriptor {
	stmt, err := parser.ParseOneTraditional(schema)
	if err != nil {
		log.Fatal(context.TODO(), err)
	}

	desc, err := MakeTableDesc(stmt.(*parser.CreateTable), parentID)
	if err != nil {
		log.Fatal(context.TODO(), err)
	}

	desc.Privileges = privileges

	desc.ID = id
	if err := desc.AllocateIDs(); err != nil {
		log.Fatalf(context.TODO(), "%s: %v", desc.Name, err)
	}

	return desc
}

// Create the key/value pairs for the default zone config entry.
func createDefaultZoneConfig() []roachpb.KeyValue {
	var ret []roachpb.KeyValue
	value := roachpb.Value{}
	desc := config.DefaultZoneConfig()
	if err := value.SetProto(&desc); err != nil {
		log.Fatalf(context.TODO(), "could not marshal %v", desc)
	}
	ret = append(ret, roachpb.KeyValue{
		Key:   MakeZoneKey(keys.RootNamespaceID),
		Value: value,
	})
	return ret
}

// addSystemDatabaseToSchema populates the supplied MetadataSchema with the
// System database and its tables. The descriptors for these objects exist
// statically in this file, but a MetadataSchema can be used to persist these
// descriptors to the cockroach store.
func addSystemDatabaseToSchema(target *MetadataSchema) {
	// Add system database.
	target.AddDescriptor(keys.RootNamespaceID, &SystemDB)

	// Add system config tables.
	target.AddDescriptor(keys.SystemDatabaseID, &NamespaceTable)
	target.AddDescriptor(keys.SystemDatabaseID, &DescriptorTable)
	target.AddDescriptor(keys.SystemDatabaseID, &UsersTable)
	target.AddDescriptor(keys.SystemDatabaseID, &ZonesTable)

	// Add other system tables.
	target.AddTable(keys.LeaseTableID, LeaseTableSchema)
	target.AddTable(keys.UITableID, UITableSchema)

	target.otherKV = append(target.otherKV, createDefaultZoneConfig()...)
}

// IsSystemConfigID returns true if this ID is for a system config object.
func IsSystemConfigID(id ID) bool {
	return id > 0 && id <= keys.MaxSystemConfigDescID
}
