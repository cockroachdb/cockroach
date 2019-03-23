// Copyright 2018 The Cockroach Authors.
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

package privilegepb

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/descid"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// AdminRole is the default (and non-droppable) role with superuser privileges.
var AdminRole = "admin"

// PublicRole is the special "public" pseudo-role.
// All users are implicit members of "public". The role cannot be created,
// dropped, assigned to another role, and is generally not listed.
// It can be granted privileges, implicitly granting them to all users (current and future).
var PublicRole = "public"

// SystemAllowedPrivileges describes the allowable privilege list for each
// system object. Super users (root and admin) must have exactly the specified privileges,
// other users must not exceed the specified privileges.
var SystemAllowedPrivileges = map[descid.T]privilege.List{
	keys.SystemDatabaseID:  privilege.ReadData,
	keys.NamespaceTableID:  privilege.ReadData,
	keys.DescriptorTableID: privilege.ReadData,
	keys.UsersTableID:      privilege.ReadWriteData,
	keys.ZonesTableID:      privilege.ReadWriteData,
	// We eventually want to migrate the table to appear read-only to force the
	// the use of a validating, logging accessor, so we'll go ahead and tolerate
	// read-only privs to make that migration possible later.
	keys.SettingsTableID:   privilege.ReadWriteData,
	keys.LeaseTableID:      privilege.ReadWriteData,
	keys.EventLogTableID:   privilege.ReadWriteData,
	keys.RangeEventTableID: privilege.ReadWriteData,
	keys.UITableID:         privilege.ReadWriteData,
	// IMPORTANT: CREATE|DROP|ALL privileges should always be denied or database
	// users will be able to modify system tables' schemas at will. CREATE and
	// DROP privileges are allowed on the above system tables for backwards
	// compatibility reasons only!
	keys.JobsTableID:            privilege.ReadWriteData,
	keys.WebSessionsTableID:     privilege.ReadWriteData,
	keys.TableStatisticsTableID: privilege.ReadWriteData,
	keys.LocationsTableID:       privilege.ReadWriteData,
	keys.RoleMembersTableID:     privilege.ReadWriteData,
	keys.CommentsTableID:        privilege.ReadWriteData,
}
