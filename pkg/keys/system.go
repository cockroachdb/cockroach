// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// SystemIDChecker determines whether a table ID corresponds to a system
// table. In the earlier days of cockroachdb (prior to 22.1), these IDs were
// all constants and had a value less than 50. In later versions, these IDs
// are generated dynamically.
type SystemIDChecker interface {

	// IsSystemID is used to determine whether a descriptor ID is part
	// of the system database. Implementations do not need to be aware of the
	// type of descriptor which may correspond to this ID. It may be that the
	// descriptor does not exist. However, if it returns true and a descriptor
	// exists with the given ID, that descriptor is part of the system database.
	IsSystemID(id uint32) bool
}

// DeprecatedSystemIDChecker returns the deprecated implementation of
// SystemIDChecker.
func DeprecatedSystemIDChecker() SystemIDChecker {
	return &deprecatedSystemIDChecker{}
}

type deprecatedSystemIDChecker struct{}

func (d deprecatedSystemIDChecker) IsSystemID(id uint32) bool {
	return id < minUserDescID
}

var _ SystemIDChecker = (*deprecatedSystemIDChecker)(nil)

// MinUserDescriptorID returns the smallest possible non-system descriptor ID
// after a cluster is bootstrapped.
func MinUserDescriptorID(idChecker SystemIDChecker) uint32 {
	id := uint32(MaxSystemConfigDescID + 1)
	for idChecker.IsSystemID(id) {
		id++
	}
	return id
}

// TestingSystemIDChecker returns an implementation of SystemIDChecker suitable
// for simple unit tests.
func TestingSystemIDChecker() SystemIDChecker {
	return &deprecatedSystemIDChecker{}
}

// TestingUserDescID is a convenience function which returns a user ID offset
// from the minimum value allowed in a simple unit test setting.
func TestingUserDescID(offset uint32) uint32 {
	return MinUserDescriptorID(TestingSystemIDChecker()) + offset
}

// TestingUserTableDataMin is a convenience function which returns the first
// user table data key in a simple unit test setting.
func TestingUserTableDataMin() roachpb.Key {
	return SystemSQLCodec.TablePrefix(TestingUserDescID(0))
}
