// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import "fmt"

// ZoneKey is the index of the first level in the constraint conformance report.
type ZoneKey struct {
	// ZoneID is the id of the zone this key is referencing.
	ZoneID uint32
	// SubzoneID identifies what subzone, if any, this key is referencing. The
	// zero value (also named NoSubzone) indicates that the key is referring to a
	// zone, not a subzone.
	SubzoneID SubzoneID
}

// NoSubzone is used inside a zoneKey to indicate that the key represents a
// zone, not a subzone.
const NoSubzone SubzoneID = 0

// SubzoneID represents a subzone within a zone. It's the subzone's index within
// the parent zone + 1; there's no subzone 0 so that 0 can be used as a
// sentinel.
type SubzoneID uint32

// SubzoneIDFromIndex turns a subzone's index within its parent zone into its
// SubzoneID.
func SubzoneIDFromIndex(idx int) SubzoneID {
	return SubzoneID(idx + 1)
}

// MakeZoneKey creates a zoneKey.
//
// Use NoSubzone for subzoneID to indicate that the key references a zone, not a
// subzone.
func MakeZoneKey(zoneID uint32, subzoneID SubzoneID) ZoneKey {
	return ZoneKey{
		ZoneID:    zoneID,
		SubzoneID: subzoneID,
	}
}

func (k ZoneKey) String() string {
	return fmt.Sprintf("%d,%d", k.ZoneID, k.SubzoneID)
}

// Less compares two ZoneKeys.
func (k ZoneKey) Less(other ZoneKey) bool {
	if k.ZoneID < other.ZoneID {
		return true
	}
	if k.ZoneID > other.ZoneID {
		return false
	}
	return k.SubzoneID < other.SubzoneID
}
