// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import "github.com/cockroachdb/cockroach/pkg/config/zonepb"

// ZoneConfig encapsulates zone config and raw bytes read from storage.
type ZoneConfig interface {
	// ZoneConfigProto returns the zone config proto buf struct.
	ZoneConfigProto() *zonepb.ZoneConfig

	// GetRawBytesInStorage returns the corresponding raw bytes read from storage.
	GetRawBytesInStorage() []byte

	// Size returns the underlying bytes used by the instance.
	Size() int
}
