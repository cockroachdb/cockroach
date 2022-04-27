// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterunique

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

// ID represents an identifier that is guaranteed to be unique across
// a cluster. It is a wrapper around a uint128. It logically consists of 96 bits
// of HLC timestamp, and 32 bits of node ID.
type ID struct {
	uint128.Uint128
}

// GenerateID takes a timestamp and SQLInstanceID, and generates a
// ID.
func GenerateID(timestamp hlc.Timestamp, instID base.SQLInstanceID) ID {
	loInt := (uint64)(instID)
	loInt = loInt | ((uint64)(timestamp.Logical) << 32)

	return ID{Uint128: uint128.FromInts((uint64)(timestamp.WallTime), loInt)}
}

// IDFromString converts a string to a ID. If the string is
// not a valid uint128, an error is returned.
func IDFromString(s string) (ID, error) {
	id, err := uint128.FromString(s)
	if err != nil {
		return ID{}, err
	}
	return ID{Uint128: id}, nil
}

// IDFromBytes converts raw bytes into a ID.
// The caller is responsible for ensuring the byte slice contains 16 bytes.
func IDFromBytes(b []byte) ID {
	id := uint128.FromBytes(b)
	return ID{Uint128: id}
}

// GetNodeID extracts the node ID from a ID.
func (id ID) GetNodeID() int32 {
	return int32(0xFFFFFFFF & id.Lo)
}
