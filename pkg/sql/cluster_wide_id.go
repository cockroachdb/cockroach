// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

// ClusterWideID represents an identifier that is guaranteed to be unique across
// a cluster. It is a wrapper around a uint128. It logically consists of 96 bits
// of HLC timestamp, and 32 bits of node ID.
type ClusterWideID struct {
	uint128.Uint128
}

// GenerateClusterWideID takes a timestamp and SQLInstanceID, and generates a
// ClusterWideID.
func GenerateClusterWideID(timestamp hlc.Timestamp, instID base.SQLInstanceID) ClusterWideID {
	loInt := (uint64)(instID)
	loInt = loInt | ((uint64)(timestamp.Logical) << 32)

	return ClusterWideID{Uint128: uint128.FromInts((uint64)(timestamp.WallTime), loInt)}
}

// StringToClusterWideID converts a string to a ClusterWideID. If the string is
// not a valid uint128, an error is returned.
func StringToClusterWideID(s string) (ClusterWideID, error) {
	id, err := uint128.FromString(s)
	if err != nil {
		return ClusterWideID{}, err
	}
	return ClusterWideID{Uint128: id}, nil
}

// BytesToClusterWideID converts raw bytes into a ClusterWideID.
// The caller is responsible for ensuring the byte slice contains 16 bytes.
func BytesToClusterWideID(b []byte) ClusterWideID {
	id := uint128.FromBytes(b)
	return ClusterWideID{Uint128: id}
}

// GetNodeID extracts the node ID from a ClusterWideID.
func (id ClusterWideID) GetNodeID() int32 {
	return int32(0xFFFFFFFF & id.Lo)
}
