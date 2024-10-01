// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterunique

import (
	"encoding/json"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ID represents an identifier that is guaranteed to be unique across
// a cluster. It is a wrapper around a uint128. It logically consists of 96 bits
// of HLC timestamp, and 32 bits of node ID.
type ID struct {
	uint128.Uint128
}

var _ redact.SafeValue = ID{}

// SafeValue implements the redact.SafeValue interface.
func (ID) SafeValue() {}

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

// Size returns the marshalled size of id in bytes.
func (id ID) Size() int {
	return len(id.GetBytes())
}

// MarshalTo marshals id to data.
func (id ID) MarshalTo(data []byte) (int, error) {
	return copy(data, id.GetBytes()), nil
}

// Unmarshal unmarshals data to id.
func (id *ID) Unmarshal(data []byte) error {
	if len(data) != 16 {
		return errors.Errorf("input data %s for uint128 must be 16 bytes", data)
	}
	id.Uint128 = uint128.FromBytes(data)
	return nil
}

// MarshalJSON returns the JSON encoding of u.
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// UnmarshalJSON unmarshal the JSON encoded data into u.
func (id *ID) UnmarshalJSON(data []byte) error {
	var uint128String string
	if err := json.Unmarshal(data, &uint128String); err != nil {
		return err
	}

	uint128, err := uint128.FromString(uint128String)
	if err != nil {
		return err
	}

	id.Uint128 = uint128
	return nil
}

// ID implements llrb.Comparable.
// While these IDs don't really have a global ordering, it is convenient to
// be able to use them as keys in our `cache.OrderedCache`: their ordering
// is at least monotonically increasing within the same SQL instance, and
// that can be useful.
var _ llrb.Comparable = ID{}

// Compare returns a value indicating the sort order relationship between the
// receiver and the parameter.
func (id ID) Compare(o llrb.Comparable) int {
	return id.Uint128.Compare(o.(ID).Uint128)
}
