// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storelivenesspb

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// Epoch is an epoch in the Store Liveness fabric, referencing an uninterrupted
// period of support from one store to another.
type Epoch int64

// Expiration is a timestamp indicating the extent of support from one store to
// another in the Store Liveness fabric within a given epoch. This expiration
// may be extended through the provision of additional support.
type Expiration hlc.Timestamp

// String is like Timestamp.Forward, but for String.
func (t Expiration) String() string { return (hlc.Timestamp)(t).String() }

// IsEmpty is like Timestamp.IsEmpty, but for Expiration.
func (t Expiration) IsEmpty() bool { return (hlc.Timestamp)(t).IsEmpty() }

// Forward is like Timestamp.Forward, but for Expiration.
func (t *Expiration) Forward(s Expiration) bool { return (*hlc.Timestamp)(t).Forward(hlc.Timestamp(s)) }

// Reset implements the protoutil.Message interface.
func (t *Expiration) Reset() { (*hlc.Timestamp)(t).Reset() }

// ProtoMessage implements the protoutil.Message interface.
func (t *Expiration) ProtoMessage() {}

// MarshalTo implements the protoutil.Message interface.
func (t *Expiration) MarshalTo(data []byte) (int, error) { return (*hlc.Timestamp)(t).MarshalTo(data) }

// MarshalToSizedBuffer implements the protoutil.Message interface.
func (t *Expiration) MarshalToSizedBuffer(data []byte) (int, error) {
	return (*hlc.Timestamp)(t).MarshalToSizedBuffer(data)
}

// Unmarshal implements the protoutil.Message interface.
func (t *Expiration) Unmarshal(data []byte) error { return (*hlc.Timestamp)(t).Unmarshal(data) }

// Size implements the protoutil.Message interface.
func (t *Expiration) Size() int { return (*hlc.Timestamp)(t).Size() }
