// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwirecancel

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/base"
)

// BackendKeyData is a 64-bit identifier used by the pgwire protocol to cancel
// queries. It is created at the time of session initialization. It contains the
// SQLInstanceID of the node. SQLInstanceID is an alias of int32, but we use a
// variable number of bits here. The leading bit will only be set if the
// SQLInstanceID is greater than or equal to 2^11. If it is set, the other 31
// bits are used for the ID; otherwise 11 bits are used for the ID. This is safe
// because SQLInstanceID is always a non-negative integer. The remaining bits
// (either 32 bits or 52 bits) are random, and are used to uniquely identify a
// session on that SQL node.
//
// See information about how Postgres uses it here:
// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.9
type BackendKeyData uint64

const (
	lower32BitsMask uint64 = 0x00000000FFFFFFFF
	lower52BitsMask uint64 = 0x000FFFFFFFFFFFFF
	leadingBitMask         = 1 << 63
)

// MakeBackendKeyData creates a new BackendKayData that contains the given
// SQLInstanceID. The rest of the data are random bits. The number of random
// bits is larger of the SQLInstanceID is small enough.
func MakeBackendKeyData(rng *rand.Rand, sqlInstanceID base.SQLInstanceID) BackendKeyData {
	ret := rng.Uint64()
	if sqlInstanceID < 1<<11 {
		// Only keep the lower 52 bits
		ret = ret & lower52BitsMask
		// Set the upper 12 bits based on the sqlInstanceID.
		ret = ret | (uint64(sqlInstanceID) << 52)
	} else {
		// Only keep the lower 32 bits.
		ret = ret & lower32BitsMask
		// Set the leading bit.
		ret = ret | leadingBitMask
		// Set the other upper 31 bits based on the sqlInstanceID.
		ret = ret | (uint64(sqlInstanceID) << 32)
	}
	return BackendKeyData(ret)
}

// GetSQLInstanceID returns the SQLInstanceID that is encoded in this
// BackendKeyData.
func (b BackendKeyData) GetSQLInstanceID() base.SQLInstanceID {
	bits := uint64(b)
	if bits&leadingBitMask == 0 {
		// Use the upper 12 bits as the sqlInstanceID.
		return base.SQLInstanceID(bits >> 52)
	}

	// Clear the leading bit.
	bits = bits &^ leadingBitMask
	// Use the upper 32 bits as the sqlInstanceID.
	return base.SQLInstanceID(bits >> 32)

}
