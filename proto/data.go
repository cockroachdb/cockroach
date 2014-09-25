// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"crypto/md5"
	"math"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{WallTime: 0, Logical: 0}
)

// Less implements the util.Ordered interface, allowing
// the comparison of timestamps.
func (t Timestamp) Less(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical < s.Logical)
}

// Equal returns whether two timestamps are the same.
func (t Timestamp) Equal(s Timestamp) bool {
	return t.WallTime == s.WallTime && t.Logical == s.Logical
}

// InitChecksum initializes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly; if the value contains an integer,
// the checksum includes the integer as 8 bytes in big-endian order.
func (v *Value) InitChecksum(key []byte) {
	if v.Checksum == nil {
		v.Checksum = gogoproto.Uint32(v.computeChecksum(key))
	}
}

// Verify verifies the value's Checksum matches a newly-computed
// checksum of the value's contents. If the value's Checksum is not
// set the verification is a noop. It also ensures that both Bytes
// and Integer are not both set.
func (v *Value) Verify(key []byte) error {
	if v.Checksum != nil {
		if v.GetChecksum() != v.computeChecksum(key) {
			return util.Errorf("invalid checksum for key %q, value %+v", key, v)
		}
	}
	if v.Bytes != nil && v.Integer != nil {
		return util.Errorf("both the value byte slice and integer fields are set for key %q: %+v", key, v)
	}
	return nil
}

// computeChecksum computes a checksum based on the provided key and
// the contents of the value. If the value contains a byte slice, the
// checksum includes it directly; if the value contains an integer,
// the checksum includes the integer as 8 bytes in big-endian order.
func (v *Value) computeChecksum(key []byte) uint32 {
	c := encoding.NewCRC32Checksum(key)
	if v.Bytes != nil {
		c.Write(v.Bytes)
	} else if v.Integer != nil {
		c.Write(encoding.EncodeUint64(nil, uint64(v.GetInteger())))
	}
	return c.Sum32()
}

// MakePriority generates a random priority value, biased by the
// specified userPriority. If userPriority=100, the resulting
// priority is 100x more likely to be probabilistically greater
// than a similar invocation with userPriority=1
func MakePriority(userPriority int32) int32 {
	if userPriority < 1 {
		userPriority = 1
	}
	// The idea here is to bias selection of a random priority from the
	// range [1, 2^31-1) such that if userPriority=100, it's 100x more
	// likely to be a higher int32 than if userPriority=1. The formula
	// below chooses random values according to the following table:
	//   userPriority  |  range
	//   1             |  all positive int32s
	//   10            |  top 9/10ths of positive int32s
	//   100           |  top 99/100ths of positive int32s
	//   1000          |  top 999/1000ths of positive int32s
	//   ...etc
	return math.MaxInt32 - util.CachedRand.Int31n(math.MaxInt32/userPriority)
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority.
func (t *Transaction) UpgradePriority(minPriority int32) {
	if minPriority > t.Priority {
		t.Priority = minPriority
	}
}

// MD5 returns the MD5 digest of the transaction ID as a string.
// This method returns an empty string if the transaction is nil.
func (t *Transaction) MD5() string {
	if t == nil {
		return ""
	}
	txnMD5 := md5.Sum(t.ID)
	return string(txnMD5[:md5.Size])
}
