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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package ts

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Time series keys are carefully constructed to usefully sort the data in the
// key-value store for the purpose of queries. Time series data is queryied by
// providing a series name and a timespan; we therefore expose the series name
// and the collection time prominently on the key.
//
// The precise formula for a time series key is:
//
//   [system key prefix]tsd[series name][resolution][time slot][source key]
//
// The series name is an arbitrary string identifying the series, although the
// ts system may enforce naming rules at a higher level. This string is binary
// encoded in the key.
//
// The source key is a possibly empty string which identifies the source from
// which the series data was gathered. Data for a series may be gathered from
// multiple sources, which are stored separately but are sorted adjacently for
// efficient access.
//
// The resolution refers to the sample duration at which data is stored.
// Cockroach supports a fixed set of named resolutions, which are stored in the
// Resolution enumeration. This value is encoded as a VarInt in the key.
//
// Cockroach divides all data for a series into contiguous "time slots" of
// uniform length based on the "key duration" of the Resolution. For each
// series/source pair, there will be one key per slot. Slot 0 begins at unix
// epoch; the slot for a specific timestamp is found by truncating the
// timestamp to an exact multiple of the key duration, and then dividing it by
// the key duration:
//
// 		slot := (timestamp / keyDuration) // integer division
var (
	// keyDataPrefix is the key prefix for time series data keys.
	keyDataPrefix = proto.MakeKey(engine.KeySystemPrefix, proto.Key("tsd"))
)

// MakeDataKey creates a time series data key for the given series name, source,
// Resolution and timestamp. The timestamp is expressed in nanoseconds since the
// epoch; it will be truncated to an exact multiple of the supplied
// Resolution's KeyDuration.
func MakeDataKey(name string, source string, r Resolution, timestamp int64) proto.Key {
	// Normalize timestamp into a timeslot before recording.
	timeslot := timestamp / r.KeyDuration()

	k := append(proto.Key(nil), keyDataPrefix...)
	k = encoding.EncodeBytes(k, []byte(name))
	k = encoding.EncodeVarint(k, int64(r))
	k = encoding.EncodeVarint(k, timeslot)
	k = append(k, source...)
	return k
}

// DecodeDataKey decodes a time series key into its components.
func DecodeDataKey(key proto.Key) (string, string, Resolution, int64) {
	var (
		name          []byte
		source        []byte
		resolutionInt int64
		timeslot      int64
		remainder     = key
	)

	// Detect and remove prefix.
	if !bytes.HasPrefix(remainder, keyDataPrefix) {
		panic(fmt.Sprintf("malformed time series data key %v: improper prefix", key))
	}
	remainder = remainder[len(keyDataPrefix):]

	// Decode series name.
	remainder, name = encoding.DecodeBytes(remainder, nil)
	// Decode resolution.
	remainder, resolutionInt = encoding.DecodeVarint(remainder)
	resolution := Resolution(resolutionInt)
	// Decode timestamp.
	remainder, timeslot = encoding.DecodeVarint(remainder)
	timestamp := timeslot * resolution.KeyDuration()
	// The remaining bytes are the source.
	source = remainder

	return string(name), string(source), resolution, timestamp
}
