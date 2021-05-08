// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

// MakeDataKey creates a time series data key for the given series name, source,
// Resolution and timestamp. The timestamp is expressed in nanoseconds since the
// epoch; it will be truncated to an exact multiple of the supplied
// Resolution's KeyDuration.
func MakeDataKey(name string, source string, r Resolution, timestamp int64) roachpb.Key {
	k := makeDataKeySeriesPrefix(name, r)

	// Normalize timestamp into a timeslot before recording.
	timeslot := timestamp / r.SlabDuration()
	k = encoding.EncodeVarintAscending(k, timeslot)
	k = append(k, source...)
	return k
}

// makeDataKeySeriesPrefix creates a key prefix for a time series at a specific
// resolution.
func makeDataKeySeriesPrefix(name string, r Resolution) roachpb.Key {
	k := append(roachpb.Key(nil), keys.TimeseriesPrefix...)
	k = encoding.EncodeBytesAscending(k, []byte(name))
	k = encoding.EncodeVarintAscending(k, int64(r))
	return k
}

// DecodeDataKey decodes a time series key into its components:
// name, source, resolution, timestamp.
func DecodeDataKey(key roachpb.Key) (string, string, Resolution, int64, error) {
	// Detect and remove prefix.
	remainder := key
	if !bytes.HasPrefix(key, keys.TimeseriesPrefix) {
		return "", "", 0, 0, errors.Errorf("malformed time series data key %v: improper prefix", key)
	}
	remainder = remainder[len(keys.TimeseriesPrefix):]

	return decodeDataKeySuffix(remainder)
}

// decodeDataKeySuffix decodes a time series key into its components.
func decodeDataKeySuffix(key roachpb.Key) (string, string, Resolution, int64, error) {
	// Decode series name.
	remainder, name, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", "", 0, 0, err
	}
	// Decode resolution.
	remainder, resolutionInt, err := encoding.DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, 0, err
	}
	resolution := Resolution(resolutionInt)
	// Decode timestamp.
	remainder, timeslot, err := encoding.DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, 0, err
	}
	timestamp := timeslot * resolution.SlabDuration()
	// The remaining bytes are the source.
	source := remainder

	return string(name), string(source), resolution, timestamp, nil
}

func prettyPrintKey(key roachpb.Key) string {
	name, source, resolution, timestamp, err := decodeDataKeySuffix(key)
	if err != nil {
		// Not a valid timeseries key, fall back to doing the best we can to display
		// it.
		return encoding.PrettyPrintValue(nil /* dirs */, key, "/")
	}
	return fmt.Sprintf("/%s/%s/%s/%s", name, source, resolution,
		timeutil.Unix(0, timestamp).Format(time.RFC3339Nano))
}

func init() {
	keys.PrettyPrintTimeseriesKey = prettyPrintKey
}
