// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// protectedTimestampDecoder decodes rows from the system.protected_ts_records
// table.
type protectedTimestampDecoder struct {
	alloc   tree.DatumAlloc
	decoder valueside.Decoder
}

// newProtectedTimestampDecoder instantiates a protectedTimestampDecoder.
func newProtectedTimestampDecoder() *protectedTimestampDecoder {
	columns := systemschema.ProtectedTimestampsRecordsTable.PublicColumns()
	return &protectedTimestampDecoder{
		decoder: valueside.MakeDecoder(columns),
	}
}

// DecodeRow decodes a row of the system.protected_ts_records table.
func (d *protectedTimestampDecoder) decode(kv roachpb.KeyValue) (target ptpb.Target, _ error) {
	if !kv.Value.IsPresent() {
		return ptpb.Target{},
			errors.AssertionFailedf("missing value for key in system.protected_ts_records: %v", kv)
	}

	// The columns after the `id` field are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return ptpb.Target{}, err
	}

	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return ptpb.Target{}, err
	}
	if t := datums[7]; t != tree.DNull {
		targetBytes := tree.MustBeDBytes(t)
		if err := protoutil.Unmarshal([]byte(targetBytes), &target); err != nil {
			return ptpb.Target{}, errors.Wrap(err, "failed to unmarshal target")
		}
	}

	return target, nil
}

// TestingProtectedTimestampDecoderFn constructs a protectedTimestampDecoder and
// exposes its decode method.
func TestingProtectedTimestampDecoderFn() func(roachpb.KeyValue) (ptpb.Target, error) {
	return newProtectedTimestampDecoder().decode
}
