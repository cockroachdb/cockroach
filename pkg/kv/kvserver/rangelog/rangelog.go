// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rangelog implements kvserver.RangeLogWriter
package rangelog

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Writer implements kvserver.RangeLogWriter using the Executor.
type Writer struct {
	generateUniqueID IDGen
	w                bootstrap.KVWriter
}

// IDGen is used to generate a unique ID for new rows.
type IDGen = func() int64

// NewWriter returns a new Writer which implements kvserver.RangeLogWriter
// using just kv APIs. The IDGen function must return unique identifiers
// every time it is called.
func NewWriter(codec keys.SQLCodec, generateUniqueID IDGen) *Writer {
	return newWriter(codec, generateUniqueID, systemschema.RangeEventTable)
}

func newWriter(codec keys.SQLCodec, id IDGen, table catalog.TableDescriptor) *Writer {
	return &Writer{
		generateUniqueID: id,
		w:                bootstrap.MakeKVWriter(codec, table),
	}
}

// WriteRangeLogEvent implements kvserver.RangeLogWriter. It writes the event
// to the system.rangelog table in the provided transaction.
func (s *Writer) WriteRangeLogEvent(
	ctx context.Context, runner kvserver.DBOrTxn, event kvserverpb.RangeLogEvent,
) error {
	ts, err := tree.MakeDTimestampTZ(event.Timestamp, time.Microsecond)
	if err != nil {
		return errors.AssertionFailedf("failed to generate event timestamp"+
			"from go time: %v", ts)
	}
	args := [...]tree.Datum{
		ts,
		tree.NewDInt(tree.DInt(event.RangeID)),
		tree.NewDInt(tree.DInt(event.StoreID)),
		tree.NewDString(event.EventType.String()),
		tree.DNull,
		tree.DNull,
		tree.NewDInt(tree.DInt(s.generateUniqueID())),
	}
	if event.OtherRangeID != 0 {
		args[4] = tree.NewDInt(tree.DInt(event.OtherRangeID))
	}
	if event.Info != nil {
		infoBytes, err := json.Marshal(*event.Info)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(
				err, "failed to encode rangelog event info",
			)
		}
		args[5] = tree.NewDString(string(infoBytes))
	}
	ba := runner.NewBatch()
	if err := s.w.Insert(ctx, ba, false /* kvTrace */, args[:]...); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to encode rangelog index entries",
		)
	}
	// TODO(mira #104075): Logging to system.rangelog is async by default, which
	// means a range change might succeed but the logging might fail. We should
	// log a *structured* event to KV_DISTRIBUTION here as an auxiliary logging
	// mechanism. Currently, we can't use kvserver.RangeLogEvent directly as a
	// structured event because that will introduce an unnecessary dependency from
	// eventpb to kvserver and roachpb. We should either refactor the
	// RangeLogEvent struct, or create a new struct for structured logging.
	return runner.Run(ctx, ba)
}

var _ kvserver.RangeLogWriter = &Writer{}
