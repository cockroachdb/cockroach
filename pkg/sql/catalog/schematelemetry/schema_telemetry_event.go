// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schematelemetry

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	gogotypes "github.com/gogo/protobuf/types"
)

// MaxElementsPerPayload is the upper bound on how many elements can feature in
// the payload of one eventpb.Schema object.
const MaxElementsPerPayload = 50

func buildLogEvents(
	ctx context.Context, cfg *sql.ExecutorConfig, aostDuration time.Duration,
) ([]eventpb.EventPayload, error) {
	asOf := cfg.Clock.Now().Add(aostDuration.Nanoseconds(), 0)
	var ess []scpb.TelemetryPayload_ElementStatus

	// Collect all element-status pairs of interest.
	if err := sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		err := txn.SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		c, err := col.GetAllDescriptors(ctx, txn)
		if err != nil {
			return err
		}
		cc := descmetadata.NewCommentCache(txn, cfg.InternalExecutor)
		visitorFn := func(status scpb.Status, element scpb.Element) {
			// Exclude elements related to system columns.
			a, _ := screl.Schema.GetAttribute(screl.ColumnID, element)
			if colID, ok := a.(descpb.ColumnID); ok && colinfo.IsColIDSystemColumn(colID) {
				return
			}
			// Accumulate elements and non-public statuses into ess.
			var es scpb.TelemetryPayload_ElementStatus
			es.SetValue(element)
			if status != scpb.Status_PUBLIC {
				es.Status = status
			}
			ess = append(ess, es)
		}
		_ = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			_ = scdecomp.WalkDescriptor(ctx, desc, c.LookupDescriptorEntry, visitorFn, cc)
			return nil
		})
		return nil
	}); err != nil {
		return nil, err
	}

	// Bundle the element-status pairs into log events.
	{
		events := make([]eventpb.EventPayload, 0, len(ess)/MaxElementsPerPayload+1)
		var pl scpb.TelemetryPayload
		flush := func() error {
			if len(pl.ElementStatuses) == 0 {
				return nil
			}
			event := &eventpb.Schema{}
			event.Timestamp = asOf.WallTime
			event.CurrentPage = uint32(len(events) + 1)
			any, err := gogotypes.MarshalAny(&pl)
			if err != nil {
				return err
			}
			event.Payload = any
			events = append(events, event)
			pl.ElementStatuses = pl.ElementStatuses[:0]
			return nil
		}

		for _, es := range ess {
			pl.ElementStatuses = append(pl.ElementStatuses, es)
			if len(pl.ElementStatuses) >= MaxElementsPerPayload {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
		if err := flush(); err != nil {
			return nil, err
		}
		for i := range events {
			events[i].(*eventpb.Schema).NumPages = uint32(len(events))
		}
		return events, nil
	}
}
