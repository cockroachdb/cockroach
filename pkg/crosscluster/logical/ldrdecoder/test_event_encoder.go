// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// EventBuilder helps construct StreamEvent_KV events for testing.
type EventBuilder struct {
	t         *testing.T
	tableDesc catalog.TableDescriptor
	colMap    catalog.TableColMap
	codec     keys.SQLCodec
}

// newEventBuilder creates a new EventBuilder for the given table descriptor.
func NewTestEventBuilder(t *testing.T, desc *descpb.TableDescriptor) *EventBuilder {
	tableDesc := tabledesc.NewBuilder(desc).BuildImmutableTable()
	var colMap catalog.TableColMap
	for i, col := range tableDesc.PublicColumns() {
		colMap.Set(col.GetID(), i)
	}
	return &EventBuilder{
		t:         t,
		tableDesc: tableDesc,
		colMap:    colMap,
		codec:     keys.SystemSQLCodec,
	}
}

func (b *EventBuilder) encodeRow(timestamp hlc.Timestamp, row tree.Datums) roachpb.KeyValue {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		false,
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)
	kv := roachpb.KeyValue{
		Key:   indexEntries[0].Key,
		Value: indexEntries[0].Value,
	}
	kv.Value.Timestamp = timestamp
	kv.Value.InitChecksum(kv.Key)
	return kv
}

// insertEvent creates an insert event for the given row at the specified timestamp.
func (b *EventBuilder) InsertEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	event := streampb.StreamEvent_KV{
		KeyValue: b.encodeRow(time, row),
	}
	return event
}

// updateEvent creates an update event for the given row and previous values at the specified timestamp.
func (b *EventBuilder) UpdateEvent(
	time hlc.Timestamp, row tree.Datums, prevValue tree.Datums,
) streampb.StreamEvent_KV {
	kv := b.encodeRow(time, row)
	kvPrev := b.encodeRow(time, prevValue)
	return streampb.StreamEvent_KV{
		KeyValue:  kv,
		PrevValue: kvPrev.Value,
	}
}

// deleteEvent creates a delete event for the given row at the specified timestamp.
func (b *EventBuilder) DeleteEvent(
	time hlc.Timestamp, prevValue tree.Datums,
) streampb.StreamEvent_KV {
	kv := b.encodeRow(time, prevValue)
	return streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   kv.Key,
			Value: roachpb.Value{Timestamp: time},
		},
		PrevValue: kv.Value,
	}
}

// TombstoneEvent creates a tombstone delete event (a delete with no previous
// value) for the given row at the specified timestamp.
func (b *EventBuilder) TombstoneEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	kv := b.encodeRow(time, row)
	return streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   kv.Key,
			Value: roachpb.Value{Timestamp: time},
		},
	}
}
