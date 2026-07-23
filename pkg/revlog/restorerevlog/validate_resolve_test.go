// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package restorerevlog

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestValidateResolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("no ticks at all", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		err := ValidateResolved(ctx, es,
			testTickEnd(0),  // backupEndTime
			testTickEnd(30), // revlogTimestamp (AOST)
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no resolved ticks")
	})

	t.Run("ticks do not reach AOST", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(0), TickEnd: testTickEnd(10),
		}))
		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(10), TickEnd: testTickEnd(20),
		}))

		err := ValidateResolved(ctx, es,
			testTickEnd(0),  // backupEndTime
			testTickEnd(30), // AOST past last tick
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has not resolved")
	})

	t.Run("ticks reach AOST exactly", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(0), TickEnd: testTickEnd(10),
		}))
		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(10), TickEnd: testTickEnd(20),
		}))

		err := ValidateResolved(ctx, es,
			testTickEnd(0),  // backupEndTime
			testTickEnd(20), // AOST = last tick end
		)
		require.NoError(t, err)
	})

	t.Run("ticks past AOST", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(0), TickEnd: testTickEnd(10),
		}))
		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(10), TickEnd: testTickEnd(20),
		}))

		err := ValidateResolved(ctx, es,
			testTickEnd(0),  // backupEndTime
			testTickEnd(15), // AOST between ticks
		)
		require.NoError(t, err)
	})

	t.Run("single tick covers AOST", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: testTickEnd(0), TickEnd: testTickEnd(10),
		}))

		err := ValidateResolved(ctx, es,
			testTickEnd(0), // backupEndTime
			testTickEnd(5), // AOST within single tick
		)
		require.NoError(t, err)
	})
}

// makeDesc constructs a catalog.Descriptor from a table proto,
// using the same path ApplyDescriptorChanges uses internally.
func makeDesc(id descpb.ID, name string) catalog.Descriptor {
	return backupinfo.NewDescriptorForManifest(
		&descpb.Descriptor{
			Union: &descpb.Descriptor_Table{
				Table: &descpb.TableDescriptor{ID: id, Name: name},
			},
		},
	)
}

func makeDescProto(id descpb.ID, name string) *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Table{
			Table: &descpb.TableDescriptor{ID: id, Name: name},
		},
	}
}

func makeDroppedDescProto(id descpb.ID, name string) *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Table{
			Table: &descpb.TableDescriptor{
				ID: id, Name: name,
				State: descpb.DescriptorState_DROP,
			},
		},
	}
}

func TestApplyDescriptorChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	backupEnd := hlc.Timestamp{WallTime: 100 * int64(time.Second)}
	revlogTS := hlc.Timestamp{WallTime: 200 * int64(time.Second)}

	t.Run("no schema changes", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		backupDescs := []catalog.Descriptor{makeDesc(1, "t1")}
		result, newIDs, err := ApplyDescriptorChanges(
			ctx, es, backupDescs, backupEnd, revlogTS,
		)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Empty(t, newIDs)
	})

	t.Run("new table added", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		backupDescs := []catalog.Descriptor{makeDesc(1, "t1")}

		changeTS := hlc.Timestamp{WallTime: 150 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, changeTS, 2, makeDescProto(2, "t2"),
		))

		result, newIDs, err := ApplyDescriptorChanges(
			ctx, es, backupDescs, backupEnd, revlogTS,
		)
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, newIDs, descpb.ID(2))
	})

	t.Run("tombstone removes descriptor", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		backupDescs := []catalog.Descriptor{
			makeDesc(1, "t1"), makeDesc(2, "t2"),
		}

		changeTS := hlc.Timestamp{WallTime: 150 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, changeTS, 2, nil, /* tombstone */
		))

		result, newIDs, err := ApplyDescriptorChanges(
			ctx, es, backupDescs, backupEnd, revlogTS,
		)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, descpb.ID(1), result[0].GetID())
		require.Empty(t, newIDs)
	})

	t.Run("DROP state filters table", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		backupDescs := []catalog.Descriptor{
			makeDesc(1, "t1"), makeDesc(2, "t2"),
		}

		changeTS := hlc.Timestamp{WallTime: 150 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, changeTS, 2, makeDroppedDescProto(2, "t2"),
		))

		result, _, err := ApplyDescriptorChanges(
			ctx, es, backupDescs, backupEnd, revlogTS,
		)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, descpb.ID(1), result[0].GetID())
	})

	t.Run("multiple changes keeps latest", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		backupDescs := []catalog.Descriptor{makeDesc(1, "t1")}

		change1 := hlc.Timestamp{WallTime: 120 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, change1, 1, makeDescProto(1, "t1_renamed"),
		))
		change2 := hlc.Timestamp{WallTime: 180 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, change2, 1, makeDescProto(1, "t1_final"),
		))

		result, _, err := ApplyDescriptorChanges(
			ctx, es, backupDescs, backupEnd, revlogTS,
		)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, "t1_final", result[0].GetName())
	})

	t.Run("changes outside window ignored", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		backupDescs := []catalog.Descriptor{makeDesc(1, "t1")}

		beforeBackup := hlc.Timestamp{WallTime: 50 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, beforeBackup, 1, makeDescProto(1, "too_early"),
		))
		afterRevlog := hlc.Timestamp{WallTime: 300 * int64(time.Second)}
		require.NoError(t, revlog.WriteSchemaDesc(
			ctx, es, afterRevlog, 1, makeDescProto(1, "too_late"),
		))

		result, _, err := ApplyDescriptorChanges(
			ctx, es, backupDescs, backupEnd, revlogTS,
		)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, "t1", result[0].GetName())
	})
}

func TestMergeTickEventsEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("single event", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		m := writeTestTick(t, ctx, es, testTickEnd(10), 1, []revlog.Event{
			testEvent("only", 500, 0, "val"),
		})
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m},
		})
		require.Len(t, merged, 1)
		require.Equal(t, "only", string(merged[0].Key.Key))
	})

	t.Run("all events filtered by AOST", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		m := writeTestTick(t, ctx, es, testTickEnd(10), 1, []revlog.Event{
			testEvent("a", 1000, 0, "v1"),
			testEvent("b", 2000, 0, "v2"),
		})
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks:            []revlogpb.Manifest{m},
			RestoreTimestamp: hlc.Timestamp{WallTime: 500},
		})
		require.Empty(t, merged)
	})

	t.Run("same key same ts across ticks deduplicates", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		m1 := writeTestTick(t, ctx, es, testTickEnd(10), 1, []revlog.Event{
			testEvent("a", 1000, 0, "v1"),
		})
		m2 := writeTestTick(t, ctx, es, testTickEnd(20), 2, []revlog.Event{
			testEvent("a", 1000, 0, "v1"),
		})
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m1, m2},
		})
		require.Len(t, merged, 1)
	})

	t.Run("many ticks merged correctly", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		var manifests []revlogpb.Manifest
		for i := 0; i < 10; i++ {
			m := writeTestTick(t, ctx, es, testTickEnd(10*(i+1)), int64(i+1), []revlog.Event{
				testEvent("k", int64(i*100+50), 0, "v"),
			})
			manifests = append(manifests, m)
		}
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks: manifests,
		})
		require.Len(t, merged, 1)
		require.Equal(t, int64(950), merged[0].Key.Timestamp.WallTime)
	})

	t.Run("logical timestamp distinguishes revisions", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		m := writeTestTick(t, ctx, es, testTickEnd(10), 1, []revlog.Event{
			testEvent("k", 1000, 0, "v0"),
			testEvent("k", 1000, 1, "v1"),
			testEvent("k", 1000, 2, "v2"),
		})
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m},
		})
		require.Len(t, merged, 1)
		require.Equal(t, int32(2), merged[0].Key.Timestamp.Logical)
		require.Equal(t, []byte("v2"), merged[0].Value)
	})

	t.Run("AOST with logical timestamp", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		m := writeTestTick(t, ctx, es, testTickEnd(10), 1, []revlog.Event{
			testEvent("k", 1000, 0, "v0"),
			testEvent("k", 1000, 1, "v1"),
			testEvent("k", 1000, 2, "v2"),
		})
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks:            []revlogpb.Manifest{m},
			RestoreTimestamp: hlc.Timestamp{WallTime: 1000, Logical: 1},
		})
		require.Len(t, merged, 1)
		require.Equal(t, int32(1), merged[0].Key.Timestamp.Logical)
		require.Equal(t, []byte("v1"), merged[0].Value)
	})

	t.Run("tombstone overwrites prior value", func(t *testing.T) {
		es := newTestStorage(t)
		defer es.Close()

		m1 := writeTestTick(t, ctx, es, testTickEnd(10), 1, []revlog.Event{
			testEvent("k", 1000, 0, "alive"),
		})
		m2 := writeTestTick(t, ctx, es, testTickEnd(20), 2, []revlog.Event{
			testEvent("k", 2000, 0, ""),
		})
		merged := collectMergedEntries(t, ctx, es, execinfrapb.RevlogLocalMergeSpec{
			Ticks: []revlogpb.Manifest{m1, m2},
		})
		require.Len(t, merged, 1)
		require.Empty(t, merged[0].Value)
	})
}
