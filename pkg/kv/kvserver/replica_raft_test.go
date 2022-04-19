// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/tracker"
)

func TestLastUpdateTimesMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	m := make(lastUpdateTimesMap)
	t1 := time.Time{}.Add(time.Second)
	t2 := t1.Add(time.Second)
	m.update(3, t1)
	m.update(1, t2)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{1: t2, 3: t1}, m)
	descs := []roachpb.ReplicaDescriptor{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}, {ReplicaID: 4}}

	t3 := t2.Add(time.Second)
	m.updateOnBecomeLeader(descs, t3)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{1: t3, 2: t3, 3: t3, 4: t3}, m)

	t4 := t3.Add(time.Second)
	descs = append(descs, []roachpb.ReplicaDescriptor{{ReplicaID: 5}, {ReplicaID: 6}}...)
	prs := map[uint64]tracker.Progress{
		1: {State: tracker.StateReplicate}, // should be updated
		// 2 is missing because why not
		3: {State: tracker.StateProbe},     // should be ignored
		4: {State: tracker.StateSnapshot},  // should be ignored
		5: {State: tracker.StateProbe},     // should be ignored
		6: {State: tracker.StateReplicate}, // should be added
		7: {State: tracker.StateReplicate}, // ignored, not in descs
	}
	m.updateOnUnquiesce(descs, prs, t4)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{
		1: t4,
		2: t3,
		3: t3,
		4: t3,
		6: t4,
	}, m)
}

func Test_handleRaftReadyStats_SafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(s int) time.Time {
		return timeutil.Now().Add(time.Duration(s) * time.Second)
	}

	stats := handleRaftReadyStats{
		tBegin:            ts(1),
		tEnd:              ts(6),
		tApplicationBegin: ts(1),
		tApplicationEnd:   ts(2),
		apply: applyCommittedEntriesStats{
			batchesProcessed:      9,
			entriesProcessed:      2,
			entriesProcessedBytes: 3,
			stateAssertions:       4,
			numEmptyEntries:       5,
			numConfChangeEntries:  6,
		},
		tAppendBegin:            ts(2),
		tAppendEnd:              ts(3),
		appendedRegularCount:    7,
		appendedSideloadedCount: 3,
		appendedSideloadedBytes: 5 * (1 << 20),
		appendedRegularBytes:    1024,
		tPebbleCommitBegin:      ts(3),
		pebbleBatchBytes:        1024 * 5,
		tPebbleCommitEnd:        ts(4),
		tSnapBegin:              ts(4),
		tSnapEnd:                ts(5),
		snap: handleSnapshotStats{
			offered: true,
			applied: true,
		},
		sync: true,
	}

	echotest.Require(t, string(redact.Sprint(stats)),
		filepath.Join(testutils.TestDataPath(t, "handle_raft_ready_stats.txt")))
}
