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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
