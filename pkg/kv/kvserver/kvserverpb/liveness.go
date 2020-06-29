// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// IsLive returns whether the node is considered live at the given time.
//
// NOTE: If one is interested whether the Liveness is valid currently, then the
// timestamp passed in should be the known high-water mark of all the clocks of
// the nodes in the cluster. For example, if the liveness expires at ts 100, our
// physical clock is at 90, but we know that another node's clock is at 110,
// then it's preferable (more consistent across nodes) for the liveness to be
// considered expired. For that purpose, it's better to pass in
// clock.Now().GoTime() rather than clock.PhysicalNow() - the former takes into
// consideration clock signals from other nodes, the latter doesn't.
func (l *Liveness) IsLive(now time.Time) bool {
	expiration := timeutil.Unix(0, l.Expiration.WallTime)
	return now.Before(expiration)
}

// IsDead returns true if the liveness expired more than threshold ago.
//
// Note that, because of threshold, IsDead() is not the inverse of IsLive().
func (l *Liveness) IsDead(now time.Time, threshold time.Duration) bool {
	expiration := timeutil.Unix(0, l.Expiration.WallTime)
	deadAsOf := expiration.Add(threshold)
	return !now.Before(deadAsOf)
}

func (l Liveness) String() string {
	var extra string
	if l.Draining || l.Decommissioning {
		extra = fmt.Sprintf(" drain:%t decom:%t", l.Draining, l.Decommissioning)
	}
	return fmt.Sprintf("liveness(nid:%d epo:%d exp:%s%s)", l.NodeID, l.Epoch, l.Expiration, extra)
}
