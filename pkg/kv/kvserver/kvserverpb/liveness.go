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

func (l *Liveness) DecommissioningOrDecommissioned() bool {
	return l.CommissionStatus.DecommissioningOrDecommissioned()
}

func (l Liveness) String() string {
	var extra string
	if l.Draining || l.DecommissioningOrDecommissioned() {
		extra = fmt.Sprintf(" drain:%t comm:%s dep:%t", l.Draining, l.CommissionStatus.String(), l.DeprecatedDecommissioning) // XXX: Remove this last one.
	}
	return fmt.Sprintf("liveness(nid:%d epo:%d exp:%s%s)", l.NodeID, l.Epoch, l.Expiration, extra)
}

// CommissionStatusFromBooleanForm converts the deprecated boolean
// decommissioning state used in the v20.1 liveness proto definition to the new
// CommissionStatus enum.
//
// TODO(irfansharif): Remove this once v20.2 is cut, as we no longer need to be
// compatible with the deprecated boolean decommissioning representation used by
// v20.1 nodes.
func CommissionStatusFromBooleanForm(decommissioning bool) CommissionStatus {
	// Liveness is from node running v20.1, we fill in the appropriate
	// commission state.
	if decommissioning {
		// We take the conservative opinion and assume the node to be
		// decommissioning, not fully decommissioned (after all, that's all
		// one can infer from a boolean decommissioning state). If operators
		// decommissioned nodes in a cluster running v20.1 and v20.2 nodes,
		// they may have to decommission the nodes again once fully onto
		// v20.2 in order to durably mark said nodes as decommissioned.
		return CommissionStatus_DECOMMISSIONING_
	}
	// We take the optimistic route here and assume the node is fully
	// commissioned (we don't have a way of representing a node in the
	// 'recommissioning' state, see comment on CommissionStatus for why
	// that is).
	return CommissionStatus_COMMISSIONED_
}

// DecommissioningOrDecommissioned converts the CommissionStatus to the
// deprecated boolean representation used in the v20.1 liveness proto
// definition.
//
// TODO(irfansharif): Remove this once v20.2 is cut, as we no longer need to be
// compatible with the deprecated boolean decommissioning representation used by
// v20.1 nodes.
func (c CommissionStatus) DecommissioningOrDecommissioned() bool {
	return c == CommissionStatus_DECOMMISSIONING_ || c == CommissionStatus_DECOMMISSIONED_
}

// XXX: Export nicer looking symbols for CommissionStatus_XYZ_

// Unknown is the placeholder default value we use for wire compatibility in
// mixed version clusters, where earlier version nodes may not make use of this
// commission status enum.
func (c CommissionStatus) Unknown() bool {
	return c == CommissionStatus_UNKNOWN_
}

func (c CommissionStatus) String() string {
	switch c {
	case CommissionStatus_UNKNOWN_:
		return "unknown"
	case CommissionStatus_COMMISSIONED_:
		return "commissioned"
	case CommissionStatus_DECOMMISSIONING_:
		return "decommissioning"
	case CommissionStatus_DECOMMISSIONED_:
		return "decommissioned"
	default:
		err := "unknown commission status, expected one of [unknown,commissioned,decommissioning,decommissioned]"
		panic(err)
	}
}
