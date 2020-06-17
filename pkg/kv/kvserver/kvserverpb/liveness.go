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
	"github.com/cockroachdb/errors"
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

func (l *Liveness) String() string {
	var extra string
	if l.Draining || l.CommissionStatus.Decommissioning() || l.CommissionStatus.Decommissioned() {
		extra = fmt.Sprintf(" drain:%t comm:%s", l.Draining, l.CommissionStatus.String())
	}
	return fmt.Sprintf("liveness(nid:%d epo:%d exp:%s%s)", l.NodeID, l.Epoch, l.Expiration, extra)
}

// EnsureCompatible is typically called before transmitting/after receiving
// Liveness objects from over the wire. The representation for a given node's
// 'commission status' was changed in v20.2. In v20.1, we used a boolean
// representation to indicated that a node was undergoing a decommissioning
// process. Since it was only a boolean, we couldn't disambiguate between a node
// currently undergoing decommissioning, and a fully decommissioned node. In
// v20.2 we introduced a dedicated enum to be able to disambiguate between the
// two. That being said, v20.2 nodes need to be able to operate in mixed
// clusters with v20.1 nodes, that only know to interpret the boolean
// representation.
//
// EnsureCompatible is able to reconcile across both representations by mutating
// the receiver such that it's understood by both v20.1 and v20.2 nodes (See
// AssertValid for what this entails). If the receiver object is clearly one
// generated from a v20.1 node, we consider the deprecated boolean
// representation as the authoritative one. We consider the enum state
// authoritative if not.
//
// TODO(irfansharif): Remove this once v20.2 is cut.
func (l *Liveness) EnsureCompatible() {
	if l.CommissionStatus.Unknown() {
		// Liveness is from node running v20.1, or is an empty
		// kvserverpb.Liveness, we fill in the commission status.
		l.CommissionStatus = CommissionStatusFromBooleanForm(l.DeprecatedDecommissioning)
	} else {
		// Liveness is from node running v20.2, we backfill in the deprecated
		// boolean state.
		l.DeprecatedDecommissioning = l.CommissionStatus.Decommissioning() ||
			l.CommissionStatus.Decommissioned()
	}
}

// AssertValid checks that the liveness record is internally consistent (i.e.
// it's deprecated v20.1 boolean decommissioning representation is consistent
// with the v20.2 enum representation).
func (l *Liveness) AssertValid() {
	if l.CommissionStatus.Unknown() {
		panic("invalid commission status")
	}

	err := fmt.Sprintf("inconsistent liveness representation: %v", l.String())
	if l.CommissionStatus.Decommissioning() || l.CommissionStatus.Decommissioned() {
		if !l.DeprecatedDecommissioning {
			panic(err)
		}
	} else {
		if l.DeprecatedDecommissioning {
			panic(err)
		}
	}
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
		return CommissionStatus_DECOMMISSIONING
	}
	// We take the optimistic route here and assume the node is fully
	// commissioned (we don't have a way of representing a node in the
	// 'recommissioning' state, see comment on CommissionStatus for why
	// that is).
	return CommissionStatus_COMMISSIONED
}

func (c CommissionStatus) Unknown() bool         { return c == CommissionStatus_UNKNOWN }
func (c CommissionStatus) Decommissioning() bool { return c == CommissionStatus_DECOMMISSIONING }
func (c CommissionStatus) Decommissioned() bool  { return c == CommissionStatus_DECOMMISSIONED }
func (c CommissionStatus) Commissioned() bool    { return c == CommissionStatus_COMMISSIONED }
func (c CommissionStatus) String() string {
	switch c {
	case CommissionStatus_UNKNOWN:
		return "unknown"
	case CommissionStatus_COMMISSIONED:
		return "commissioned"
	case CommissionStatus_DECOMMISSIONING:
		return "decommissioning"
	case CommissionStatus_DECOMMISSIONED:
		return "decommissioned"
	default:
		err := "unknown commission status, expected one of [unknown,commissioned,decommissioning,decommissioned]"
		panic(err)
	}
}

// ErrIllegalCommissionStatusTransition is the sentinel error for illegal
// commission status transition attempts.
var ErrIllegalCommissionStatusTransition = errors.New("illegal commission status transition")

// ValidateTransition validates transitions of the liveness record, returning an
// error if the proposed transition is invalid. Ignoring no-ops, the valid state
// transitions for CommissionStatus are as follows:
// 	  Decommissioning 	=> Commissioned
// 	  Commissioned 		=> Decommissioning
// 	  Decommissioning 	=> Decommissioned
//
// See diagram above the CommissionStatus type for more details.
func ValidateTransition(old, new Liveness) error {
	if new.CommissionStatus.Unknown() {
		panic("illegal usage: liveness commission status is unknown")
	}

	if old == (Liveness{}) {
		// No previous liveness present, all states are considered valid.
		return nil
	}

	if old.CommissionStatus == new.CommissionStatus {
		// No-op.
		return nil
	}

	if new.CommissionStatus.Commissioned() && !old.CommissionStatus.Decommissioning() {
		err := errors.Newf("can only recommission a decommissioning node; n%d found to be %s",
			new.NodeID, old.CommissionStatus.String())
		return errors.Wrapf(err, ErrIllegalCommissionStatusTransition.Error())
	}
	if new.CommissionStatus.Decommissioning() && !old.CommissionStatus.Commissioned() {
		// NB: This code-path is actually inaccessible, given the no-op
		// conditions above. We keep it for clarity.
		err := errors.Newf("can only decommission a commissioned node, found %s",
			old.CommissionStatus.String())
		return errors.Wrapf(err, ErrIllegalCommissionStatusTransition.Error())
	}
	if new.CommissionStatus.Decommissioned() && !old.CommissionStatus.Decommissioning() {
		err := errors.Newf("can only fully decommission a decommissioning node, found %s",
			old.CommissionStatus.String())
		return errors.Wrapf(err, ErrIllegalCommissionStatusTransition.Error())
	}

	return nil
}
