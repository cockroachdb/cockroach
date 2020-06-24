// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// NodeIDContainer is used to share a single roachpb.NodeID instance between
// multiple layers. It allows setting and getting the value. Once a value is
// set, the value cannot change.
type NodeIDContainer struct {
	_ util.NoCopy

	// nodeID is atomically updated under the mutex; it can be read atomically
	// without the mutex.
	nodeID int32
}

// String returns the node ID, or "?" if it is unset.
func (n *NodeIDContainer) String() string {
	return redact.StringWithoutMarkers(n)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (n *NodeIDContainer) SafeFormat(w redact.SafePrinter, _ rune) {
	val := n.Get()
	if val == 0 {
		w.SafeRune('?')
	} else {
		w.Print(val)
	}
}

// Get returns the current node ID; 0 if it is unset.
func (n *NodeIDContainer) Get() roachpb.NodeID {
	return roachpb.NodeID(atomic.LoadInt32(&n.nodeID))
}

// Set sets the current node ID. If it is already set, the value must match.
func (n *NodeIDContainer) Set(ctx context.Context, val roachpb.NodeID) {
	if val <= 0 {
		log.Fatalf(ctx, "trying to set invalid NodeID: %d", val)
	}
	oldVal := atomic.SwapInt32(&n.nodeID, int32(val))
	if oldVal == 0 {
		if log.V(2) {
			log.Infof(ctx, "NodeID set to %d", val)
		}
	} else if oldVal != int32(val) {
		log.Fatalf(ctx, "different NodeIDs set: %d, then %d", oldVal, val)
	}
}

// Reset changes the NodeID regardless of the old value.
//
// Should only be used in testing code.
func (n *NodeIDContainer) Reset(val roachpb.NodeID) {
	atomic.StoreInt32(&n.nodeID, int32(val))
}

// A SQLInstanceID is an ephemeral ID assigned to a running instance of the SQL
// server. This is distinct from a NodeID, which is a long-lived identifier
// assigned to a node in the KV layer which is unique across all KV nodes in the
// cluster and persists across restarts. Instead, a SQLInstance is similar to a
// process ID from the unix world: an integer assigned to the SQL server
// on process start which is unique across all SQL server processes running
// on behalf of the tenant, while the SQL server is running.
//
// NB: until https://github.com/cockroachdb/cockroach/issues/47899 is addressed,
// the properties of the SQLInstanceID hold trivially due to the constraint that
// only one SQL server must be running on behalf of the tenant at any given
// time. After that, it's likely that we'll allocate these IDs off a counter,
// so they will be completely unique (per tenant).
type SQLInstanceID int32

func (s SQLInstanceID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

// SQLIDContainer wraps a SQLInstanceID and optionally a NodeID.
type SQLIDContainer struct {
	w             errorutil.TenantSQLDeprecatedWrapper // NodeID
	sqlInstanceID SQLInstanceID
}

// NewSQLIDContainer sets up an SQLIDContainer wrapping the (positive) SQLInstanceID
// and a NodeID. See errorutil.TenantSQLDeprecatedWrapper for an explanation of
// the nodeIDExposed parameter.
//
// As a special case, a zero sqlInstanceID in conjunction with
// nodeIDExposed==true falls back to the NodeID in SQLInstanceID(). This is used
// in single-tenant deployments.
func NewSQLIDContainer(
	sqlInstanceID SQLInstanceID, nodeID *NodeIDContainer, nodeIDExposed bool,
) *SQLIDContainer {
	if !nodeIDExposed && sqlInstanceID == 0 {
		panic("sqlInstanceID must not be zero")
	}
	return &SQLIDContainer{
		w:             errorutil.MakeTenantSQLDeprecatedWrapper(nodeID, nodeIDExposed),
		sqlInstanceID: sqlInstanceID,
	}
}

// OptionalNodeID returns the NodeID and true, if the former is exposed.
// Otherwise, returns zero and false.
func (c *SQLIDContainer) OptionalNodeID() (roachpb.NodeID, bool) {
	v, ok := c.w.Optional()
	if !ok {
		return 0, false
	}
	return v.(*NodeIDContainer).Get(), true
}

// OptionalNodeIDErr is like OptionalNodeID, but returns an error (referring to
// the optionally supplied Github issues) if the ID is not present.
func (c *SQLIDContainer) OptionalNodeIDErr(issueNos ...int) (roachpb.NodeID, error) {
	v, err := c.w.OptionalErr(issueNos...)
	if err != nil {
		return 0, err
	}
	return v.(*NodeIDContainer).Get(), nil
}

// DeprecatedNodeID returns the NodeID. This call is deprecated: removal of all
// call sites is the goal, at which point this method will be removed. Calls to
// this method reflect essential functionality which needs to be reworked in
// order to enable multi-tenancy.
func (c *SQLIDContainer) DeprecatedNodeID(issueNo int) roachpb.NodeID {
	return c.w.Deprecated(issueNo).(*NodeIDContainer).Get()
}

// SQLInstanceID returns the wrapped SQLInstanceID.
func (c *SQLIDContainer) SQLInstanceID() SQLInstanceID {
	if n, ok := c.OptionalNodeID(); ok {
		return SQLInstanceID(n)
	}
	return c.sqlInstanceID
}

// Get is a temporary method to aid refactoring.
//
// TODO(tbg): remove.
func (c *SQLIDContainer) Get() roachpb.NodeID {
	// Silence staticcheck.
	var _ = (*SQLIDContainer)(nil).OptionalNodeID
	var _ = (*SQLIDContainer)(nil).OptionalNodeIDErr
	var _ = (*SQLIDContainer)(nil).SQLInstanceID
	return c.DeprecatedNodeID(-12131415)

}

// TestingIDContainer is an SQLIDContainer with hard-coded SQLInstanceID of 10 and
// NodeID of 1.
var TestingIDContainer = func() *SQLIDContainer {
	var c NodeIDContainer
	c.Set(context.Background(), 1)
	return NewSQLIDContainer(10, &c, true /* exposed */)
}()
