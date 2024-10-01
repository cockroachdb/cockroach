// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// NodeIDContainer is used to share a single roachpb.NodeID or
// SQLInstanceID instance between multiple layers. It allows setting
// and getting the value. Once a value is set, the value cannot
// change.
// Note: we plan to rename it to denote its generic nature, see
// https://github.com/cockroachdb/cockroach/pull/73309
type NodeIDContainer struct {
	_ util.NoCopy

	// nodeID represents either a NodeID or a SQLInstanceID (if
	// sqlInstance is set). It is accessed atomically.
	nodeID int32

	// sqlInstance is set to true when the node is meant to be a
	// standalone SQL instance.
	standaloneSQLInstance bool

	// If nodeID has been set, str represents nodeID converted to string. We
	// precompute this value to speed up String() and keep it from allocating
	// memory dynamically.
	str atomic.Value

	// OnSet, if non-nil, is called after the ID is set with the new value.
	OnSet func(roachpb.NodeID)
}

// String returns the node ID, or "?" if it is unset.
func (n *NodeIDContainer) String() string {
	s := n.str.Load()
	if s == nil {
		if n.standaloneSQLInstance {
			return "sql?"
		}
		return "?"
	}
	return s.(string)
}

var _ redact.SafeValue = &NodeIDContainer{}

// SafeValue implements the redact.SafeValue interface.
func (n *NodeIDContainer) SafeValue() {}

// Get returns the current node ID; 0 if it is unset.
//
// Note that Get() returns a value of type roachpb.NodeID even though
// the container is configured to store SQL instance IDs. This is
// because components that call Get() do so in a context where the
// type distinction between NodeID and SQLInstanceID does not matter,
// and we benefit from using a single type instead of duplicating the
// code. See for example the `rpc` package, where server-to-server
// RPCs get addressed with server IDs regardless of whether they are
// KV nodes or SQL instances.
// See also: https://github.com/cockroachdb/cockroach/pull/73309
func (n *NodeIDContainer) Get() roachpb.NodeID {
	return roachpb.NodeID(atomic.LoadInt32(&n.nodeID))
}

// Set sets the current node ID. If it is already set, the value must match.
func (n *NodeIDContainer) Set(ctx context.Context, val roachpb.NodeID) {
	n.setInternal(ctx, int32(val))
}

func (n *NodeIDContainer) setInternal(ctx context.Context, val int32) {
	if val <= 0 {
		panic(errors.AssertionFailedf("trying to set invalid NodeID: %d", val))
	}
	oldVal := atomic.SwapInt32(&n.nodeID, val)
	if oldVal != 0 && oldVal != val {
		panic(errors.AssertionFailedf("different IDs set: %d, then %d", oldVal, val))
	}
	prefix := ""
	if n.standaloneSQLInstance {
		prefix = "sql"
	}
	n.str.Store(prefix + strconv.Itoa(int(val)))
	if oldVal == 0 && n.OnSet != nil {
		n.OnSet(roachpb.NodeID(val))
	}
}

// Reset changes the NodeID regardless of the old value.
//
// Should only be used in testing code.
func (n *NodeIDContainer) Reset(val roachpb.NodeID) {
	atomic.StoreInt32(&n.nodeID, int32(val))
	n.str.Store(strconv.Itoa(int(val)))
}

// StoreIDContainer is added as a logtag in the pebbleLogger's context.
// The storeID value is later set atomically. The storeID can only be
// set after engine creation because the storeID is determined only after the
// pebbleLogger's context is created.
type StoreIDContainer struct {
	_ util.NoCopy

	// storeID is accessed atomically.
	storeID int32

	// If storeID has been set, str represents storeID converted to string. We
	// precompute this value to speed up String() and keep it from allocating
	// memory dynamically.
	str atomic.Value
}

// TempStoreID is used as the store id for a temp pebble engine's log
const TempStoreID = -1

// String returns "temp" for temp stores, and the storeID for main
// stores if they haven't been initialized. If a main store hasn't
// been initialized, then "?" is returned.
func (s *StoreIDContainer) String() string {
	str := s.str.Load()
	if str == nil {
		return "?"
	}
	return str.(string)
}

var _ redact.SafeValue = &StoreIDContainer{}

// SafeValue implements the redact.SafeValue interface.
func (s *StoreIDContainer) SafeValue() {}

// Get returns the current storeID; 0 if it is unset.
func (s *StoreIDContainer) Get() int32 {
	return atomic.LoadInt32(&s.storeID)
}

// Set sets the current storeID. If it is already set, the value should match.
func (s *StoreIDContainer) Set(ctx context.Context, val int32) {
	if val != TempStoreID && val <= 0 {
		panic(errors.AssertionFailedf("trying to set invalid storeID for the store in the Pebble log: %d", val))
	}
	oldVal := atomic.SwapInt32(&s.storeID, val)
	if oldVal != 0 && oldVal != val {
		panic(errors.AssertionFailedf("different storeIDs set for the store in the Pebble log: %d, then %d",
			oldVal, val))
	}
	if val == TempStoreID {
		s.str.Store("temp")
	} else {
		s.str.Store(strconv.Itoa(int(val)))
	}
}

// A SQLInstanceID is an ephemeral ID assigned to a running instance of the SQL
// server. This is distinct from a NodeID, which is a long-lived identifier
// assigned to a node in the KV layer which is unique across all KV nodes in the
// cluster and persists across restarts. Instead, a Instance is similar to a
// process ID from the unix world: an integer assigned to the SQL server
// on process start which is unique across all SQL server processes running
// on behalf of the tenant, while the SQL server is running.
type SQLInstanceID int32

func (s SQLInstanceID) String() string {
	if s == 0 {
		return "?"
	}
	return strconv.Itoa(int(s))
}

// SafeValue implements the redact.SafeValue interface.
func (s SQLInstanceID) SafeValue() {}

// SQLIDContainer is a variant of NodeIDContainer that contains SQL instance IDs.
type SQLIDContainer NodeIDContainer

// NewSQLIDContainerForNode sets up a SQLIDContainer which serves the underlying
// NodeID as the SQL instance ID.
func NewSQLIDContainerForNode(nodeID *NodeIDContainer) *SQLIDContainer {
	if nodeID.standaloneSQLInstance {
		// This assertion exists to prevent misuse of the API, where a
		// caller would call NewSQLIDContainerForNode() once, cast the
		// result type to `*NodeIDContainer`, then mistakenly call
		// NewSQLIDContainerForNode() again.
		panic(errors.AssertionFailedf("programming error: container is already for a standalone SQL instance"))
	}
	return (*SQLIDContainer)(nodeID)
}

// SwitchToSQLIDContainer changes a NodeIDContainer to become able to
// store SQL instance IDs for standalone SQL instances.
//
// After it has been switched, the original container will report the
// SQL instance ID value as NodeID via its Get() method, under the
// assumption that anything using that ID actually needs the SQL
// Instance ID.
func (n *NodeIDContainer) SwitchToSQLIDContainerForStandaloneSQLInstance() *SQLIDContainer {
	sc := NewSQLIDContainerForNode(n)
	sc.standaloneSQLInstance = true
	return sc
}

// SetSQLInstanceID sets the SQL instance ID. It returns an error if
// we attempt to set an instance ID when the nodeID has already been
// initialized.
func (c *SQLIDContainer) SetSQLInstanceID(ctx context.Context, sqlInstanceID SQLInstanceID) error {
	(*NodeIDContainer)(c).setInternal(ctx, int32(sqlInstanceID))
	return nil
}

// OptionalNodeID returns the NodeID and true, if the former is exposed.
// Otherwise, returns zero and false.
func (c *SQLIDContainer) OptionalNodeID() (roachpb.NodeID, bool) {
	if (*NodeIDContainer)(c).standaloneSQLInstance {
		return 0, false
	}
	return (*NodeIDContainer)(c).Get(), true
}

// SQLInstanceID returns the wrapped SQLInstanceID.
func (c *SQLIDContainer) SQLInstanceID() SQLInstanceID {
	return SQLInstanceID((*NodeIDContainer)(c).Get())
}

// SafeValue implements the redact.SafeValue interface.
func (c *SQLIDContainer) SafeValue() {}

func (c *SQLIDContainer) String() string { return (*NodeIDContainer)(c).String() }

// TestingIDContainer is an SQLIDContainer with hard-coded SQLInstanceID of 10.
var TestingIDContainer = func() *SQLIDContainer {
	var c NodeIDContainer
	sc := c.SwitchToSQLIDContainerForStandaloneSQLInstance()
	if err := sc.SetSQLInstanceID(context.Background(), 10); err != nil {
		panic(err)
	}
	return sc
}()
