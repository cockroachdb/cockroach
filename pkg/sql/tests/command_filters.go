// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// CommandFilters provides facilities for registering "TestingCommandFilters"
// (i.e. functions to be run on every replica command).
// CommandFilters is thread-safe.
// CommandFilters also optionally does replay protection if filters need it.
type CommandFilters struct {
	syncutil.RWMutex
	filters []struct {
		id         int
		idempotent bool
		filter     kvserverbase.ReplicaCommandFilter
	}
	nextID int

	numFiltersTrackingReplays int
	replayProtection          kvserverbase.ReplicaCommandFilter
}

// RunFilters executes the registered filters, stopping at the first one
// that returns an error.
func (c *CommandFilters) RunFilters(args kvserverbase.FilterArgs) *roachpb.Error {
	c.RLock()
	defer c.RUnlock()

	if c.replayProtection != nil {
		return c.replayProtection(args)
	}
	return c.runFiltersInternal(args)
}

func (c *CommandFilters) runFiltersInternal(args kvserverbase.FilterArgs) *roachpb.Error {
	for _, f := range c.filters {
		if pErr := f.filter(args); pErr != nil {
			return pErr
		}
	}
	return nil
}

// AppendFilter registers a filter function to run after all the previously
// registered filters.
// idempotent specifies if this filter can be safely run multiple times on the
// same command. If this property doesn't hold, CommandFilters will start
// tracking commands for replay protection, which might be expensive.
// Returns a closure that the client must run for doing cleanup when the
// filter should be deregistered.
func (c *CommandFilters) AppendFilter(
	filter kvserverbase.ReplicaCommandFilter, idempotent bool,
) func() {

	c.Lock()
	defer c.Unlock()
	id := c.nextID
	c.nextID++
	c.filters = append(c.filters, struct {
		id         int
		idempotent bool
		filter     kvserverbase.ReplicaCommandFilter
	}{id, idempotent, filter})

	if !idempotent {
		if c.numFiltersTrackingReplays == 0 {
			c.replayProtection =
				storageutils.WrapFilterForReplayProtection(c.runFiltersInternal)
		}
		c.numFiltersTrackingReplays++
	}

	return func() {
		c.removeFilter(id)
	}
}

// removeFilter removes a filter previously registered. Meant to be used as the
// closure returned by AppendFilter.
func (c *CommandFilters) removeFilter(id int) {
	c.Lock()
	defer c.Unlock()
	for i, f := range c.filters {
		if f.id == id {
			if !f.idempotent {
				c.numFiltersTrackingReplays--
				if c.numFiltersTrackingReplays == 0 {
					c.replayProtection = nil
				}
			}
			c.filters = append(c.filters[:i], c.filters[i+1:]...)
			return
		}
	}
	panic(errors.AssertionFailedf("failed to find filter with id: %d.", id))
}
