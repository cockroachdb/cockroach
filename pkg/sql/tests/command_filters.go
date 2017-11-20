// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tests

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		filter     storagebase.ReplicaCommandFilter
	}
	nextID int

	numFiltersTrackingReplays int
	replayProtection          storagebase.ReplicaCommandFilter
}

// RunFilters executes the registered filters, stopping at the first one
// that returns an error.
func (c *CommandFilters) RunFilters(args storagebase.FilterArgs) *roachpb.Error {
	c.RLock()
	defer c.RUnlock()

	if c.replayProtection != nil {
		return c.replayProtection(args)
	}
	return c.runFiltersInternal(args)
}

func (c *CommandFilters) runFiltersInternal(args storagebase.FilterArgs) *roachpb.Error {
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
	filter storagebase.ReplicaCommandFilter, idempotent bool,
) func() {

	c.Lock()
	defer c.Unlock()
	id := c.nextID
	c.nextID++
	c.filters = append(c.filters, struct {
		id         int
		idempotent bool
		filter     storagebase.ReplicaCommandFilter
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
	panic(fmt.Sprintf("failed to find filter with id: %d.", id))
}
