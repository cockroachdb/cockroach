// Copyright 2015 The Cockroach Authors.
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
//
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"
)

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

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

// runFilters executes the registered filters, stopping at the first one
// that returns an error.
func (c *CommandFilters) runFilters(args storagebase.FilterArgs) *roachpb.Error {
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
	filter storagebase.ReplicaCommandFilter, idempotent bool) func() {

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

// checkEndTransactionTrigger verifies that an EndTransactionRequest
// that includes intents for the SystemDB keys sets the proper trigger.
func checkEndTransactionTrigger(args storagebase.FilterArgs) *roachpb.Error {
	req, ok := args.Req.(*roachpb.EndTransactionRequest)
	if !ok {
		return nil
	}

	if !req.Commit {
		// This is a rollback: skip trigger verification.
		return nil
	}

	modifiedSpanTrigger := req.InternalCommitTrigger.GetModifiedSpanTrigger()
	modifiedSystemConfigSpan := modifiedSpanTrigger != nil && modifiedSpanTrigger.SystemConfigSpan

	var hasSystemKey bool
	for _, span := range req.IntentSpans {
		keyAddr, err := keys.Addr(span.Key)
		if err != nil {
			return roachpb.NewError(err)
		}
		if bytes.Compare(keyAddr, keys.SystemConfigSpan.Key) >= 0 &&
			bytes.Compare(keyAddr, keys.SystemConfigSpan.EndKey) < 0 {
			hasSystemKey = true
			break
		}
	}
	// If the transaction in question has intents in the system span, then
	// modifiedSystemConfigSpan should always be true. However, it is possible
	// for modifiedSystemConfigSpan to be set, even though no system keys are
	// present. This can occur with certain conditional DDL statements (e.g.
	// "CREATE TABLE IF NOT EXISTS"), which set the SystemConfigTrigger
	// aggressively but may not actually end up changing the system DB depending
	// on the current state.
	// For more information, see the related comment at the beginning of
	// planner.makePlan().
	if hasSystemKey && !modifiedSystemConfigSpan {
		return roachpb.NewError(errors.Errorf("EndTransaction hasSystemKey=%t, but hasSystemConfigTrigger=%t",
			hasSystemKey, modifiedSystemConfigSpan))
	}

	return nil
}

// createTestServerParams creates a set of params suitable for SQL tests.
// It enables some EndTransaction sanity checking and installs a flexible
// TestingCommandFilter.
// TODO(andrei): this function is not used consistently by SQL tests. Figure out
// if the EndTransaction checks are important.
func createTestServerParams() (base.TestServerArgs, *CommandFilters) {
	var cmdFilters CommandFilters
	cmdFilters.AppendFilter(checkEndTransactionTrigger, true)
	params := base.TestServerArgs{}
	params.Knobs.Store = &storage.StoreTestingKnobs{
		TestingCommandFilter: cmdFilters.runFilters,
	}
	return params, &cmdFilters
}

func TestMain(m *testing.M) {
	security.SetReadFileFn(securitytest.Asset)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
