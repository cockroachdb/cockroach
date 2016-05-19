// Copyright 2016 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

// planner is the centerpiece of SQL statement execution combining session
// state and database state with the logic for SQL execution.
// A planner is generally part of a Session object. If one needs to be created
// outside of a Session, use makePlanner().
type planner struct {
	txn *client.Txn
	// As the planner executes statements, it may change the current user session.
	// TODO(andrei): see if the circular dependency between planner and Session
	// can be broken if we move the User and Database here from the Session.
	session  *Session
	evalCtx  parser.EvalContext
	leases   []*LeaseState
	leaseMgr *LeaseManager
	// This is used as a cache for database names.
	// TODO(andrei): get rid of it and replace it with a leasing system for
	// database descriptors.
	systemConfig  config.SystemConfig
	databaseCache *databaseCache

	testingVerifyMetadataFn func(config.SystemConfig) error
	verifyFnCheckedOnce     bool

	parser parser.Parser
	params parameters

	// Avoid allocations by embedding commonly used visitors.
	isAggregateVisitor isAggregateVisitor
	subqueryVisitor    subqueryVisitor
	qnameVisitor       qnameVisitor

	execCtx *ExecutorContext
}

// makePlanner creates a new planner instances, referencing a dummy Session.
// Only use this internally where a Session cannot be created.
func makePlanner() *planner {
	// init with an empty session. We can't leave this nil because too much code
	// looks in the session for the current database.
	return &planner{session: &Session{Location: time.UTC}}
}

// queryRunner abstracts the services provided by a planner object
// to the other SQL front-end components.
type queryRunner interface {
	// The following methods control the state of the planner during its
	// lifecycle.

	// setTxn  resets the current transaction in the planner and
	// initializes the timestamps used by SQL built-in functions from
	// the new txn object, if any.
	setTxn(*client.Txn)

	// resetTxn clears the planner's current transaction.
	resetTxn()

	// resetForBatch prepares the planner for executing a new batch of
	// statements.
	resetForBatch(e *Executor)

	// The following methods run SQL queries.

	// queryRow executes a SQL query string where exactly 1 result row is
	// expected and returns that row.
	queryRow(sql string, args ...interface{}) (parser.DTuple, error)

	// exec executes a SQL query string and returns the number of rows
	// affected.
	exec(sql string, args ...interface{}) (int, error)

	// The following methods can be used during testing.

	// setTestingVerifyMetadata sets a callback to be called after the planner
	// is done executing the current SQL statement. It can be used to verify
	// assumptions about how metadata will be asynchronously updated.
	// Note that this can overwrite a previous callback that was waiting to be
	// verified, which is not ideal.
	setTestingVerifyMetadata(fn func(config.SystemConfig) error)

	// blockConfigUpdatesMaybe will ask the Executor to block config updates,
	// so that checkTestingVerifyMetadataInitialOrDie() can later be run.
	// The point is to lock the system config so that no gossip updates sneak in
	// under us, so that we're able to assert that the verify callback only succeeds
	// after a gossip update.
	//
	// It returns an unblock function which can be called after
	// checkTestingVerifyMetadata{Initial}OrDie() has been called.
	//
	// This lock does not change semantics. Even outside of tests, the planner uses
	// static systemConfig for a user request, so locking the Executor's
	// systemConfig cannot change the semantics of the SQL operation being performed
	// under lock.
	blockConfigUpdatesMaybe(e *Executor) func()

	// checkTestingVerifyMetadataInitialOrDie verifies that the metadata callback,
	// if one was set, fails. This validates that we need a gossip update for it to
	// eventually succeed.
	// No-op if we've already done an initial check for the set callback.
	// Gossip updates for the system config are assumed to be blocked when this is
	// called.
	checkTestingVerifyMetadataInitialOrDie(e *Executor, stmts parser.StatementList)

	// checkTestingVerifyMetadataOrDie verifies the metadata callback, if one was
	// set.
	// Gossip updates for the system config are assumed to be blocked when this is
	// called.
	checkTestingVerifyMetadataOrDie(e *Executor, stmts parser.StatementList)
}

var _ queryRunner = &planner{}

// setTxn implements the queryRunner interface.
func (p *planner) setTxn(txn *client.Txn) {
	p.txn = txn
	if txn != nil {
		p.evalCtx.SetClusterTimestamp(txn.Proto.OrigTimestamp)
	} else {
		p.evalCtx.SetTxnTimestamp(time.Time{})
		p.evalCtx.SetStmtTimestamp(time.Time{})
		p.evalCtx.SetClusterTimestamp(roachpb.ZeroTimestamp)
	}
}

// resetTxn implements the queryRunner interface.
func (p *planner) resetTxn() {
	p.setTxn(nil)
}

// resetForBatch implements the queryRunner interface.
func (p *planner) resetForBatch(e *Executor) {
	// Update the systemConfig to a more recent copy, so that we can use tables
	// that we created in previus batches of the same transaction.
	cfg, cache := e.getSystemConfig()
	p.systemConfig = cfg
	p.databaseCache = cache

	p.params = parameters{}

	// The parser cannot be reused between batches.
	p.parser = parser.Parser{}

	p.evalCtx = parser.EvalContext{
		NodeID:   e.nodeID,
		ReCache:  e.reCache,
		TmpDec:   new(inf.Dec),
		Location: p.session.Location,
	}
	p.session.TxnState.schemaChangers.curGroupNum++
}

// query initializes a planNode from a SQL statement string.  This
// should not be used directly; queryRow() and exec() below should be
// used instead.
func (p *planner) query(sql string, args ...interface{}) (planNode, error) {
	stmt, err := parser.ParseOneTraditional(sql)
	if err != nil {
		return nil, err
	}
	stmt, err = parser.FillArgs(stmt, golangParameters(args))
	if err != nil {
		return nil, err
	}
	return p.makePlan(stmt, false)
}

// queryRow implements the queryRunner interface.
func (p *planner) queryRow(sql string, args ...interface{}) (parser.DTuple, error) {
	plan, err := p.query(sql, args...)
	if err != nil {
		return nil, err
	}
	if err := plan.Start(); err != nil {
		return nil, err
	}
	if !plan.Next() {
		if err := plan.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	values := plan.Values()
	if plan.Next() {
		return nil, util.Errorf("%s: unexpected multiple results", sql)
	}
	if err := plan.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

// exec implements the queryRunner interface.
func (p *planner) exec(sql string, args ...interface{}) (int, error) {
	plan, err := p.query(sql, args...)
	if err != nil {
		return 0, err
	}
	if err := plan.Start(); err != nil {
		return 0, err
	}
	return countRowsAffected(plan), plan.Err()
}

// setTestingVerifyMetadata implements the queryRunner interface.
func (p *planner) setTestingVerifyMetadata(fn func(config.SystemConfig) error) {
	p.testingVerifyMetadataFn = fn
	p.verifyFnCheckedOnce = false
}

// blockConfigUpdatesMaybe implements the queryRunner interface.
func (p *planner) blockConfigUpdatesMaybe(e *Executor) func() {
	if !e.ctx.TestingKnobs.WaitForGossipUpdate {
		return func() {}
	}
	return e.blockConfigUpdates()
}

// checkTestingVerifyMetadataInitialOrDie implements the queryRunner interface.
func (p *planner) checkTestingVerifyMetadataInitialOrDie(
	e *Executor, stmts parser.StatementList) {
	if !p.execCtx.TestingKnobs.WaitForGossipUpdate {
		return
	}
	// If there's nothinging to verify, or we've already verified the initial
	// condition, there's nothing to do.
	if p.testingVerifyMetadataFn == nil || p.verifyFnCheckedOnce {
		return
	}
	if p.testingVerifyMetadataFn(e.systemConfig) == nil {
		panic(fmt.Sprintf(
			"expected %q (or the statements before them) to require a "+
				"gossip update, but they did not", stmts))
	}
	p.verifyFnCheckedOnce = true
}

// checkTestingVerifyMetadataOrDie implements the queryRunner interface.
func (p *planner) checkTestingVerifyMetadataOrDie(
	e *Executor, stmts parser.StatementList) {
	if !p.execCtx.TestingKnobs.WaitForGossipUpdate ||
		p.testingVerifyMetadataFn == nil {
		return
	}
	if !p.verifyFnCheckedOnce {
		panic("intial state of the condition to verify was not checked")
	}

	for p.testingVerifyMetadataFn(e.systemConfig) != nil {
		e.waitForConfigUpdate()
	}
	p.testingVerifyMetadataFn = nil
}
