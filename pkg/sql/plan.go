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

package sql

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type planMaker interface {
	// newPlan starts preparing the query plan for a single SQL
	// statement.
	//
	// It performs as many early checks as possible on the structure of
	// the SQL statement, including verifying permissions and type
	// checking.  The returned plan object is not ready to execute; the
	// optimizePlan() method must be called first. See makePlan()
	// below.
	//
	// This method should not be used directly; instead prefer makePlan()
	// or prepare() below.
	newPlan(
		ctx context.Context, stmt tree.Statement, desiredTypes []types.T,
	) (planNode, error)

	// makePlan prepares the query plan for a single SQL statement.  it
	// calls newPlan() then optimizePlan() on the result.  Execution must
	// start by calling Start() first and then iterating using Next()
	// and Values() in order to retrieve matching rows.
	//
	// makePlan starts preparing the query plan for a single SQL
	// statement.
	// It performs as many early checks as possible on the structure of
	// the SQL statement, including verifying permissions and type checking.
	// The returned plan object is ready to execute. Execution
	// must start by calling Start() first and then iterating using
	// Next() and Values() in order to retrieve matching
	// rows.
	makePlan(ctx context.Context, stmt Statement) (planNode, error)

	// prepare does the same checks as makePlan but skips building some
	// data structures necessary for execution, based on the assumption
	// that the plan will never be run. A planNode built with prepare()
	// will do just enough work to check the structural validity of the
	// SQL statement and determine types for placeholders. However it is
	// not appropriate to call optimizePlan(), Next() or Values() on a plan
	// object created with prepare().
	prepare(ctx context.Context, stmt tree.Statement) (planNode, error)
}

var _ planMaker = &planner{}

// runParams is a struct containing all parameters passed to planNode.Next() and
// planNode.Start().
type runParams struct {
	// context.Context for this method call.
	ctx context.Context

	// evalCtx is the tree.EvalContext associated with this execution.
	// Used during local execution and distsql physical planning.
	evalCtx *tree.EvalContext

	// planner associated with this execution. Only used during local
	// execution.
	p *planner
}

// planNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planMaker.newPlan()
// - planMaker.prepare()
// - planMaker.setNeededColumns()  (needed_columns.go)
// - planMaker.expandPlan()        (expand_plan.go)
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - planMaker.optimizeFilters()   (filter_opt.go)
// - setLimitHint()                (limit_hint.go)
// - collectSpans()                (plan_spans.go)
// - planOrdering()                (plan_ordering.go)
// - planColumns()                 (plan_columns.go)
//
type planNode interface {
	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
	// See executor.go: forEachRow() for an example.
	//
	// Available after startPlan(). It is illegal to call Next() after it returns
	// false. It is legal to call Next() even if the node implements
	// planNodeFastPath and the FastPathResults() method returns true.
	Next(params runParams) (bool, error)

	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	//
	// Available after Next().
	Values() tree.Datums

	// Close terminates the planNode execution and releases its resources.
	// This method should be called if the node has been used in any way (any
	// methods on it have been called) after it was constructed. Note that this
	// doesn't imply that startPlan() has been necessarily called.
	Close(ctx context.Context)
}

// PlanNode is the exported name for planNode. Useful for CCL hooks.
type PlanNode = planNode

// planNodeFastPath is implemented by nodes that can perform all their
// work during startPlan(), possibly affecting even multiple rows. For
// example, DELETE can do this.
type planNodeFastPath interface {
	// FastPathResults returns the affected row count and true if the
	// node has no result set and has already executed when startPlan() completes.
	// Note that Next() must still be valid even if this method returns
	// true, although it may have nothing left to do.
	FastPathResults() (int, bool)
}

var _ planNode = &alterTableNode{}
var _ planNode = &alterSequenceNode{}
var _ planNode = &copyNode{}
var _ planNode = &createDatabaseNode{}
var _ planNode = &createIndexNode{}
var _ planNode = &createTableNode{}
var _ planNode = &createViewNode{}
var _ planNode = &createSequenceNode{}
var _ planNode = &createStatsNode{}
var _ planNode = &delayedNode{}
var _ planNode = &deleteNode{}
var _ planNode = &distinctNode{}
var _ planNode = &dropDatabaseNode{}
var _ planNode = &dropIndexNode{}
var _ planNode = &dropTableNode{}
var _ planNode = &dropViewNode{}
var _ planNode = &dropSequenceNode{}
var _ planNode = &zeroNode{}
var _ planNode = &unaryNode{}
var _ planNode = &explainDistSQLNode{}
var _ planNode = &explainPlanNode{}
var _ planNode = &showTraceNode{}
var _ planNode = &filterNode{}
var _ planNode = &groupNode{}
var _ planNode = &hookFnNode{}
var _ planNode = &indexJoinNode{}
var _ planNode = &insertNode{}
var _ planNode = &joinNode{}
var _ planNode = &limitNode{}
var _ planNode = &ordinalityNode{}
var _ planNode = &testingRelocateNode{}
var _ planNode = &renderNode{}
var _ planNode = &scanNode{}
var _ planNode = &scatterNode{}
var _ planNode = &showRangesNode{}
var _ planNode = &showFingerprintsNode{}
var _ planNode = &sortNode{}
var _ planNode = &splitNode{}
var _ planNode = &unionNode{}
var _ planNode = &updateNode{}
var _ planNode = &valueGenerator{}
var _ planNode = &valuesNode{}
var _ planNode = &windowNode{}
var _ planNode = &CreateUserNode{}
var _ planNode = &DropUserNode{}

var _ planNodeFastPath = &alterUserSetPasswordNode{}
var _ planNodeFastPath = &createTableNode{}
var _ planNodeFastPath = &CreateUserNode{}
var _ planNodeFastPath = &deleteNode{}
var _ planNodeFastPath = &DropUserNode{}
var _ planNodeFastPath = &setZoneConfigNode{}

// makePlan implements the Planner interface.
func (p *planner) makePlan(ctx context.Context, stmt Statement) (planNode, error) {
	plan, err := p.newPlan(ctx, stmt.AST, nil)
	if err != nil {
		return nil, err
	}
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(planColumns(plan)) {
			return nil, pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
				"cached plan must not change result type")
		}
	}
	if err := p.semaCtx.Placeholders.AssertAllAssigned(); err != nil {
		return nil, err
	}

	needed := allColumns(plan)
	plan, err = p.optimizePlan(ctx, plan, needed)
	if err != nil {
		// Once the plan has undergone optimization, it may contain
		// monitor-registered memory, even in case of error.
		plan.Close(ctx)
		return nil, err
	}

	if log.V(3) {
		log.Infof(ctx, "statement %s compiled to:\n%s", stmt, planToString(ctx, plan))
	}
	return plan, nil
}

// startPlan starts the plan and all its sub-query nodes.
func startPlan(params runParams, plan planNode) error {
	if err := startExec(params, plan); err != nil {
		return err
	}
	// Trigger limit propagation through the plan and sub-queries.
	params.p.setUnlimited(plan)
	return nil
}

// execStartable is implemented by planNodes that have an initial
// execution step.
type execStartable interface {
	startExec(params runParams) error
}

// startExec calls startExec() on each planNode that supports
// execStartable using a depth-first, post-order traversal.
// The subqueries, if any, are also started.
//
// Reminder: walkPlan() ensures that subqueries and sub-plans are
// started before startExec() is called.
func startExec(params runParams, plan planNode) error {
	o := planObserver{
		enterNode: func(ctx context.Context, _ string, p planNode) (bool, error) {
			switch p.(type) {
			case *explainPlanNode, *explainDistSQLNode:
				// Do not recurse: we're not starting the plan if we just show its structure with EXPLAIN.
				return false, nil
			case *showTraceNode:
				// showTrace needs to override the params struct, and does so in its startExec() method.
				return false, nil
			case *createStatsNode:
				return false, errors.Errorf("statistics can only be created via DistSQL")
			}
			return true, nil
		},
		leaveNode: func(_ string, n planNode) error {
			if s, ok := n.(execStartable); ok {
				return s.startExec(params)
			}
			return nil
		},
		subqueryNode: func(ctx context.Context, sq *subquery) error {
			if !sq.expanded {
				panic("subquery was not expanded properly")
			}
			if !sq.started {
				if err := startExec(params, sq.plan); err != nil {
					return err
				}
				sq.started = true
				res, err := sq.doEval(ctx, params.p)
				if err != nil {
					return err
				}
				sq.result = res
			}
			return nil
		},
	}
	return walkPlan(params.ctx, plan, o)
}

func (p *planner) maybePlanHook(ctx context.Context, stmt tree.Statement) (planNode, error) {
	// TODO(dan): This iteration makes the plan dispatch no longer constant
	// time. We could fix that with a map of `reflect.Type` but including
	// reflection in such a primary codepath is unfortunate. Instead, the
	// upcoming IR work will provide unique numeric type tags, which will
	// elegantly solve this.
	for _, planHook := range planHooks {
		if fn, header, err := planHook(stmt, p); err != nil {
			return nil, err
		} else if fn != nil {
			return &hookFnNode{f: fn, header: header}, nil
		}
	}
	for _, planHook := range wrappedPlanHooks {
		if node, err := planHook(ctx, stmt, p); err != nil {
			return nil, err
		} else if node != nil {
			return node, err
		}
	}

	return nil, nil
}

// delegateQuery creates a plan for a given SQL query.
// In addition, the caller can specify an additional validation
// function (initialCheck) that will be ran and checked for errors
// during plan optimization. This is meant for checks that cannot be
// run during a SQL prepare operation.
func (p *planner) delegateQuery(
	ctx context.Context,
	name string,
	sql string,
	initialCheck func(ctx context.Context) error,
	desiredTypes []types.T,
) (planNode, error) {
	// Prepare the sub-plan.
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return nil, err
	}
	plan, err := p.newPlan(ctx, stmt, desiredTypes)
	if err != nil {
		return nil, err
	}

	if initialCheck == nil {
		return plan, nil
	}

	// To enable late calling into initialCheck, we use a delayedNode.
	return &delayedNode{
		name: name,

		// The columns attribute cannot be a straight-up reference to the sub-plan's
		// own columns, because they can be modified in-place by setNeededColumns().
		columns: append(sqlbase.ResultColumns(nil), planColumns(plan)...),

		// The delayed constructor's only responsibility is to call
		// initialCheck() - the plan is already constructed.
		constructor: func(ctx context.Context, _ *planner) (planNode, error) {
			if err := initialCheck(ctx); err != nil {
				return nil, err
			}
			return plan, nil
		},

		// Breaking with the common usage pattern of delayedNode, where
		// the plan attribute is initially nil (the constructor creates
		// it), here we prepopulate the field with the sub-plan created
		// above. We do this instead of simply returning the newly created
		// sub-plan in a constructor closure, to ensure the sub-plan is
		// properly Close()d if the delayedNode is discarded before its
		// constructor is called.
		plan: plan,
	}, nil
}

// newPlan constructs a planNode from a statement. This is used
// recursively by the various node constructors.
func (p *planner) newPlan(
	ctx context.Context, stmt tree.Statement, desiredTypes []types.T,
) (planNode, error) {
	tracing.AnnotateTrace()

	// This will set the system DB trigger for transactions containing
	// schema-modifying statements that have no effect, such as
	// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
	// where the table already exists. This will generate some false
	// refreshes, but that's expected to be quite rare in practice.
	canModifySchema := tree.CanModifySchema(stmt)
	if canModifySchema {
		if err := p.txn.SetSystemConfigTrigger(); err != nil {
			return nil, errors.Wrap(err,
				"schema change statement cannot follow a statement that has written in the same transaction")
		}
	}

	if p.session.TxnState.readOnly {
		if canModifySchema || tree.CanWriteData(stmt) {
			return nil, pgerror.NewErrorf(pgerror.CodeReadOnlySQLTransactionError,
				"cannot execute %s in a read-only transaction", stmt.StatementTag())
		}
	}

	if plan, err := p.maybePlanHook(ctx, stmt); plan != nil || err != nil {
		return plan, err
	}

	switch n := stmt.(type) {
	case *tree.AlterTable:
		return p.AlterTable(ctx, n)
	case *tree.AlterSequence:
		return p.AlterSequence(ctx, n)
	case *tree.AlterUserSetPassword:
		return p.AlterUserSetPassword(ctx, n)
	case *tree.BeginTransaction:
		return p.BeginTransaction(n)
	case *tree.CancelQuery:
		return p.CancelQuery(ctx, n)
	case *tree.CancelJob:
		return p.CancelJob(ctx, n)
	case *tree.Scrub:
		return p.Scrub(ctx, n)
	case CopyDataBlock:
		return p.CopyData(ctx, n)
	case *tree.CopyFrom:
		return p.Copy(ctx, n)
	case *tree.CreateDatabase:
		return p.CreateDatabase(n)
	case *tree.CreateIndex:
		return p.CreateIndex(ctx, n)
	case *tree.CreateTable:
		return p.CreateTable(ctx, n)
	case *tree.CreateUser:
		return p.CreateUser(ctx, n)
	case *tree.CreateView:
		return p.CreateView(ctx, n)
	case *tree.CreateSequence:
		return p.CreateSequence(ctx, n)
	case *tree.CreateStats:
		return p.CreateStatistics(ctx, n)
	case *tree.Deallocate:
		return p.Deallocate(ctx, n)
	case *tree.Delete:
		return p.Delete(ctx, n, desiredTypes)
	case *tree.Discard:
		return p.Discard(ctx, n)
	case *tree.DropDatabase:
		return p.DropDatabase(ctx, n)
	case *tree.DropIndex:
		return p.DropIndex(ctx, n)
	case *tree.DropTable:
		return p.DropTable(ctx, n)
	case *tree.DropView:
		return p.DropView(ctx, n)
	case *tree.DropSequence:
		return p.DropSequence(ctx, n)
	case *tree.DropUser:
		return p.DropUser(ctx, n)
	case *tree.Execute:
		return p.Execute(ctx, n)
	case *tree.Explain:
		return p.Explain(ctx, n)
	case *tree.Grant:
		return p.Grant(ctx, n)
	case *tree.Insert:
		return p.Insert(ctx, n, desiredTypes)
	case *tree.ParenSelect:
		return p.newPlan(ctx, n.Select, desiredTypes)
	case *tree.PauseJob:
		return p.PauseJob(ctx, n)
	case *tree.TestingRelocate:
		return p.TestingRelocate(ctx, n)
	case *tree.RenameColumn:
		return p.RenameColumn(ctx, n)
	case *tree.RenameDatabase:
		return p.RenameDatabase(ctx, n)
	case *tree.RenameIndex:
		return p.RenameIndex(ctx, n)
	case *tree.RenameTable:
		return p.RenameTable(ctx, n)
	case *tree.ResumeJob:
		return p.ResumeJob(ctx, n)
	case *tree.Revoke:
		return p.Revoke(ctx, n)
	case *tree.Scatter:
		return p.Scatter(ctx, n)
	case *tree.Select:
		return p.Select(ctx, n, desiredTypes)
	case *tree.SelectClause:
		return p.SelectClause(ctx, n, nil /* orderBy */, nil, /* limit */
			desiredTypes, publicColumns)
	case *tree.SetClusterSetting:
		return p.SetClusterSetting(ctx, n)
	case *tree.SetZoneConfig:
		return p.SetZoneConfig(ctx, n)
	case *tree.SetVar:
		return p.SetVar(ctx, n)
	case *tree.SetTransaction:
		return p.SetTransaction(n)
	case *tree.SetSessionCharacteristics:
		return p.SetSessionCharacteristics(n)
	case *tree.ShowClusterSetting:
		return p.ShowClusterSetting(ctx, n)
	case *tree.ShowVar:
		return p.ShowVar(ctx, n)
	case *tree.ShowColumns:
		return p.ShowColumns(ctx, n)
	case *tree.ShowConstraints:
		return p.ShowConstraints(ctx, n)
	case *tree.ShowCreateTable:
		return p.ShowCreateTable(ctx, n)
	case *tree.ShowCreateView:
		return p.ShowCreateView(ctx, n)
	case *tree.ShowDatabases:
		return p.ShowDatabases(ctx, n)
	case *tree.ShowGrants:
		return p.ShowGrants(ctx, n)
	case *tree.ShowHistogram:
		return p.ShowHistogram(ctx, n)
	case *tree.ShowIndex:
		return p.ShowIndex(ctx, n)
	case *tree.ShowQueries:
		return p.ShowQueries(ctx, n)
	case *tree.ShowJobs:
		return p.ShowJobs(ctx, n)
	case *tree.ShowRoles:
		return p.ShowRoles(ctx, n)
	case *tree.ShowSessions:
		return p.ShowSessions(ctx, n)
	case *tree.ShowTableStats:
		return p.ShowTableStats(ctx, n)
	case *tree.ShowTables:
		return p.ShowTables(ctx, n)
	case *tree.ShowTrace:
		return p.ShowTrace(ctx, n)
	case *tree.ShowTransactionStatus:
		return p.ShowTransactionStatus(ctx)
	case *tree.ShowUsers:
		return p.ShowUsers(ctx, n)
	case *tree.ShowZoneConfig:
		return p.ShowZoneConfig(ctx, n)
	case *tree.ShowRanges:
		return p.ShowRanges(ctx, n)
	case *tree.ShowFingerprints:
		return p.ShowFingerprints(ctx, n)
	case *tree.Split:
		return p.Split(ctx, n)
	case *tree.Truncate:
		if err := p.txn.SetSystemConfigTrigger(); err != nil {
			return nil, err
		}
		return p.Truncate(ctx, n)
	case *tree.UnionClause:
		return p.Union(ctx, n, desiredTypes)
	case *tree.Update:
		return p.Update(ctx, n, desiredTypes)
	case *tree.ValuesClause:
		return p.Values(ctx, n, desiredTypes)
	default:
		return nil, errors.Errorf("unknown statement type: %T", stmt)
	}
}

// prepare constructs the logical plan for the statement.  This is
// needed both to type placeholders and to inform pgwire of the types
// of the result columns. All statements that either support
// placeholders or have result columns must be handled here.
func (p *planner) prepare(ctx context.Context, stmt tree.Statement) (planNode, error) {
	if plan, err := p.maybePlanHook(ctx, stmt); plan != nil || err != nil {
		return plan, err
	}
	p.isPreparing = true

	switch n := stmt.(type) {
	case *tree.AlterUserSetPassword:
		return p.AlterUserSetPassword(ctx, n)
	case *tree.CancelQuery:
		return p.CancelQuery(ctx, n)
	case *tree.CancelJob:
		return p.CancelJob(ctx, n)
	case *tree.CreateUser:
		return p.CreateUser(ctx, n)
	case *tree.Delete:
		return p.Delete(ctx, n, nil)
	case *tree.DropUser:
		return p.DropUser(ctx, n)
	case *tree.Explain:
		return p.Explain(ctx, n)
	case *tree.Insert:
		return p.Insert(ctx, n, nil)
	case *tree.PauseJob:
		return p.PauseJob(ctx, n)
	case *tree.ResumeJob:
		return p.ResumeJob(ctx, n)
	case *tree.Select:
		return p.Select(ctx, n, nil)
	case *tree.SelectClause:
		return p.SelectClause(ctx, n, nil /* orderBy */, nil, /* limit */
			nil /* desiredTypes */, publicColumns)
	case *tree.SetClusterSetting:
		return p.SetClusterSetting(ctx, n)
	case *tree.SetVar:
		return p.SetVar(ctx, n)
	case *tree.ShowClusterSetting:
		return p.ShowClusterSetting(ctx, n)
	case *tree.ShowVar:
		return p.ShowVar(ctx, n)
	case *tree.ShowCreateTable:
		return p.ShowCreateTable(ctx, n)
	case *tree.ShowCreateView:
		return p.ShowCreateView(ctx, n)
	case *tree.ShowColumns:
		return p.ShowColumns(ctx, n)
	case *tree.ShowDatabases:
		return p.ShowDatabases(ctx, n)
	case *tree.ShowGrants:
		return p.ShowGrants(ctx, n)
	case *tree.ShowIndex:
		return p.ShowIndex(ctx, n)
	case *tree.ShowConstraints:
		return p.ShowConstraints(ctx, n)
	case *tree.ShowQueries:
		return p.ShowQueries(ctx, n)
	case *tree.ShowJobs:
		return p.ShowJobs(ctx, n)
	case *tree.ShowRoles:
		return p.ShowRoles(ctx, n)
	case *tree.ShowSessions:
		return p.ShowSessions(ctx, n)
	case *tree.ShowTables:
		return p.ShowTables(ctx, n)
	case *tree.ShowTrace:
		return p.ShowTrace(ctx, n)
	case *tree.ShowUsers:
		return p.ShowUsers(ctx, n)
	case *tree.ShowTransactionStatus:
		return p.ShowTransactionStatus(ctx)
	case *tree.ShowRanges:
		return p.ShowRanges(ctx, n)
	case *tree.Split:
		return p.Split(ctx, n)
	case *tree.TestingRelocate:
		return p.TestingRelocate(ctx, n)
	case *tree.Scatter:
		return p.Scatter(ctx, n)
	case *tree.Update:
		return p.Update(ctx, n, nil)
	default:
		// Other statement types do not have result columns and do not
		// support placeholders so there is no need for any special
		// handling here.
		return nil, nil
	}
}
