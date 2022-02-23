// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"reflect"

	"github.com/cockroachdb/errors"
)

// planObserver is the interface to implement by components that need
// to visit a planNode tree.
// Used mainly by EXPLAIN, but also for the collector of back-references
// for view definitions.
type planObserver struct {
	// replaceNode is invoked upon entering a tree node. It can replace the
	// current planNode in the tree by returning a non-nil planNode. Returning
	// nil will continue the recursion and not modify the current node.
	replaceNode func(ctx context.Context, nodeName string, plan planNode) (planNode, error)

	// enterNode is invoked upon entering a tree node. It can return false to
	// stop the recursion at this node.
	enterNode func(ctx context.Context, nodeName string, plan planNode) (bool, error)

	// leaveNode is invoked upon leaving a tree node.
	leaveNode func(nodeName string, plan planNode) error
}

// walkPlan performs a depth-first traversal of the plan given as
// argument, informing the planObserver of the node details at each
// level.
func walkPlan(ctx context.Context, plan planNode, observer planObserver) error {
	v := makePlanVisitor(ctx, observer)
	v.visit(plan)
	return v.err
}

// planVisitor is the support structure for walkPlan().
type planVisitor struct {
	observer planObserver
	ctx      context.Context
	err      error
}

// makePlanVisitor creates a planVisitor instance.
// ctx will be stored in the planVisitor and used when visiting planNode's and
// expressions..
func makePlanVisitor(ctx context.Context, observer planObserver) planVisitor {
	return planVisitor{observer: observer, ctx: ctx}
}

// visit is the recursive function that supports walkPlan().
func (v *planVisitor) visit(plan planNode) planNode {
	if v.err != nil {
		return plan
	}

	name := nodeName(plan)

	if v.observer.replaceNode != nil {
		newNode, err := v.observer.replaceNode(v.ctx, name, plan)
		if err != nil {
			v.err = err
			return plan
		}
		if newNode != nil {
			return newNode
		}
	}
	v.visitInternal(plan, name)
	return plan
}

// visitConcrete is like visit, but provided for the case where a planNode is
// trying to recurse into a concrete planNode type, and not a planNode
// interface.
func (v *planVisitor) visitConcrete(plan planNode) {
	if v.err != nil {
		return
	}

	name := nodeName(plan)
	v.visitInternal(plan, name)
}

func (v *planVisitor) visitInternal(plan planNode, name string) {
	if v.err != nil {
		return
	}
	recurse := true

	if v.observer.enterNode != nil {
		recurse, v.err = v.observer.enterNode(v.ctx, name, plan)
		if v.err != nil {
			return
		}
	}
	if v.observer.leaveNode != nil {
		defer func() {
			if v.err != nil {
				return
			}
			v.err = v.observer.leaveNode(name, plan)
		}()
	}

	if !recurse {
		return
	}

	switch n := plan.(type) {
	case *valuesNode:
	case *scanNode:

	case *filterNode:
		n.source.plan = v.visit(n.source.plan)

	case *renderNode:
		n.source.plan = v.visit(n.source.plan)

	case *indexJoinNode:
		n.input = v.visit(n.input)

	case *lookupJoinNode:
		n.input = v.visit(n.input)

	case *vTableLookupJoinNode:
		n.input = v.visit(n.input)

	case *zigzagJoinNode:

	case *applyJoinNode:
		n.input.plan = v.visit(n.input.plan)

	case *joinNode:
		n.left.plan = v.visit(n.left.plan)
		n.right.plan = v.visit(n.right.plan)

	case *invertedFilterNode:
		n.input = v.visit(n.input)

	case *invertedJoinNode:
		n.input = v.visit(n.input)

	case *limitNode:
		n.plan = v.visit(n.plan)

	case *max1RowNode:
		n.plan = v.visit(n.plan)

	case *distinctNode:
		n.plan = v.visit(n.plan)

	case *sortNode:
		n.plan = v.visit(n.plan)

	case *topKNode:
		n.plan = v.visit(n.plan)

	case *groupNode:
		n.plan = v.visit(n.plan)

	case *windowNode:
		n.plan = v.visit(n.plan)

	case *unionNode:
		n.left = v.visit(n.left)
		n.right = v.visit(n.right)

	case *splitNode:
		n.rows = v.visit(n.rows)

	case *unsplitNode:
		n.rows = v.visit(n.rows)

	case *relocateNode:
		n.rows = v.visit(n.rows)

	case *relocateRange:
		n.rows = v.visit(n.rows)

	case *insertNode, *insertFastPathNode:
		if ins, ok := n.(*insertNode); ok {
			ins.source = v.visit(ins.source)
		}

	case *upsertNode:
		n.source = v.visit(n.source)

	case *updateNode:
		n.source = v.visit(n.source)

	case *deleteNode:
		n.source = v.visit(n.source)

	case *deleteRangeNode:

	case *serializeNode:
		v.visitConcrete(n.source)

	case *rowCountNode:
		v.visitConcrete(n.source)

	case *createTableNode:
		if n.n.As() {
			n.sourcePlan = v.visit(n.sourcePlan)
		}

	case *alterTenantSetClusterSettingNode:
	case *createViewNode:
	case *setVarNode:
	case *setClusterSettingNode:
	case *resetAllNode:

	case *delayedNode:
		if n.plan != nil {
			n.plan = v.visit(n.plan)
		}

	case *explainVecNode:
		// We check whether planNode is nil because the plan might be
		// represented physically. We don't yet have a walker over such
		// representation, so we simply short-circuit.
		// TODO(yuzefovich): implement that walker and use it here.
		if n.plan.main.planNode == nil {
			return
		}
		n.plan.main.planNode = v.visit(n.plan.main.planNode)

	case *explainDDLNode:
		// We check whether planNode is nil because the plan might be
		// represented physically. We don't yet have a walker over such
		// representation, so we simply short-circuit.
		// TODO(yuzefovich): implement that walker and use it here.
		if n.plan.main.planNode == nil {
			return
		}
		n.plan.main.planNode = v.visit(n.plan.main.planNode)

	case *ordinalityNode:
		n.source = v.visit(n.source)

	case *spoolNode:
		n.source = v.visit(n.source)

	case *saveTableNode:
		n.source = v.visit(n.source)

	case *showTraceReplicaNode:
		n.plan = v.visit(n.plan)

	case *cancelQueriesNode:
		n.rows = v.visit(n.rows)

	case *cancelSessionsNode:
		n.rows = v.visit(n.rows)

	case *controlJobsNode:
		n.rows = v.visit(n.rows)

	case *controlSchedulesNode:
		n.rows = v.visit(n.rows)

	case *setZoneConfigNode:

	case *projectSetNode:
		n.source = v.visit(n.source)

	case *rowSourceToPlanNode:
		// No need to recurse into the original planNode since
		// planNodeToRowSource on the other end of the adapter will take care of
		// propagating signals via its own walker.

	case *errorIfRowsNode:
		n.plan = v.visit(n.plan)

	case *scanBufferNode:

	case *bufferNode:
		n.plan = v.visit(n.plan)

	case *recursiveCTENode:
		n.initial = v.visit(n.initial)

	case *exportNode:
		n.source = v.visit(n.source)
	}
}

// nodeName returns the name of the given planNode as string.  The
// node's current state is taken into account, e.g. sortNode has
// either name "sort" or "nosort" depending on whether sorting is
// needed.
func nodeName(plan planNode) string {
	// Some nodes have custom names depending on attributes.
	switch n := plan.(type) {
	case *scanNode:
		if n.reverse {
			return "revscan"
		}
	case *unionNode:
		if n.emitAll {
			return "append"
		}

	case *joinNode:
		if len(n.mergeJoinOrdering) > 0 {
			return "merge join"
		}
		if len(n.pred.leftEqualityIndices) == 0 {
			return "cross join"
		}
		return "hash join"
	}

	name, ok := planNodeNames[reflect.TypeOf(plan)]
	if !ok {
		panic(errors.AssertionFailedf("name missing for type %T", plan))
	}

	return name
}

// planNodeNames is the mapping from node type to strings.  The
// strings are constant and not precomputed so that the type names can
// be changed without changing the output of "EXPLAIN".
var planNodeNames = map[reflect.Type]string{
	reflect.TypeOf(&alterDatabaseOwnerNode{}):           "alter database owner",
	reflect.TypeOf(&alterDatabaseAddRegionNode{}):       "alter database add region",
	reflect.TypeOf(&alterDatabasePrimaryRegionNode{}):   "alter database primary region",
	reflect.TypeOf(&alterDatabasePlacementNode{}):       "alter database placement",
	reflect.TypeOf(&alterDatabaseSurvivalGoalNode{}):    "alter database survive",
	reflect.TypeOf(&alterDatabaseDropRegionNode{}):      "alter database drop region",
	reflect.TypeOf(&alterDefaultPrivilegesNode{}):       "alter default privileges",
	reflect.TypeOf(&alterIndexNode{}):                   "alter index",
	reflect.TypeOf(&alterSequenceNode{}):                "alter sequence",
	reflect.TypeOf(&alterSchemaNode{}):                  "alter schema",
	reflect.TypeOf(&alterTableNode{}):                   "alter table",
	reflect.TypeOf(&alterTableOwnerNode{}):              "alter table owner",
	reflect.TypeOf(&alterTableSetLocalityNode{}):        "alter table set locality",
	reflect.TypeOf(&alterTableSetSchemaNode{}):          "alter table set schema",
	reflect.TypeOf(&alterTenantSetClusterSettingNode{}): "alter tenant set cluster setting",
	reflect.TypeOf(&alterTypeNode{}):                    "alter type",
	reflect.TypeOf(&alterRoleNode{}):                    "alter role",
	reflect.TypeOf(&alterRoleSetNode{}):                 "alter role set var",
	reflect.TypeOf(&applyJoinNode{}):                    "apply join",
	reflect.TypeOf(&bufferNode{}):                       "buffer",
	reflect.TypeOf(&cancelQueriesNode{}):                "cancel queries",
	reflect.TypeOf(&cancelSessionsNode{}):               "cancel sessions",
	reflect.TypeOf(&changePrivilegesNode{}):             "change privileges",
	reflect.TypeOf(&commentOnColumnNode{}):              "comment on column",
	reflect.TypeOf(&commentOnConstraintNode{}):          "comment on constraint",
	reflect.TypeOf(&commentOnDatabaseNode{}):            "comment on database",
	reflect.TypeOf(&commentOnIndexNode{}):               "comment on index",
	reflect.TypeOf(&commentOnTableNode{}):               "comment on table",
	reflect.TypeOf(&commentOnSchemaNode{}):              "comment on schema",
	reflect.TypeOf(&controlJobsNode{}):                  "control jobs",
	reflect.TypeOf(&controlSchedulesNode{}):             "control schedules",
	reflect.TypeOf(&createDatabaseNode{}):               "create database",
	reflect.TypeOf(&createExtensionNode{}):              "create extension",
	reflect.TypeOf(&createIndexNode{}):                  "create index",
	reflect.TypeOf(&createSequenceNode{}):               "create sequence",
	reflect.TypeOf(&createSchemaNode{}):                 "create schema",
	reflect.TypeOf(&createStatsNode{}):                  "create statistics",
	reflect.TypeOf(&createTableNode{}):                  "create table",
	reflect.TypeOf(&createTypeNode{}):                   "create type",
	reflect.TypeOf(&CreateRoleNode{}):                   "create user/role",
	reflect.TypeOf(&createViewNode{}):                   "create view",
	reflect.TypeOf(&delayedNode{}):                      "virtual table",
	reflect.TypeOf(&deleteNode{}):                       "delete",
	reflect.TypeOf(&deleteRangeNode{}):                  "delete range",
	reflect.TypeOf(&distinctNode{}):                     "distinct",
	reflect.TypeOf(&dropDatabaseNode{}):                 "drop database",
	reflect.TypeOf(&dropIndexNode{}):                    "drop index",
	reflect.TypeOf(&dropSequenceNode{}):                 "drop sequence",
	reflect.TypeOf(&dropSchemaNode{}):                   "drop schema",
	reflect.TypeOf(&dropTableNode{}):                    "drop table",
	reflect.TypeOf(&dropTypeNode{}):                     "drop type",
	reflect.TypeOf(&DropRoleNode{}):                     "drop user/role",
	reflect.TypeOf(&dropViewNode{}):                     "drop view",
	reflect.TypeOf(&errorIfRowsNode{}):                  "error if rows",
	reflect.TypeOf(&explainPlanNode{}):                  "explain plan",
	reflect.TypeOf(&explainVecNode{}):                   "explain vectorized",
	reflect.TypeOf(&explainDDLNode{}):                   "explain ddl",
	reflect.TypeOf(&exportNode{}):                       "export",
	reflect.TypeOf(&fetchNode{}):                        "fetch",
	reflect.TypeOf(&filterNode{}):                       "filter",
	reflect.TypeOf(&GrantRoleNode{}):                    "grant role",
	reflect.TypeOf(&groupNode{}):                        "group",
	reflect.TypeOf(&hookFnNode{}):                       "plugin",
	reflect.TypeOf(&indexJoinNode{}):                    "index join",
	reflect.TypeOf(&insertNode{}):                       "insert",
	reflect.TypeOf(&insertFastPathNode{}):               "insert fast path",
	reflect.TypeOf(&invertedFilterNode{}):               "inverted filter",
	reflect.TypeOf(&invertedJoinNode{}):                 "inverted join",
	reflect.TypeOf(&joinNode{}):                         "join",
	reflect.TypeOf(&limitNode{}):                        "limit",
	reflect.TypeOf(&lookupJoinNode{}):                   "lookup join",
	reflect.TypeOf(&max1RowNode{}):                      "max1row",
	reflect.TypeOf(&ordinalityNode{}):                   "ordinality",
	reflect.TypeOf(&projectSetNode{}):                   "project set",
	reflect.TypeOf(&reassignOwnedByNode{}):              "reassign owned by",
	reflect.TypeOf(&dropOwnedByNode{}):                  "drop owned by",
	reflect.TypeOf(&recursiveCTENode{}):                 "recursive cte",
	reflect.TypeOf(&refreshMaterializedViewNode{}):      "refresh materialized view",
	reflect.TypeOf(&relocateNode{}):                     "relocate",
	reflect.TypeOf(&relocateRange{}):                    "relocate range",
	reflect.TypeOf(&renameColumnNode{}):                 "rename column",
	reflect.TypeOf(&renameDatabaseNode{}):               "rename database",
	reflect.TypeOf(&renameIndexNode{}):                  "rename index",
	reflect.TypeOf(&renameTableNode{}):                  "rename table",
	reflect.TypeOf(&reparentDatabaseNode{}):             "reparent database",
	reflect.TypeOf(&renderNode{}):                       "render",
	reflect.TypeOf(&resetAllNode{}):                     "reset all",
	reflect.TypeOf(&RevokeRoleNode{}):                   "revoke role",
	reflect.TypeOf(&rowCountNode{}):                     "count",
	reflect.TypeOf(&rowSourceToPlanNode{}):              "row source to plan node",
	reflect.TypeOf(&saveTableNode{}):                    "save table",
	reflect.TypeOf(&scanBufferNode{}):                   "scan buffer",
	reflect.TypeOf(&scanNode{}):                         "scan",
	reflect.TypeOf(&scatterNode{}):                      "scatter",
	reflect.TypeOf(&scrubNode{}):                        "scrub",
	reflect.TypeOf(&sequenceSelectNode{}):               "sequence select",
	reflect.TypeOf(&serializeNode{}):                    "run",
	reflect.TypeOf(&setClusterSettingNode{}):            "set cluster setting",
	reflect.TypeOf(&setVarNode{}):                       "set",
	reflect.TypeOf(&setZoneConfigNode{}):                "configure zone",
	reflect.TypeOf(&showFingerprintsNode{}):             "show fingerprints",
	reflect.TypeOf(&showTraceNode{}):                    "show trace for",
	reflect.TypeOf(&showTraceReplicaNode{}):             "replica trace",
	reflect.TypeOf(&showVarNode{}):                      "show",
	reflect.TypeOf(&sortNode{}):                         "sort",
	reflect.TypeOf(&splitNode{}):                        "split",
	reflect.TypeOf(&topKNode{}):                         "top-k",
	reflect.TypeOf(&unsplitNode{}):                      "unsplit",
	reflect.TypeOf(&unsplitAllNode{}):                   "unsplit all",
	reflect.TypeOf(&spoolNode{}):                        "spool",
	reflect.TypeOf(&truncateNode{}):                     "truncate",
	reflect.TypeOf(&unaryNode{}):                        "emptyrow",
	reflect.TypeOf(&unionNode{}):                        "union",
	reflect.TypeOf(&updateNode{}):                       "update",
	reflect.TypeOf(&upsertNode{}):                       "upsert",
	reflect.TypeOf(&valuesNode{}):                       "values",
	reflect.TypeOf(&virtualTableNode{}):                 "virtual table values",
	reflect.TypeOf(&vTableLookupJoinNode{}):             "virtual table lookup join",
	reflect.TypeOf(&windowNode{}):                       "window",
	reflect.TypeOf(&zeroNode{}):                         "norows",
	reflect.TypeOf(&zigzagJoinNode{}):                   "zigzag join",
	reflect.TypeOf(&schemaChangePlanNode{}):             "schema change",
}
