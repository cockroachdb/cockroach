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
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// planDependencyInfo collects the dependencies related to a single
// table -- which index and columns are being depended upon.
type planDependencyInfo struct {
	// desc is a reference to the descriptor for the table being
	// depended on.
	desc *sqlbase.TableDescriptor
	// deps is the list of ways in which the current plan depends on
	// that table. There can be more than one entries when the same
	// table is used in different places. The entries can also be
	// different because some may reference an index and others may
	// reference only a subset of the table's columns.
	// Note: the "ID" field of TableDescriptor_Reference is not
	// (and cannot be) filled during plan construction / dependency
	// analysis because the descriptor that is using this dependency
	// has not been constructed yet.
	deps []sqlbase.TableDescriptor_Reference
}

// planDependencies maps the ID of a table depended upon to a list of
// detailed dependencies on that table.
type planDependencies map[sqlbase.ID]planDependencyInfo

// String implements the fmt.Stringer interface.
func (d planDependencies) String() string {
	var buf bytes.Buffer
	for id, deps := range d {
		fmt.Fprintf(&buf, "%d (%q):", id, tree.ErrNameString(&deps.desc.Name))
		for _, dep := range deps.deps {
			buf.WriteString(" [")
			if dep.IndexID != 0 {
				fmt.Fprintf(&buf, "idx: %d ", dep.IndexID)
			}
			fmt.Fprintf(&buf, "cols: %v]", dep.ColumnIDs)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// analyzeViewQuery extracts the set of dependencies (tables and views
// that this view's query depends on), together with the more detailed
// information about which indexes and columns are needed from each
// dependency. The set of columns from the view query's results is
// also returned.
func (p *planner) analyzeViewQuery(
	ctx context.Context, viewSelect *tree.Select,
) (planDependencies, sqlbase.ResultColumns, error) {
	// To avoid races with ongoing schema changes to tables that the view
	// depends on, make sure we use the most recent versions of table
	// descriptors rather than the copies in the lease cache.
	defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
	p.avoidCachedDescriptors = true

	// Request dependency tracking.
	defer func(prev planDependencies) { p.curPlan.deps = prev }(p.curPlan.deps)
	p.curPlan.deps = make(planDependencies)

	// Request star detection
	defer func(prev bool) { p.curPlan.hasStar = prev }(p.curPlan.hasStar)
	p.curPlan.hasStar = false

	// Now generate the source plan.
	sourcePlan, err := p.Select(ctx, viewSelect, []types.T{})
	if err != nil {
		return nil, nil, err
	}
	// The plan will not be needed further.
	defer sourcePlan.Close(ctx)

	// TODO(a-robinson): Support star expressions as soon as we can (#10028).
	if p.curPlan.hasStar {
		return nil, nil, fmt.Errorf("views do not currently support * expressions")
	}

	return p.curPlan.deps, planColumns(sourcePlan), nil
}

// RecomputeViewDependencies does the work of CREATE VIEW w.r.t.
// dependencies over again. Used by a migration to fix existing
// view descriptors created prior to fixing #17269 and #17306;
// it may also be used by a future "fsck" utility.
func RecomputeViewDependencies(ctx context.Context, txn *client.Txn, e *Executor) error {
	lm := e.cfg.LeaseManager
	// We run as NodeUser because we may update system descriptors.
	p, cleanup := newInternalPlanner(
		"recompute-view-dependencies", txn, security.NodeUser, lm.memMetrics, &e.cfg,
	)
	defer cleanup()
	p.Tables().leaseMgr = lm

	log.VEventf(ctx, 2, "recomputing view dependencies")

	// The transaction may modify some system tables (e.g. if a view
	// uses one). Ensure the transaction is anchored to the system
	// range.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}

	// We set "avoidCachedDescriptors" so that the plan constructions
	// below are forced to read the descriptors within the
	// transaction. This is necessary to ensure that the descriptors are
	// read and written within the same transaction, so that the update
	// appears transactional.
	p.avoidCachedDescriptors = true

	// Collect all the descriptors.
	databases := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	tables := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	descs, err := GetAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}
	for _, desc := range descs {
		if db, ok := desc.(*sqlbase.DatabaseDescriptor); ok {
			databases[db.ID] = db
		} else if table, ok := desc.(*sqlbase.TableDescriptor); ok {
			tables[table.ID] = table
		}
	}

	// For each view, analyze the dependencies again.
	allViewDeps := make(map[sqlbase.ID]planDependencies)
	for tableID, table := range tables {
		if !table.IsView() || table.Dropped() {
			continue
		}

		tn := resolveTableNameFromID(ctx, tableID, tables, databases)

		// Is the view query valid?
		stmt, err := parser.ParseOne(table.ViewQuery)
		if err != nil {
			log.Errorf(ctx, "view [%d] (%q) has broken query %q: %v",
				tableID, tn, table.ViewQuery, err)
			continue
		}

		// Request dependency tracking and generate the source plan
		// to collect the dependencies.
		p.curPlan.deps = make(planDependencies)
		sourcePlan, err := p.newPlan(ctx, stmt, []types.T{})
		if err != nil {
			log.Errorf(ctx, "view [%d] (%q) has broken query %q: %v",
				tableID, tn, table.ViewQuery, err)
			continue
		}
		// The plan is not used further, throw it away.
		sourcePlan.Close(ctx)

		log.VEventf(ctx, 1, "collected dependencies for view [%d] (%q):\n%s",
			tableID, tn, p.curPlan.deps.String())
		allViewDeps[tableID] = p.curPlan.deps
	}

	affected := make(map[sqlbase.ID]*sqlbase.TableDescriptor)

	// Clear all the backward dependencies.
	for tableID, table := range tables {
		if len(table.DependedOnBy) > 0 {
			table.DependedOnBy = nil
			affected[tableID] = table
		}
	}

	// Now re-build the dependencies.
	for viewID, viewDeps := range allViewDeps {
		viewDesc := tables[viewID]

		// Register the backward dependencies from the view to the tables
		// it depends on.
		viewDesc.DependsOn = make([]sqlbase.ID, 0, len(viewDeps))
		for backrefID := range viewDeps {
			viewDesc.DependsOn = append(viewDesc.DependsOn, backrefID)
		}

		// Register the forward dependencies from the tables depended on
		// to the view.
		for backrefID, updated := range viewDeps {
			backrefDesc := tables[backrefID]
			for _, dep := range updated.deps {
				// The logical plan constructor merely registered the dependencies.
				// It did not populate the "ID" field of TableDescriptor_Reference.
				// We need to do it here.
				dep.ID = viewID
				backrefDesc.DependedOnBy = append(backrefDesc.DependedOnBy, dep)
			}
			affected[backrefID] = backrefDesc
		}

		affected[viewID] = viewDesc
	}

	// Now persist all the changes.
	for _, updated := range affected {
		// First log the changes being made.
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "relation [%d] (%q):",
			updated.ID, resolveTableNameFromID(ctx, updated.ID, tables, databases))
		if len(updated.DependsOn) == 0 && len(updated.DependedOnBy) == 0 {
			buf.WriteString(" (no dependency links)")
		} else {
			buf.WriteByte('\n')
		}
		// Log the backward dependencies.
		if len(updated.DependsOn) > 0 {
			buf.WriteString("uses:")
			for _, depID := range updated.DependsOn {
				fmt.Fprintf(&buf, " [%d] (%q)", depID, resolveTableNameFromID(ctx, depID, tables, databases))
			}
			buf.WriteByte('\n')
		}
		// Log the forward dependencies.
		if len(updated.DependedOnBy) > 0 {
			buf.WriteString("depended on by: ")
			for i, dep := range updated.DependedOnBy {
				if i > 0 {
					buf.WriteString(", ")
				}
				fmt.Fprintf(&buf, "[%d] (%q): ",
					dep.ID, resolveTableNameFromID(ctx, dep.ID, tables, databases))
				if dep.IndexID != 0 {
					fmt.Fprintf(&buf, "idx: %d ", dep.IndexID)
				}
				fmt.Fprintf(&buf, "cols: %v", dep.ColumnIDs)
			}
			buf.WriteByte('\n')
		}
		log.VEventf(ctx, 1, "updated deps: %s", buf.String())

		// Then register the update.
		if !updated.Dropped() {
			updated.UpVersion = true
		}
		if err := p.writeTableDesc(ctx, updated); err != nil {
			return err
		}
	}

	return nil
}
