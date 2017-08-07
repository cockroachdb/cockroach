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
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
		fmt.Fprintf(&buf, "%d (%q):", id, parser.ErrString(parser.Name(deps.desc.Name)))
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
// also returned, in a mutable form.
func (p *planner) analyzeViewQuery(
	ctx context.Context, viewSelect *parser.Select,
) (planDependencies, sqlbase.ResultColumns, error) {
	// To avoid races with ongoing schema changes to tables that the view
	// depends on, make sure we use the most recent versions of table
	// descriptors rather than the copies in the lease cache.
	defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
	p.avoidCachedDescriptors = true

	// Request dependency tracking.
	defer func(prev planDependencies) { p.planDeps = prev }(p.planDeps)
	p.planDeps = make(planDependencies)

	// Now generate the source plan.
	sourcePlan, err := p.Select(ctx, viewSelect, []parser.Type{})
	if err != nil {
		return nil, nil, err
	}
	// The plan will not be needed further.
	defer sourcePlan.Close(ctx)

	// TODO(a-robinson): Support star expressions as soon as we can (#10028).
	if p.planContainsStar(ctx, sourcePlan) {
		return nil, nil, fmt.Errorf("views do not currently support * expressions")
	}

	return p.planDeps, planMutableColumns(sourcePlan), nil
}

// enforceViewResultColumnNames ensures that the view query, when it
// will be compiled again each time the view is used, yields a plan
// that renders columns as specified in the CREATE VIEW statement.
//
// In particular the two following properties are enforced:
//
// - the column names match those requested. For example:
//
//      CREATE VIEW v(a, b) AS TABLE kv
//   -> CREATE VIEW v(a, b) AS SELECT k AS a, v AS b FROM kv
//
//   Otherwise, clients that query pg_catalog.pg_views may get
//   confused.
//
// - all the columns must not be hidden. This is e.g. a concern with:
//
//     CREATE TABLE foo(x INT);
//     CREATE VIEW v(x, rowid) AS SELECT x, rowid FROM foo;
//
//   for otherwise 'rowid' may become hidden when using view v in a
//   query like SELECT * FROM v.
func enforceViewResultColumnNames(
	viewQuery *parser.Select, requestedNames parser.NameList, sourceColumns sqlbase.ResultColumns,
) (*parser.Select, error) {
	// Check the requested column interface and the source plan's columns
	// have the same arity.
	numColNames := len(requestedNames)
	numColumns := len(sourceColumns)
	if numColNames != 0 && numColNames != numColumns {
		return nil, sqlbase.NewSyntaxError(fmt.Sprintf(
			"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
			numColNames, util.Pluralize(int64(numColNames)),
			numColumns, util.Pluralize(int64(numColumns))))
	}

	// If the final column names differ from the plan's column names, or
	// if the plan's columns are hidden, we need to add explicit renders
	// that render to the final (demanded) column names.
	needExplicitRenders := false
	for i, desired := range requestedNames {
		if string(desired) != sourceColumns[i].Name {
			needExplicitRenders = true
			break
		}
	}
	if !needExplicitRenders {
		for i := range sourceColumns {
			if sourceColumns[i].Hidden {
				needExplicitRenders = true
				break
			}
		}
	}
	if !needExplicitRenders {
		return viewQuery, nil
	}

	// Transform:
	// <viewquery> -> SELECT @1 AS a, @2 AS b, @3 AS c FROM (<viewquery>)
	//
	// Note: this cannot be changed to SELECT origName AS newName FROM ...
	// (i.e. use origName instead of an ordinal reference) because of
	// views like `CREATE v(a) AS ARRAY[3]`. Here the computed column
	// name is "ARRAY[3]" during view creation, but "ARRAY[3:::INT]"
	// during view execution. So creation would cause a view
	// descriptor containing `SELECT "ARRAY[3]" AS a FROM (SELECT
	// ARRAY[3:::INT])` which would be broken.
	renamedRenders := make(parser.SelectExprs, len(requestedNames))
	for i, desired := range requestedNames {
		renamedRenders[i].Expr = parser.NewOrdinalReference(i)
		renamedRenders[i].As = desired
		sourceColumns[i].Name = string(desired)
	}
	return &parser.Select{
		Select: &parser.SelectClause{
			Exprs: renamedRenders,
			From: &parser.From{
				Tables: []parser.TableExpr{
					&parser.AliasedTableExpr{
						Expr: &parser.Subquery{
							Select: &parser.ParenSelect{Select: viewQuery},
						},
					},
				},
			},
		},
	}, nil
}

// prepareApparentSchema constructs the name-to-ID mapping as
// perceived by this view at time of creation, which will be used
// whenever the view query is expanded in the future.
func (p *planner) prepareApparentSchema(
	ctx context.Context, planDeps planDependencies,
) (result []sqlbase.TableDescriptor_SchemaDependency, err error) {
	for _, dep := range planDeps {
		tableDesc := dep.desc

		dbDesc, err := getDatabaseDescByID(ctx, p.txn, tableDesc.ParentID)
		if err != nil {
			return nil, err
		}

		// What are the column and index IDs actually needed?
		neededColumns := make(map[sqlbase.ColumnID]struct{})
		neededIndexes := make(map[sqlbase.IndexID]struct{})
		for _, backref := range dep.deps {
			if backref.IndexID != 0 {
				neededIndexes[backref.IndexID] = struct{}{}
			}
			for _, c := range backref.ColumnIDs {
				neededColumns[c] = struct{}{}
			}
		}

		sDep := sqlbase.TableDescriptor_SchemaDependency{
			ID:           tableDesc.ID,
			DatabaseName: dbDesc.Name,
			TableName:    tableDesc.Name,
			ColumnIDs:    make([]sqlbase.ColumnID, 0, len(neededColumns)),
			ColumnNames:  make([]string, 0, len(neededColumns)),
			IndexIDs:     make([]sqlbase.IndexID, 0, len(neededIndexes)),
			IndexNames:   make([]string, 0, len(neededIndexes)),
		}
		for _, col := range tableDesc.Columns {
			if _, ok := neededColumns[col.ID]; ok {
				sDep.ColumnNames = append(sDep.ColumnNames, col.Name)
				sDep.ColumnIDs = append(sDep.ColumnIDs, col.ID)
			}
		}
		if _, ok := neededIndexes[tableDesc.PrimaryIndex.ID]; ok {
			sDep.IndexNames = append(sDep.IndexNames, tableDesc.PrimaryIndex.Name)
			sDep.IndexIDs = append(sDep.IndexIDs, tableDesc.PrimaryIndex.ID)
		}
		for _, idx := range tableDesc.Indexes {
			if _, ok := neededIndexes[idx.ID]; ok {
				sDep.IndexNames = append(sDep.IndexNames, idx.Name)
				sDep.IndexIDs = append(sDep.IndexIDs, idx.ID)
			}
		}

		result = append(result, sDep)
	}
	return result, nil
}

// prepareViewQuery ensures that any table referenced by the view's source query
// becomes referenced by a numeric table reference.
func (p *planner) prepareViewQuery(
	ctx context.Context,
	viewSelect *parser.Select,
	planDeps planDependencies,
) (string, error) {
	var queryBuf bytes.Buffer
	var fmtErr error

	processAndFormatTableRef := func(tref *parser.TableRef, buf *bytes.Buffer, f parser.FmtFlags) bool {
		if fmtErr != nil {
			return false
		}
		deps, ok := planDeps[sqlbase.ID(tref.TableID)]
		if !ok {
			fmtErr = errors.Errorf("table [%d] was not found during dependency analysis", tref.TableID)
			return false
		}
		desc := deps.desc

		// What are the columns that may be named by expressions
		// in the view query, even if they are not subsequently used?
		// For example:
		//   SELECT k FROM (SELECT k, v FROM kv)
		// column "v" is named but not needed.
		var namedColIDs []parser.ColumnID
		var namedColNames parser.NameList
		if tref.As.Cols != nil {
			// The user has specified the columns they want already,
			// so we can use that, no questions asked.
			namedColNames = tref.As.Cols
			if tref.Columns != nil {
				// The user also has specified in which order they want their
				// columns, so use that.
				// Example: [123(2,1) as kv(v,k)]
				namedColIDs = tref.Columns
			} else {
				// The user has specified names, but no column IDs. They
				// want all the columns in the table.
				// Example: [123 as kv(k,v)] -> [123(1,2) as kv(k,v)]
				namedColIDs = make([]parser.ColumnID, len(desc.Columns))
				for i := range desc.Columns {
					namedColIDs[i] = parser.ColumnID(desc.Columns[i].ID)
				}
			}
			// Unfortunately, we're not done yet. The name list can be
			// smaller than the ID list, in which case the "natural" names
			// from the columns listed in the ID list are implicitly
			// assumed. Make them explicit.
			// Examples:
			// [123 as kv(a)]      -> [123(1,2) as kv(a,v)]
			// [123(2,1) as kv(v)] -> [123(2,1) as kv(v,k)]
			// [123(2,1) as kv]    -> [123(2,1) as kv(v,k)]
			for i := len(namedColNames); i < len(namedColIDs); i++ {
				for _, col := range desc.Columns {
					if col.ID == sqlbase.ColumnID(namedColIDs[i]) {
						namedColNames = append(namedColNames, parser.Name(col.Name))
						break
					}
				}
			}
		} else if tref.Columns != nil {
			// The user hasn't specified column names, but
			// they did specify a column list. Trust them.
			// Example: [123(2,1)] -> [123(2,1) as kv(v,k)]
			namedColIDs = tref.Columns
			namedColNames = make(parser.NameList, len(tref.Columns))
			for i, c := range tref.Columns {
				for _, col := range desc.Columns {
					if col.ID == sqlbase.ColumnID(c) {
						namedColNames[i] = parser.Name(col.Name)
						break
					}
				}
			}
		} else {
			// The user has specified neither a column list
			// nor a column name list, so they really want all the table.
			// Example: [123] -> [123(1,2) as kv(k,v)]
			namedColIDs = make([]parser.ColumnID, len(desc.Columns))
			namedColNames = make(parser.NameList, len(desc.Columns))
			for i := range desc.Columns {
				namedColIDs[i] = parser.ColumnID(desc.Columns[i].ID)
				namedColNames[i] = parser.Name(desc.Columns[i].Name)
			}
		}

		// At this point, we have a full table reference with both a list
		// of column IDs and a list of names, and both match in length.
		// If some of the columns are not actually needed by the query, we

		// Reconnect the dependency list in the table reference.
		tref.Columns = namedColIDs
		tref.As.Cols = namedColNames
		if tref.As.Alias == "" {
			// If there was no explicit table alias in the table reference,
			// populate it so there is no surprise if the table gets renamed.
			tref.As.Alias = parser.Name(desc.Name)
		}

		return false
	}

	viewSelect.Format(
		&queryBuf,
		parser.FmtReformatTableRefs(
			parser.FmtParsable,
			processAndFormatTableRef))
	if fmtErr != nil {
		return "", fmtErr
	}
	return queryBuf.String(), nil
}

// RecomputeViewDependencies does the work of CREATE VIEW w.r.t.
// dependencies over again. Used by a migration to fix existing
// view descriptors created prior to fixing #17269 and #17306;
// it may also be used by a future "fsck" utility.
func RecomputeViewDependencies(ctx context.Context, txn *client.Txn, e *Executor) error {
	lm := e.cfg.LeaseManager
	// We run as NodeUser because we may update system descriptors.
	p := makeInternalPlanner("recompute-view-dependencies", txn, security.NodeUser, lm.memMetrics)
	defer finishInternalPlanner(p)
	p.session.tables.leaseMgr = lm

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
	descs, err := getAllDescriptors(ctx, p.txn)
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
		p.planDeps = make(planDependencies)
		sourcePlan, err := p.newPlan(ctx, stmt, []parser.Type{})
		if err != nil {
			log.Errorf(ctx, "view [%d] (%q) has broken query %q: %v",
				tableID, tn, table.ViewQuery, err)
			continue
		}
		// The plan is not used further, throw it away.
		sourcePlan.Close(ctx)

		log.VEventf(ctx, 1, "collected dependencies for view [%d] (%q):\n%s",
			tableID, tn, p.planDeps.String())
		allViewDeps[tableID] = p.planDeps
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
