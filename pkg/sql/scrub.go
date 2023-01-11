// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type scrubNode struct {
	optColumnsSlot

	n *tree.Scrub

	run scrubRun
}

// checkOperation is an interface for scrub check execution. The
// different types of checks implement the interface. The checks are
// then bundled together and iterated through to pull results.
//
// NB: Other changes that need to be made to implement a new check are:
//  1. Add the option parsing in startScrubTable
//  2. Queue the checkOperation structs into scrubNode.checkQueue.
//
// TODO(joey): Eventually we will add the ability to repair check
// failures. In that case, we can add a AttemptRepair function that is
// called after each call to Next.
type checkOperation interface {
	// Started indicates if a checkOperation has already been initialized
	// by Start during the lifetime of the operation.
	Started() bool

	// Start initializes the check. In many cases, this does the bulk of
	// the work behind a check.
	Start(params runParams) error

	// Next will return the next check result. The datums returned have
	// the column types specified by scrubTypes, which are the values
	// returned to the user.
	//
	// Next is not called if Done() is false.
	Next(params runParams) (tree.Datums, error)

	// Done indicates when there are no more results to iterate through.
	Done(context.Context) bool

	// Close will clean up any in progress resources.
	Close(context.Context)
}

// Scrub checks the database.
// Privileges: superuser.
func (p *planner) Scrub(ctx context.Context, n *tree.Scrub) (planNode, error) {
	if err := p.RequireAdminRole(ctx, "SCRUB"); err != nil {
		return nil, err
	}
	return &scrubNode{n: n}, nil
}

// scrubRun contains the run-time state of scrubNode during local execution.
type scrubRun struct {
	checkQueue []checkOperation
	row        tree.Datums
}

func (n *scrubNode) startExec(params runParams) error {
	switch n.n.Typ {
	case tree.ScrubTable:
		// If the tableName provided refers to a view and error will be
		// returned here.
		tableDesc, err := params.p.ResolveExistingObjectEx(
			params.ctx, n.n.Table, true /*required*/, tree.ResolveRequireTableDesc)
		if err != nil {
			return err
		}
		tn, ok := params.p.ResolvedName(n.n.Table).(*tree.TableName)
		if !ok {
			return errors.AssertionFailedf("%q was not resolved as a table", n.n.Table)
		}
		if err := n.startScrubTable(params.ctx, params.p, tableDesc, tn); err != nil {
			return err
		}
	case tree.ScrubDatabase:
		if err := n.startScrubDatabase(params.ctx, params.p, &n.n.Database); err != nil {
			return err
		}
	default:
		return errors.AssertionFailedf("unexpected SCRUB type received, got: %v", n.n.Typ)
	}
	return nil
}

func (n *scrubNode) Next(params runParams) (bool, error) {
	for len(n.run.checkQueue) > 0 {
		nextCheck := n.run.checkQueue[0]
		if !nextCheck.Started() {
			if err := nextCheck.Start(params); err != nil {
				return false, err
			}
		}

		// Check if the iterator is finished before calling Next. This
		// happens if there are no more results to report.
		if !nextCheck.Done(params.ctx) {
			var err error
			n.run.row, err = nextCheck.Next(params)
			if err != nil {
				return false, err
			}
			return true, nil
		}

		nextCheck.Close(params.ctx)
		// Prepare the next iterator. If we happen to finish this iterator,
		// we want to begin the next one so we still return a result.
		n.run.checkQueue = n.run.checkQueue[1:]
	}
	return false, nil
}

func (n *scrubNode) Values() tree.Datums {
	return n.run.row
}

func (n *scrubNode) Close(ctx context.Context) {
	// Close any iterators which have not been completed.
	for len(n.run.checkQueue) > 0 {
		n.run.checkQueue[0].Close(ctx)
		n.run.checkQueue = n.run.checkQueue[1:]
	}
}

// startScrubDatabase prepares a scrub check for each of the tables in
// the database. Views are skipped without errors.
func (n *scrubNode) startScrubDatabase(ctx context.Context, p *planner, name *tree.Name) error {
	// Check that the database exists.
	database := string(*name)
	db, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, database)
	if err != nil {
		return err
	}

	schemas, err := p.Descriptors().GetSchemasForDatabase(ctx, p.txn, db)
	if err != nil {
		return err
	}

	var tbNames tree.TableNames
	for _, schema := range schemas {
		sc, err := p.byNameGetterBuilder().Get().Schema(ctx, db, schema)
		if err != nil {
			return err
		}
		toAppend, _, err := p.GetObjectNamesAndIDs(ctx, db, sc)
		if err != nil {
			return err
		}
		tbNames = append(tbNames, toAppend...)
	}

	for i := range tbNames {
		tableName := &tbNames[i]
		flags := tree.ObjectLookupFlags{DesiredObjectKind: tree.TableObject}
		_, _, objDesc, err := p.LookupObject(
			ctx, flags, tableName.Catalog(), tableName.Schema(), tableName.Table(),
		)
		// Skip over descriptors that are not tables (like types).
		// Note: We are asking for table objects above, so It's valid to only
		// get a prefix, and no descriptor.
		if errors.Is(err, catalog.ErrDescriptorWrongType) {
			continue
		}
		if err != nil {
			return err
		}
		if objDesc == nil || objDesc.DescriptorType() != catalog.Table {
			continue
		}
		tableDesc := objDesc.(catalog.TableDescriptor)
		// Skip non-tables and don't throw an error if we encounter one.
		if !tableDesc.IsTable() {
			continue
		}
		if err := n.startScrubTable(ctx, p, tableDesc, tableName); err != nil {
			return err
		}
	}
	return nil
}

func (n *scrubNode) startScrubTable(
	ctx context.Context, p *planner, tableDesc catalog.TableDescriptor, tableName *tree.TableName,
) error {
	ts, hasTS, err := p.getTimestamp(ctx, n.n.AsOf)
	if err != nil {
		return err
	}
	// Process SCRUB options. These are only present during a SCRUB TABLE
	// statement.
	var indexesSet bool
	var physicalCheckSet bool
	var constraintsSet bool
	for _, option := range n.n.Options {
		switch v := option.(type) {
		case *tree.ScrubOptionIndex:
			if indexesSet {
				return pgerror.Newf(pgcode.Syntax,
					"cannot specify INDEX option more than once")
			}
			indexesSet = true
			checks, err := createIndexCheckOperations(v.IndexNames, tableDesc, tableName, ts)
			if err != nil {
				return err
			}
			n.run.checkQueue = append(n.run.checkQueue, checks...)

		case *tree.ScrubOptionPhysical:
			if physicalCheckSet {
				return pgerror.Newf(pgcode.Syntax,
					"cannot specify PHYSICAL option more than once")
			}
			if hasTS {
				return pgerror.Newf(pgcode.Syntax,
					"cannot use AS OF SYSTEM TIME with PHYSICAL option")
			}
			physicalCheckSet = true
			return pgerror.Newf(pgcode.FeatureNotSupported, "PHYSICAL scrub not implemented")

		case *tree.ScrubOptionConstraint:
			if constraintsSet {
				return pgerror.Newf(pgcode.Syntax,
					"cannot specify CONSTRAINT option more than once")
			}
			constraintsSet = true
			constraintsToCheck, err := createConstraintCheckOperations(
				ctx, p, v.ConstraintNames, tableDesc, tableName, ts)
			if err != nil {
				return err
			}
			n.run.checkQueue = append(n.run.checkQueue, constraintsToCheck...)

		default:
			panic(errors.AssertionFailedf("unhandled SCRUB option received: %+v", v))
		}
	}

	// When no options are provided the default behavior is to run
	// exhaustive checks.
	if len(n.n.Options) == 0 {
		indexesToCheck, err := createIndexCheckOperations(nil /* indexNames */, tableDesc, tableName,
			ts)
		if err != nil {
			return err
		}
		n.run.checkQueue = append(n.run.checkQueue, indexesToCheck...)
		constraintsToCheck, err := createConstraintCheckOperations(
			ctx, p, nil /* constraintNames */, tableDesc, tableName, ts)
		if err != nil {
			return err
		}
		n.run.checkQueue = append(n.run.checkQueue, constraintsToCheck...)

		// Physical checks are no longer implemented.
	}
	return nil
}

// getPrimaryColIdxs returns a list of the primary index columns and
// their corresponding index in the columns list.
func getPrimaryColIdxs(
	tableDesc catalog.TableDescriptor, columns []catalog.Column,
) (primaryColIdxs []int, err error) {
	for i := 0; i < tableDesc.GetPrimaryIndex().NumKeyColumns(); i++ {
		colID := tableDesc.GetPrimaryIndex().GetKeyColumnID(i)
		rowIdx := -1
		for idx, col := range columns {
			if col.GetID() == colID {
				rowIdx = idx
				break
			}
		}
		if rowIdx == -1 {
			return nil, errors.Errorf(
				"could not find primary index column in projection: columnID=%d columnName=%s",
				colID,
				tableDesc.GetPrimaryIndex().GetKeyColumnName(i))
		}
		primaryColIdxs = append(primaryColIdxs, rowIdx)
	}
	return primaryColIdxs, nil
}

// col returns the string for referencing a column, with a specific alias,
// e.g. "table.col".
func colRef(tableAlias string, columnName string) string {
	u := tree.UnrestrictedName(columnName)
	if tableAlias == "" {
		return u.String()
	}
	return fmt.Sprintf("%s.%s", tableAlias, &u)
}

// colRefs returns the strings for referencing a list of columns (as a list).
func colRefs(tableAlias string, columnNames []string) []string {
	res := make([]string, len(columnNames))
	for i := range res {
		res[i] = colRef(tableAlias, columnNames[i])
	}
	return res
}

// pairwiseOp joins each string on the left with the string on the right, with a
// given operator in-between. For example
//
//	pairwiseOp([]string{"a","b"}, []string{"x", "y"}, "=")
//
// returns
//
//	[]string{"a = x", "b = y"}.
func pairwiseOp(left []string, right []string, op string) []string {
	if len(left) != len(right) {
		panic(errors.AssertionFailedf("slice length mismatch (%d vs %d)", len(left), len(right)))
	}
	res := make([]string, len(left))
	for i := range res {
		res[i] = fmt.Sprintf("%s %s %s", left[i], op, right[i])
	}
	return res
}

// createIndexCheckOperations will return the checkOperations for the
// provided indexes. If indexNames is nil, then all indexes are
// returned.
// TODO(joey): This can be simplified with
// TableDescriptor.FindIndexByName(), but this will only report the
// first invalid index.
func createIndexCheckOperations(
	indexNames tree.NameList,
	tableDesc catalog.TableDescriptor,
	tableName *tree.TableName,
	asOf hlc.Timestamp,
) (results []checkOperation, err error) {
	if indexNames == nil {
		// Populate results with all secondary indexes of the
		// table.
		for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
			results = append(results, newIndexCheckOperation(
				tableName,
				tableDesc,
				idx,
				asOf,
			))
		}
		return results, nil
	}

	// Find the indexes corresponding to the user input index names.
	names := make(map[string]struct{})
	for _, idxName := range indexNames {
		names[idxName.String()] = struct{}{}
	}
	for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
		if _, ok := names[idx.GetName()]; ok {
			results = append(results, newIndexCheckOperation(
				tableName,
				tableDesc,
				idx,
				asOf,
			))
			delete(names, idx.GetName())
		}
	}
	if len(names) > 0 {
		// Get a list of all the indexes that could not be found.
		missingIndexNames := []string(nil)
		for _, idxName := range indexNames {
			if _, ok := names[idxName.String()]; ok {
				missingIndexNames = append(missingIndexNames, idxName.String())
			}
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject,
			"specified indexes to check that do not exist on table %q: %v",
			tableDesc.GetName(), strings.Join(missingIndexNames, ", "))
	}
	return results, nil
}

// createConstraintCheckOperations will return all of the constraints
// that are being checked. If constraintNames is nil, then all
// constraints are returned.
// Only SQL CHECK, FOREIGN KEY, and UNIQUE constraints are supported.
func createConstraintCheckOperations(
	ctx context.Context,
	p *planner,
	constraintNames tree.NameList,
	tableDesc catalog.TableDescriptor,
	tableName *tree.TableName,
	asOf hlc.Timestamp,
) (results []checkOperation, err error) {
	var constraints []catalog.Constraint
	// Keep only the constraints specified by the constraints in
	// constraintNames.
	if constraintNames != nil {
		for _, constraintName := range constraintNames {
			c := catalog.FindConstraintByName(tableDesc, string(constraintName))
			if c == nil {
				return nil, pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q of relation %q does not exist", constraintName, tableDesc.GetName())
			}
			constraints = append(constraints, c)
		}
	} else {
		constraints = tableDesc.EnforcedConstraints()
	}

	// Populate results with all constraints on the table.
	for _, constraint := range constraints {
		var op checkOperation
		if ck := constraint.AsCheck(); ck != nil {
			op = newSQLCheckConstraintCheckOperation(tableName, tableDesc, ck, asOf)
		} else if fk := constraint.AsForeignKey(); fk != nil {
			referencedTable, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Table(ctx, fk.GetReferencedTableID())
			if err != nil {
				return nil, err
			}
			op = newSQLForeignKeyCheckOperation(tableName, tableDesc, fk, referencedTable, asOf)
		} else if uwi := constraint.AsUniqueWithIndex(); uwi != nil {
			op = newSQLUniqueWithIndexConstraintCheckOperation(tableName, tableDesc, uwi, asOf)
		} else if uwoi := constraint.AsUniqueWithoutIndex(); uwoi != nil {
			op = newSQLUniqueWithoutIndexConstraintCheckOperation(tableName, tableDesc, uwoi, asOf)
		} else {
			return nil, errors.AssertionFailedf("unknown constraint type %T", constraint)
		}
		results = append(results, op)
	}
	return results, nil
}
