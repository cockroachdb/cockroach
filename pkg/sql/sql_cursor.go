// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DeclareCursor implements the DECLARE statement.
// See https://www.postgresql.org/docs/current/sql-declare.html for details.
func (p *planner) DeclareCursor(ctx context.Context, s *tree.DeclareCursor) (planNode, error) {
	if s.Binary {
		return nil, unimplemented.NewWithIssue(77099, "DECLARE BINARY CURSOR")
	}
	if s.Scroll == tree.Scroll {
		return nil, unimplemented.NewWithIssue(77102, "DECLARE SCROLL CURSOR")
	}

	return &delayedNode{
		name: s.String(),
		constructor: func(ctx context.Context, p *planner) (_ planNode, _ error) {
			if p.extendedEvalCtx.TxnImplicit {
				if s.Hold {
					return nil, unimplemented.NewWithIssue(77101, "DECLARE CURSOR WITH HOLD can only be used in transaction blocks")
				}
				return nil, pgerror.Newf(pgcode.NoActiveSQLTransaction, "DECLARE CURSOR can only be used in transaction blocks")
			}

			sd := p.SessionData()
			// This session variable was introduced as a workaround to #96322.
			// Today, if a timeout is set, FETCH's timeout is from the point
			// DECLARE CURSOR is executed rather than the FETCH itself.
			// The setting allows us to override the setting without affecting
			// third-party applications.
			if !p.SessionData().DeclareCursorStatementTimeoutEnabled {
				sd = sd.Clone()
				sd.StmtTimeout = 0
			}
			// We avoid using the internal executor provided by p.InternalSQLTxn()
			// since we want to customize the session data used by the cursor.
			ief := p.ExecCfg().InternalDB
			ie := MakeInternalExecutor(ief.server, ief.memMetrics, ief.monitor)
			ie.SetSessionData(sd)
			ie.extraTxnState = &extraTxnState{
				txn:                p.Txn(),
				descCollection:     p.Descriptors(),
				jobs:               p.extendedEvalCtx.jobs,
				schemaChangerState: p.extendedEvalCtx.SchemaChangerState,
			}
			if err := p.checkIfCursorExists(s.Name); err != nil {
				return nil, err
			}

			// Try to plan the cursor query to make sure that it's valid.
			stmt := makeStatement(statements.Statement[tree.Statement]{AST: s.Select}, clusterunique.ID{})
			pt := planTop{}
			pt.init(&stmt, &p.instrumentation)
			opc := &p.optPlanningCtx
			opc.p.stmt = stmt
			opc.reset(ctx)

			memo, err := opc.buildExecMemo(ctx)
			if err != nil {
				return nil, err
			}
			if err := opc.runExecBuilder(
				ctx,
				&pt,
				&stmt,
				newExecFactory(ctx, p),
				memo,
				p.SemaCtx(),
				p.EvalContext(),
				p.autoCommit,
			); err != nil {
				return nil, err
			}
			if pt.flags.IsSet(planFlagContainsMutation) {
				// Cursors with mutations are invalid.
				return nil, pgerror.Newf(pgcode.FeatureNotSupported,
					"DECLARE CURSOR must not contain data-modifying statements in WITH")
			}

			statement := formatWithPlaceholders(ctx, s.Select, p.EvalContext())
			itCtx := context.Background()
			rows, err := ie.QueryIterator(itCtx, "sql-cursor", p.txn, statement)
			if err != nil {
				return nil, errors.Wrap(err, "failed to DECLARE CURSOR")
			}
			cursor := &sqlCursor{
				Rows:       rows,
				readSeqNum: p.txn.GetReadSeqNum(),
				txn:        p.txn,
				statement:  statement,
				created:    timeutil.Now(),
				withHold:   s.Hold,
			}
			if err := p.sqlCursors.addCursor(s.Name, cursor); err != nil {
				// This case shouldn't happen because cursor names are scoped to a session,
				// and sessions can't have more than one statement running at once. But
				// let's be diligent and clean up if it somehow does happen anyway.
				_ = cursor.Close()
				return nil, err
			}
			return newZeroNode(nil /* columns */), nil
		},
	}, nil
}

// checkIfCursorExists checks whether a cursor or portal with the given name
// already exists, and returns an error if one does.
func (p *planner) checkIfCursorExists(name tree.Name) error {
	if cursor := p.sqlCursors.getCursor(name); cursor != nil {
		return pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists", name)
	}
	if p.extendedEvalCtx.PreparedStatementState.HasPortal(string(name)) {
		return pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists as portal", name)
	}
	return nil
}

var errBackwardScan = pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "cursor can only scan forward")

// FetchCursor implements the FETCH and MOVE statements.
// See https://www.postgresql.org/docs/current/sql-fetch.html for details.
func (p *planner) FetchCursor(_ context.Context, s *tree.CursorStmt) (planNode, error) {
	return p.newFetchNode(s)
}

// newFetchNode creates a new fetchNode, which implements FETCH and MOVE
// statements.
func (p *planner) newFetchNode(s *tree.CursorStmt) (*fetchNode, error) {
	cursor := p.sqlCursors.getCursor(s.Name)
	if cursor == nil {
		return nil, pgerror.Newf(
			pgcode.InvalidCursorName, "cursor %q does not exist", s.Name,
		)
	}
	if s.Count < 0 || s.FetchType == tree.FetchBackwardAll {
		return nil, errBackwardScan
	}
	node := &fetchNode{
		n:         s.Count,
		fetchType: s.FetchType,
		cursor:    cursor,
	}
	if s.FetchType != tree.FetchNormal {
		node.n = 0
		node.offset = s.Count
	}
	return node, nil
}

type fetchNode struct {
	cursor *sqlCursor
	// n is the number of rows requested.
	n int64
	// offset is the number of rows to read first, when in relative or absolute
	// mode.
	offset    int64
	fetchType tree.FetchType

	seeked bool

	// origTxnSeqNum is the transaction sequence number of the user's transaction
	// before the fetch began.
	origTxnSeqNum enginepb.TxnSeq
}

func (f *fetchNode) startInternal() error {
	if !f.cursor.eagerExecution {
		// We need to make sure that we're reading at the same read sequence number
		// that we had when we created the cursor, to preserve the "sensitivity"
		// semantics of cursors, which demand that data written after the cursor
		// was declared is not visible to the cursor.
		f.origTxnSeqNum = f.cursor.txn.GetReadSeqNum()
		return f.cursor.txn.SetReadSeqNum(f.cursor.readSeqNum)
	}
	// If eagerExecution is set, the cursor has already been fully read into a row
	// container, so there is no need to set the read sequence number.
	return nil
}

func (f *fetchNode) nextInternal(ctx context.Context) (bool, error) {
	if f.fetchType == tree.FetchAll {
		return f.cursor.Next(ctx)
	}

	if !f.seeked {
		// FIRST, LAST, ABSOLUTE, and RELATIVE require seeking before returning
		// values. Do that first.
		f.seeked = true
		switch f.fetchType {
		case tree.FetchFirst:
			switch f.cursor.curRow {
			case 0:
				_, err := f.cursor.Next(ctx)
				return true, err
			case 1:
				return true, nil
			}
			return false, errBackwardScan
		case tree.FetchLast:
			return false, errBackwardScan
		case tree.FetchAbsolute:
			if f.cursor.curRow > f.offset {
				return false, errBackwardScan
			}
			for f.cursor.curRow < f.offset {
				more, err := f.cursor.Next(ctx)
				if !more || err != nil {
					return more, err
				}
			}
			return true, nil
		case tree.FetchRelative:
			for i := int64(0); i < f.offset; i++ {
				more, err := f.cursor.Next(ctx)
				if !more || err != nil {
					return more, err
				}
			}
			return true, nil
		}
	}
	if f.n <= 0 {
		return false, nil
	}
	f.n--
	return f.cursor.Next(ctx)
}

func (f *fetchNode) startExec(params runParams) error {
	return f.startInternal()
}

func (f *fetchNode) Next(params runParams) (bool, error) {
	return f.nextInternal(params.ctx)
}

func (f fetchNode) Values() tree.Datums {
	return f.cursor.Cur()
}

func (f fetchNode) Close(ctx context.Context) {
	// We explicitly do not pass through the Close to our Rows, because
	// running FETCH on a CURSOR does not close it.
	if !f.cursor.eagerExecution {
		// Reset the transaction's read sequence number to what it was before the
		// fetch began, so that subsequent reads in the transaction can still see
		// writes from that transaction.
		if err := f.cursor.txn.SetReadSeqNum(f.origTxnSeqNum); err != nil {
			log.Warningf(ctx, "error resetting transaction read seq num after CURSOR operation: %v", err)
		}
	}
}

// CloseCursor implements the FETCH statement.
// See https://www.postgresql.org/docs/current/sql-close.html for details.
func (p *planner) CloseCursor(ctx context.Context, n *tree.CloseCursor) (planNode, error) {
	return &delayedNode{
		name: n.String(),
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			if n.All {
				return newZeroNode(nil /* columns */), p.sqlCursors.closeAll(false /* errorOnWithHold */)
			}
			return newZeroNode(nil /* columns */), p.sqlCursors.closeCursor(n.Name)
		},
	}, nil
}

// GenUniqueCursorName implements the eval.Planner interface.
func (p *planner) GenUniqueCursorName() tree.Name {
	return p.sqlCursors.genUniqueName()
}

// PLpgSQLCloseCursor implements the eval.Planner interface.
func (p *planner) PLpgSQLCloseCursor(cursorName tree.Name) error {
	return p.sqlCursors.closeCursor(cursorName)
}

// PLpgSQLFetchCursor returns the next row from the cursor with the given name
// after seeking past the specified number of rows. It is used to implement the
// PLpgSQL FETCH and MOVE statements. When there are no rows left to return,
// PLpgSQLFetchCursor returns nil.
//
// Note: when called with the FORWARD ALL option, PLpgSQLFetchCursor will only
// return the last row. This is compatible with PLpgSQL, as FORWARD ALL can only
// be used with PLpgSQL MOVE, which ignores all returned rows.
func (p *planner) PLpgSQLFetchCursor(
	ctx context.Context, cursorStmt *tree.CursorStmt,
) (res tree.Datums, err error) {
	cursor, err := p.newFetchNode(cursorStmt)
	if err != nil {
		return nil, err
	}
	if err = cursor.startInternal(); err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var hasNext bool
	hasNext, err = cursor.nextInternal(ctx)
	for err == nil && hasNext {
		res = cursor.Values()
		hasNext, err = cursor.nextInternal(ctx)
	}
	return res, err
}

type sqlCursor struct {
	isql.Rows
	// txn is the transaction object that the internal executor for this cursor
	// is running with.
	txn *kv.Txn
	// readSeqNum is the sequence number of the transaction that the cursor was
	// initialized with.
	readSeqNum enginepb.TxnSeq
	statement  string
	created    time.Time
	curRow     int64
	withHold   bool
	// eagerExecution indicates that the cursor's query was executed eagerly and
	// stored in a row container. If true, there is no need to set the transaction
	// sequence number, since the query is no longer active.
	eagerExecution bool
}

// Next implements the Rows interface.
func (s *sqlCursor) Next(ctx context.Context) (bool, error) {
	more, err := s.Rows.Next(ctx)
	if err == nil {
		s.curRow++
	}
	return more, err
}

// sqlCursors contains a set of active cursors for a session.
type sqlCursors interface {
	// closeAll closes all cursors in the set. If any of the cursors were
	// created WITH HOLD, and the errorOnWithHold flag is true, an error is
	// returned.
	closeAll(errorOnWithHold bool) error
	// closeCursor closes the named cursor, returning an error if that cursor
	// didn't exist in the set.
	closeCursor(tree.Name) error
	// getCursor returns the named cursor, returning nil if that cursor
	// didn't exist in the set.
	getCursor(tree.Name) *sqlCursor
	// addCursor adds a new cursor with the given name to the set, returning an
	// error if the cursor already existed in the set.
	addCursor(tree.Name, *sqlCursor) error
	// list returns all open cursors in the set.
	list() map[tree.Name]*sqlCursor
	// genUniqueName is used to generate a name for an unnamed PLpgSQL cursor that
	// will not conflict with other cursors currently defined on the session.
	genUniqueName() tree.Name
}

// emptySqlCursors is the default impl used by the planner when the
// connExecutor is not available.
type emptySqlCursors struct{}

var _ sqlCursors = emptySqlCursors{}

func (e emptySqlCursors) closeAll(bool) error {
	return errors.AssertionFailedf("closeAll not supported in emptySqlCursors")
}

func (e emptySqlCursors) closeCursor(tree.Name) error {
	return errors.AssertionFailedf("closeCursor not supported in emptySqlCursors")
}

func (e emptySqlCursors) getCursor(tree.Name) *sqlCursor {
	return nil
}

func (e emptySqlCursors) addCursor(tree.Name, *sqlCursor) error {
	return errors.AssertionFailedf("addCursor not supported in emptySqlCursors")
}

func (e emptySqlCursors) list() map[tree.Name]*sqlCursor {
	return nil
}

func (e emptySqlCursors) genUniqueName() tree.Name {
	return ""
}

// cursorMap is a sqlCursors that's backed by an actual map.
type cursorMap struct {
	cursors map[tree.Name]*sqlCursor
	// nameCounter is used to help generate unique names for unnamed PLpgSQL
	// cursors.
	nameCounter int
}

func (c *cursorMap) closeAll(errorOnWithHold bool) error {
	for n, c := range c.cursors {
		if c.withHold && errorOnWithHold {
			return unimplemented.NewWithIssuef(77101, "cursor %s WITH HOLD must be closed before committing", n)
		}
		if err := c.Close(); err != nil {
			return err
		}
	}
	c.cursors = nil
	return nil
}

func (c *cursorMap) closeCursor(s tree.Name) error {
	cursor, ok := c.cursors[s]
	if !ok {
		return pgerror.Newf(pgcode.InvalidCursorName, "cursor %q does not exist", s)
	}
	err := cursor.Close()
	delete(c.cursors, s)
	return err
}

func (c *cursorMap) getCursor(s tree.Name) *sqlCursor {
	return c.cursors[s]
}

func (c *cursorMap) addCursor(s tree.Name, cursor *sqlCursor) error {
	if c.cursors == nil {
		c.cursors = make(map[tree.Name]*sqlCursor)
	}
	if _, ok := c.cursors[s]; ok {
		return pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists", s)
	}
	c.cursors[s] = cursor
	return nil
}

func (c *cursorMap) list() map[tree.Name]*sqlCursor {
	return c.cursors
}

func (c *cursorMap) genUniqueName() tree.Name {
	for {
		c.nameCounter++
		name := tree.Name(fmt.Sprintf("<unnamed portal %d>", c.nameCounter))
		if _, ok := c.cursors[name]; !ok {
			// This name is unique.
			return name
		}
	}
}

// connExCursorAccessor is a sqlCursors that delegates to a connExecutor's
// extraTxnState.
type connExCursorAccessor struct {
	ex *connExecutor
}

func (c connExCursorAccessor) closeAll(errorOnWithHold bool) error {
	return c.ex.extraTxnState.sqlCursors.closeAll(errorOnWithHold)
}

func (c connExCursorAccessor) closeCursor(s tree.Name) error {
	return c.ex.extraTxnState.sqlCursors.closeCursor(s)
}

func (c connExCursorAccessor) getCursor(s tree.Name) *sqlCursor {
	return c.ex.extraTxnState.sqlCursors.getCursor(s)
}

func (c connExCursorAccessor) addCursor(s tree.Name, cursor *sqlCursor) error {
	return c.ex.extraTxnState.sqlCursors.addCursor(s, cursor)
}

func (c connExCursorAccessor) list() map[tree.Name]*sqlCursor {
	return c.ex.extraTxnState.sqlCursors.list()
}

func (c connExCursorAccessor) genUniqueName() tree.Name {
	return c.ex.extraTxnState.sqlCursors.genUniqueName()
}

// checkNoConflictingCursors returns an error if the input schema changing
// statement conflicts with any open SQL cursors in the current planner.
func (p *planner) checkNoConflictingCursors(stmt tree.Statement) error {
	// TODO(jordan): this is stricter than Postgres is. Postgres permits
	// concurrent schema changes within a transaction where a cursor is already
	// open if the open cursors do not depend on schema objects being changed.
	// We could improve this by matching the memo metadata's list of dependent
	// schema objects in each open cursor with the objects being changed in the
	// schema change.
	if len(p.sqlCursors.list()) > 0 {
		return unimplemented.NewWithIssue(74608, "cannot run schema change "+
			"in a transaction with open DECLARE cursors")
	}
	return nil
}
